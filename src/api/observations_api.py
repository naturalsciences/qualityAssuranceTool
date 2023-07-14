from collections import Counter
from copy import deepcopy
from functools import partial
import logging
from datetime import datetime
import pandas as pd
from math import ceil
import json

from stapy import Query, Entity

from models.enums import Entities, Properties, Qactions, QualityFlags, Settings, Filter
from datetime import datetime
from models.constants import ISO_STR_FORMAT
from qc_functions.functions import min_max_check_values
from utils.utils import convert_to_datetime, get_request


log = logging.getLogger(__name__)


def filter_cfg_to_query(filter_cfg) -> str:
    filter_condition = ""
    if filter_cfg:
        range = filter_cfg.get(Properties.PHENOMENONTIME).get("range")
        format = filter_cfg.get(Properties.PHENOMENONTIME).get("format")

        t0, t1 = [datetime.strptime(str(ti), format) for ti in range]

        filter_condition = (
            f"{Properties.PHENOMENONTIME} gt {t0.strftime(ISO_STR_FORMAT)} and "
            f"{Properties.PHENOMENONTIME} lt {t1.strftime(ISO_STR_FORMAT)}"
        )
    return filter_condition


def get_results_n_datastreams_query(
    entity_id,
    n,
    skip,
    top_observations,
    filter_condition,
    expand_feature_of_interest=True,
):
    # TODO: cleanup!!
    idx_slice = 3
    if expand_feature_of_interest:
        idx_slice = 4
    expand_list = [
        Entities.OBSERVATIONS(
            [
                Filter.FILTER(filter_condition),
                Settings.TOP(top_observations),
                Qactions.SELECT(
                    [
                        Properties.IOT_ID,
                        "result",
                        Properties.PHENOMENONTIME,
                    ]
                ),
                Qactions.EXPAND(
                    [
                        Entities.FEATUREOFINTEREST(
                            [Qactions.SELECT([Properties.COORDINATES])]
                        )
                    ]
                ),
            ][:idx_slice]
        ),
        Entities.OBSERVEDPROPERTY(
            [
                Qactions.SELECT(
                    [
                        Properties.IOT_ID,
                        Properties.NAME,
                    ]
                )
            ]
        ),
    ]
    Q = Qactions.EXPAND(
        [
            Entities.DATASTREAMS(
                [
                    Settings.TOP(n),
                    Settings.SKIP(skip),
                    Qactions.SELECT(
                        [
                            Properties.IOT_ID,
                            Properties.UNITOFMEASUREMENT,
                            Entities.OBSERVATIONS,
                        ]
                    ),
                    Qactions.EXPAND(expand_list),
                ]
            )
        ]
    )
    Q_out = (
        Query(Entity.Thing)
        .entity_id(entity_id)
        .select(Entities.DATASTREAMS)
        .get_query()
        + "&"
        + Q
    )
    return Q_out


def get_results_n_datastreams(Q):
    log.info("Start request")
    request = get_request(Q)
    # request = json.loads(Query(Entity.Thing).get_with_retry(complete_query).content)
    log.info("End request")

    return request


def features_request_to_df(request_features):
    data = []
    for fi in request_features["value"]:
        v = fi.get(Properties.IOT_ID)
        long, lat = fi.get("feature").get("coordinates")
        idx = [oi.get(Properties.IOT_ID) for oi in fi.get(Entities.OBSERVATIONS)]
        for idx_i in idx:
            data.append([idx_i, v, long, lat])
    df = pd.DataFrame(data, columns=["observation_id", "feature_id", "long", "lat"])
    return df


def get_features_of_interest(filter_cfg, top_observations):
    filter_condition = filter_cfg_to_query(filter_cfg)
    base_query = Query(Entity.FeatureOfInterest).get_query()
    complete_query = (
        base_query
        + "?"
        + Qactions.SELECT(
            [Properties.IOT_ID, "feature/coordinates", Entities.OBSERVATIONS]
        )
        + "&"
        + Qactions.EXPAND(
            [
                Entities.OBSERVATIONS(
                    [
                        Qactions.SELECT([Properties.IOT_ID]),
                        Settings.TOP(top_observations),
                    ]
                )
            ]
        )
    )
    complete_query += "&" + Settings.TOP(top_observations)
    log.info("Start request features")
    log.debug(f"{complete_query}")
    request_features = json.loads(
        Query(Entity.FeatureOfInterest).get_with_retry(complete_query).content
    )
    log.info("End request features")

    df_features = features_request_to_df(request_features)
    features_observations_dict = {
        fi.get(Properties.IOT_ID): [
            oi.get(Properties.IOT_ID) for oi in fi.get(Entities.OBSERVATIONS)
        ]
        for fi in request_features["value"]
    }
    # possible to write to pickle?
    # how to test if needed or not?
    return features_observations_dict


## from typing import Tuple, cast
# get_id_result_lists, get_datetime_latest_observation
# def get_id_result_lists(iot_id: int) -> Tuple[int, list]:
#     id_list: int
#     result_list: list
#
#     id_list, result_list = cast(
#         list,
#         Query(Entity.Datastream)
#         .entity_id(iot_id)
#         .sub_entity(Entity.Observation)
#         .select(Properties.IOT_ID, "result")
#         .get_data_sets(),
#     )
#     log.info(f"call done")
#     return id_list, result_list


# def get_iot_id_datastreams_in_qc(dict_in: dict, summary_dict: dict):
#     log.debug(f"Start loop datastreams items.")
#     dict_out = copy.deepcopy(dict_in)
#     for k, dsi in summary_dict.get(Entities.DATASTREAMS, {}).items():
#         property_name = k.split(" -- ", 1)[1]
#         if property_name in dict_out:
#             dict_out[property_name] += dsi.get(Properties.IOT_ID)
#     return dict_out


def datastreams_request_to_df(request_datastreams):
    df = pd.DataFrame()
    for di in request_datastreams:
        observations_list = di.get(Entities.OBSERVATIONS)
        if observations_list:
            df_i = pd.DataFrame(observations_list).astype(
                {Properties.IOT_ID: int, "result": float}
            )
            df_i["datastream_id"] = int(di.get(Properties.IOT_ID))
            df_i[Properties.PHENOMENONTIME] = df_i[Properties.PHENOMENONTIME].apply(
                convert_to_datetime
            )
            df_i["observation_type"] = di.get(Entities.OBSERVEDPROPERTY).get(
                Properties.NAME
            )
            df_i["observation_type"] = df_i["observation_type"].astype("category")
            k1, k2 = Properties.UNITOFMEASUREMENT.split("/", 1)
            df_i["units"] = di.get(k1).get(k2)
            df_i["units"] = df_i["units"].astype("category")

            df_i[["long", "lat"]] = pd.DataFrame.from_records(
                df_i[str(Entities.FEATUREOFINTEREST)].apply(
                    lambda x: x.get("feature").get("coordinates")
                )
            )
            del df_i[str(Entities.FEATUREOFINTEREST)]
            # df_i.drop(columns=str(Entities.FEATUREOFINTEREST))
            df = pd.concat([df, df_i], ignore_index=True)

    return df

def get_nb_datastreams_of_thing(thing_id: int) -> int:
    base_query = (
        Query(Entity.Thing).entity_id(thing_id).select("Datastreams/@iot.count")
    )
    add_query_nb = Qactions.EXPAND(
        [
            Entities.DATASTREAMS(
                [Settings.COUNT("true"), Qactions.SELECT([Properties.IOT_ID])]
            )
        ]
    )
    nb_datastreams = json.loads(
        Query(Entity.Datastream)
        .get_with_retry(base_query.get_query() + "&" + add_query_nb)
        .content
    ).get("Datastreams@iot.count")
    return nb_datastreams


def get_all_datastreams_data(
    thing_id, nb_streams_per_call, top_observations, filter_cfg
) -> pd.DataFrame:
    df_all = pd.DataFrame()

    nb_datastreams = get_nb_datastreams_of_thing(thing_id=thing_id)
    log.debug(f"{nb_datastreams=}")
    for i in range(ceil(nb_datastreams / nb_streams_per_call)):
        log.info(f"nb {i} of {ceil(nb_datastreams/nb_streams_per_call)}")
        query = get_results_n_datastreams_query(
            entity_id=thing_id,
            n=nb_streams_per_call,
            skip=nb_streams_per_call * i,
            top_observations=top_observations,
            filter_condition=filter_cfg,
            # expand_feature_of_interest=True,
        )
        status_code, response = get_results_n_datastreams(query)
        if status_code != 200:
            raise IOError(f"Status code: {status_code}")
        for ds_i in response[Entities.DATASTREAMS]:
            if f"{Entities.OBSERVATIONS}@iot.nextLink" in ds_i:
                log.warning("Not all observations are extracted!")  # TODO: follow link!
        df_i = datastreams_request_to_df(response[Entities.DATASTREAMS])
        log.debug(f"{df_i.shape[0]=}")
        df_all = pd.concat([df_all, df_i], ignore_index=True)
    return df_all


def qc_df(df_in, function):
    # http://vocab.nerc.ac.uk/collection/L20/current/
    df_out = deepcopy(df_in)
    df_out["bool"] = function(df_out["result"].array)
    df_out.loc[df_out["bool"], "qc_flag"] = QualityFlags.PROBABLY_GOOD
    df_out.loc[~df_out["bool"], "qc_flag"] = QualityFlags.PROBABLY_BAD
    return df_out


def qc_on_df(df: pd.DataFrame, cfg: dict[str, dict]) -> pd.DataFrame:
    df_out = deepcopy(df)
    df_out["bool"] = None
    df_out["qc_flag"] = None
    for _, row in (
        df_out[["datastream_id", "units", "observation_type"]]
        .drop_duplicates()
        .iterrows()
    ):
        d_id_i, u_i, ot_i = row.values
        df_sub = df_out.loc[df_out["datastream_id"] == d_id_i]
        cfg_ds_i = cfg.get("QC", {}).get(ot_i, {})
        if cfg_ds_i:
            min_, max_ = cfg_ds_i.get(
                "range"
            )  # type:ignore  Don't know why this is an issue
            function_i = partial(min_max_check_values, min_=min_, max_=max_)
            df_sub = qc_df(df_sub, function_i)
            df_out.loc[df_sub.index] = df_sub
    return df_out


def df_type_conversions(df):
    df_out = deepcopy(df)
    for ci in ["observation_type", "units", "qc_flag"]:
        mu0 = df_out[[ci]].memory_usage().get(ci)
        df_out[ci] = df_out[ci].astype("category")
        mu1 = df_out[[ci]].memory_usage().get(ci)
        if mu1 > mu0:
            log.warning("df type conversion might not reduce the memory usage!")

    for ci in ["bool"]:
        df_out[ci] = df_out[ci].astype("bool")

    return df_out
