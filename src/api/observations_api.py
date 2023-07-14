import logging
from typing import Tuple, cast
from datetime import datetime
import pandas as pd
from stapy import Query, Entity
import json

from models.enums import Entities, Properties, Qactions, Settings, Filter
from models.constants import ISO_STR_FORMAT
from utils.utils import get_request


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
    expand_feature_of_interest=False,
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
    Q_out = Query(Entity.Thing).entity_id(entity_id).select(Entities.DATASTREAMS).get_query() + "&" +Q
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
