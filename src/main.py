import hydra
from stapy import Query, Entity, Patch, config
import stapy
import copy
import json
import numpy as np
import pandas as pd
import logging
from functools import partial
from typing import Callable
from math import ceil
from datetime import datetime, timedelta
import pickle
import os.path
from pathlib import Path
import requests
from collections import Counter
import time

from models.enums import (
    Properties,
    Settings,
    Entities,
    Qactions,
    Filter,
    Order,
    OrderOption,
    QualityFlags,
)
from models.constants import ISO_STR_FORMAT, ISO_STR_FORMAT2
from api.observations_api import (
    get_results_n_datastreams,
    filter_cfg_to_query,
    get_features_of_interest,
    get_results_n_datastreams_query,
)


# Type hinting often ignored
# name and COUNT are probably *known* variables names of python property
# might be solved with _name, _count, or NAME, COUNT. when all caps is used, the __str__ will need to be changed to lower
# doesn't work because other ARE with camelcase


def min_max_check_values(values: pd.DataFrame, min_: float, max_: float):
    out = np.logical_and(values >= min_, values <= max_)
    return out


def qc_df(df_in, function):
    # http://vocab.nerc.ac.uk/collection/L20/current/
    df_out = copy.deepcopy(df_in)
    df_out["bool"] = function(df_out["result"].array)
    df_out.loc[df_out["bool"], "qc_flag"] = QualityFlags.PROBABLY_GOOD
    df_out.loc[~df_out["bool"], "qc_flag"] = QualityFlags.PROBABLY_BAD
    return df_out


# def qc_observation(iot_id: int, function: Callable):
#     log.info(f"start qc {iot_id}")
#     id_list, result_list = get_id_result_lists(iot_id)
#     df_ = pd.DataFrame.from_dict(
#         {Properties.IOT_ID: id_list, "result": result_list}
#     ).astype({Properties.IOT_ID: int, "result": float})
#     return qc_df(df_, function)


def convert_to_datetime(value):
    try:
        d_out = datetime.strptime(value, ISO_STR_FORMAT)
    except ValueError:
        d_out = datetime.strptime(value, ISO_STR_FORMAT2)
    return d_out


def datastreams_request_to_df(request_datastreams):
    df = pd.DataFrame()
    for di in request_datastreams:
        data_coordinates = di.get(Entities.FEATUREOFINTEREST, {})
        if data_coordinates:
            del di[Entities.FEATUREOFINTEREST]
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
            # df_i[["long", "lat"]] = pd.DataFrame.from_records(df_i[str(Entities.FEATUREOFINTEREST)].apply(
            #     lambda x: x.get('feature').get('coordinates')))
            # df_i.drop(columns=str(Entities.FEATUREOFINTEREST))
            df = pd.concat([df, df_i], ignore_index=True)

    return df


def get_datetime_latest_observation():
    query = (
        Query(Entity.Observation).get_query()
        + "?"
        + Order.ORDERBY(Properties.PHENOMENONTIME, OrderOption.DESC)  # type: ignore
        + "&"
        + Settings.TOP(1)
        + "&"
        + Qactions.SELECT([Properties.PHENOMENONTIME])
    )  # type:ignore
    request = json.loads(Query(Entity.Observation).get_with_retry(query).content)
    # https://sensors.naturalsciences.be/sta/v1.1/OBSERVATIONS?$ORDERBY=phenomenonTime%20desc&$TOP=1&$SELECT=phenomenonTime
    latest_phenomenonTime = convert_to_datetime(
        request["value"][0].get(Properties.PHENOMENONTIME)
    )
    return latest_phenomenonTime


def features_to_global_df(
    features_dict: dict[int, list[int]], df: pd.DataFrame
) -> pd.DataFrame:
    df_out = df.set_index(Properties.IOT_ID)
    i = 0
    for k, v in features_dict.items():
        log.info(f"{i}/{len(features_dict)}")
        existing_indices = df_out.index.intersection(v)
        # df_out.loc[existing_indices] = k
        i += 1
    return df_out


log = logging.getLogger(__name__)


# def test_patch_single(id, value):
#     a = Patch.observation(entity_id=id, result_quality=str(value))
#     return a


def series_to_patch_dict(x, group_per_x=1000):
    # qc_fla is hardcoded!
    # atomicityGroup seems to improve performance, but amount of groups seems irrelevant (?)
    # UNLESS multiple runs are done simultaneously?
    d_out = {
        "id": str(x.name + 1),
        "atomicityGroup": f"Group{(int(x.name/group_per_x)+1)}",
        "method": "patch",
        "url": f"Observations({x.get(Properties.IOT_ID)})",
        "body": {"resultQuality": str(x.get("qc_flag"))},
    }
    return d_out


def compose_batch_qc_patch(df, col_id, col_qc):
    df_ = df[[col_id, col_qc]].convert_dtypes(convert_string=True)
    body = json.dumps(df_.to_json(orient="records"))
    pass


@hydra.main(config_path="../conf", config_name="config.yaml", version_base="1.2")
def main(cfg):
    log.info("Start")
    stapy.set_sta_url(cfg.data_api.base_url)

    thing_id = cfg.data_api.things.id

    nb_streams_per_call = cfg.data_api.datastreams.top
    top_observations = cfg.data_api.observations.top
    filter_cfg = cfg.data_api.get("filter", {})

    features_file = Path(cfg.other.pickle.path)
    recreate_features_file = True
    if features_file.exists():
        latest_time = get_datetime_latest_observation()
        mod_time = datetime.fromtimestamp(os.path.getmtime(features_file))
        if mod_time > latest_time:
            recreate_features_file = False
    if recreate_features_file:
        # filter NOT USED!!
        feature_dict = get_features_of_interest(filter_cfg, top_observations)
        with open(features_file, "wb") as f:
            pickle.dump(feature_dict, f)
    else:
        with open(features_file, "rb") as f:
            feature_dict = pickle.load(f)

    base_query = (
        Query(Entity.Thing).entity_id(thing_id).select("Datastreams/@iot.count")
    )
    # summary = inspect_datastreams_thing(1)

    df_all = pd.DataFrame()
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
    log.debug(f"{nb_datastreams=}")
    for i in range(ceil(nb_datastreams / nb_streams_per_call)):
        log.info(f"nb {i} of {ceil(nb_datastreams/nb_streams_per_call)}")
        query = get_results_n_datastreams_query(
            entity_id=thing_id,
            n=nb_streams_per_call,
            skip=nb_streams_per_call * i,
            top_observations=top_observations,
            filter_condition=filter_cfg,
        )
        status_code, response = get_results_n_datastreams(query)
        for ds_i in response[Entities.DATASTREAMS]:  # type:ignore
            if f"{Entities.OBSERVATIONS}@iot.nextLink" in ds_i:
                log.warning("Not all observations are extracted!")  # TODO: follow link!
        df_i = datastreams_request_to_df(response[Entities.DATASTREAMS])  # type: ignore
        log.debug(f"{df_i.shape[0]=}")
        df_all = pd.concat([df_all, df_i], ignore_index=True)
    log.debug(f"{df_all.shape=}")

    log.debug("done with df_all")

    log.info("Start features to global df")
    df_out = features_to_global_df(feature_dict, df_all)
    log.info("End features to global df")
    df_all["bool"] = None
    df_all["qc_flag"] = None
    for _, row in (
        df_all[["datastream_id", "units", "observation_type"]]
        .drop_duplicates()
        .iterrows()
    ):
        d_id_i, u_i, ot_i = row.values
        df_sub = df_all.loc[df_all["datastream_id"] == d_id_i]
        cfg_ds_i = cfg.get("QC").get(ot_i, {})
        if cfg_ds_i:
            min_, max_ = cfg.get("QC").get(ot_i).get("range")
            function_i = partial(min_max_check_values, min_=min_, max_=max_)
            df_sub = qc_df(df_sub, function_i)
            df_all.loc[df_sub.index] = df_sub
    df_all["observation_type"] = df_all["observation_type"].astype("category")
    df_all["units"] = df_all["units"].astype("category")

    df_all["patch_dict"] = None
    df_all["path_dict"] = df_all["patch_dict"].astype("category")
    df_all["patch_dict"] = df_all[[Properties.IOT_ID, "qc_flag"]].apply(
        series_to_patch_dict, axis=1
    )

    final_json = {"requests": df_all["patch_dict"].to_list()}

    dict_jsons = {
        "final_json_15000": {"requests": df_all["patch_dict"].iloc[:15000].to_list()},
        "final_json_10000": {"requests": df_all["patch_dict"].iloc[:10000].to_list()},
        "final_json_05000": {"requests": df_all["patch_dict"].iloc[:5000].to_list()},
        "final_json_01000": {"requests": df_all["patch_dict"].iloc[:1000].to_list()},
        "final_json_00500": {"requests": df_all["patch_dict"].iloc[:500].to_list()},
    }
    # log.info("Start batch patch query")
    # response = requests.post(
    #     headers={"Content-Type": "application/json"},
    #     url="http://localhost:8080/FROST-Server/v1.1/$batch",
    #     data=json.dumps(final_json),
    # )
    # count_res = Counter([ri["status"] for ri in response.json()["responses"]])
    # log.info("End batch patch query")
    # log.info(f"{count_res}")

    for di in dict_jsons.keys():
        log.info(f"Start batch {di}")
        start_i = time.time()
        response_i = requests.post(
            headers={"Content-Type": "application/json"},
            url="http://localhost:8080/FROST-Server/v1.1/$batch",
            data=json.dumps(dict_jsons[di]),
        )
        end_i = time.time()
        count_res_i = Counter([ri["status"] for ri in response_i.json()["responses"]])

        log.info(f"End batch patch query {di}: {end_i-start_i}")
        log.info(f"{count_res_i}")

    for gp_i in [100, 500, 1000, 1500, 5000, 10000, 15000]:
        df_all["patch_dict"] = df_all[[Properties.IOT_ID, "qc_flag"]].apply(
            partial(series_to_patch_dict, group_per_x=gp_i), axis=1
        )
        used_json = {"requests": df_all["patch_dict"].iloc[:15000].to_list()}

        log.info(f"Start batch group_per_x: {gp_i}")
        start_i = time.time()
        response_i = requests.post(
            headers={"Content-Type": "application/json"},
            url="http://localhost:8080/FROST-Server/v1.1/$batch",
            data=json.dumps(used_json),
        )
        end_i = time.time()
        count_res_i = Counter([ri["status"] for ri in response_i.json()["responses"]])

        log.info(f"End batch patch query {gp_i}: {end_i-start_i}")
        log.info(f"{count_res_i}")

    # compose_batch_qc_patch(df_all.loc[0:10], Properties.IOT_ID, "qc_flag")
    print(f"{df_all.shape=}")

    log.info("End")


if __name__ == "__main__":
    log.debug("testing...")
    main()


# possibly faster?
# https://sensors.naturalsciences.be/sta/v1.1/OBSERVATIONS?$FILTER=phenomenonTime%20gt%202022-03-01T00:00:00Z%20and%20phenomenonTime%20lt%202022-04-01T00:00:00Z&$EXPAND=FEATUREOFINTEREST($SELECT=feature/coordinates)&$SELECT=FEATUREOFINTEREST/feature/coordinates,result
# https://sensors.naturalsciences.be/sta/v1.1/OBSERVATIONS?$FILTER=phenomenonTime%20gt%202022-03-01T00:00:00Z%20and%20phenomenonTime%20lt%202022-04-01T00:00:00Z&$EXPAND=FEATUREOFINTEREST($SELECT=feature/coordinates)&$SELECT=FEATUREOFINTEREST/feature/coordinates,result&$resultFormat=GeoJSON
