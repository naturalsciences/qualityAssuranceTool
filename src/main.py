import hydra
from stapy import Query, Entity
import stapy
import copy
import json
import numpy as np
import pandas as pd
import logging
from functools import partial
from math import ceil
from datetime import datetime
from copy import deepcopy
from pathlib import Path
import requests
from collections import Counter

from models.enums import (
    Properties,
    Settings,
    Entities,
    Qactions,
    QualityFlags,
)
from models.constants import ISO_STR_FORMAT, ISO_STR_FORMAT2
from api.observations_api import (
    get_results_n_datastreams,
    get_results_n_datastreams_query,
    datastreams_request_to_df
)
from utils.utils import get_datetime_latest_observation


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



# def features_to_global_df(
#     features_dict: dict[int, list[int]], df: pd.DataFrame
# ) -> pd.DataFrame:
#     df_out = df.set_index(Properties.IOT_ID)
#     i = 0
#     for k, v in features_dict.items():
#         log.info(f"{i}/{len(features_dict)}")
#         existing_indices = df_out.index.intersection(v)
#         # df_out.loc[existing_indices] = k
#         i += 1
#     return df_out


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


def get_all_datastreams_data(thing_id, nb_streams_per_call, top_observations, filter_cfg) -> pd.DataFrame:
    df_all = pd.DataFrame()
    
    nb_datastreams = get_nb_datastreams_of_thing(thing_id = thing_id)
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
            min_, max_ = cfg_ds_i.get("range") #type:ignore  Don't know why this is an issue
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

    return df_out

    
def patch_qc_flags(df: pd.DataFrame, url) -> Counter:
    df["patch_dict"] = df[[Properties.IOT_ID, "qc_flag"]].apply(
        series_to_patch_dict, axis=1
    )

    final_json = {"requests": df["patch_dict"].to_list()}
    log.info("Start batch patch query")
    response = requests.post(
        headers={"Content-Type": "application/json"},
        url="http://localhost:8080/FROST-Server/v1.1/$batch",
        data=json.dumps(final_json),
    )
    count_res = Counter([ri["status"] for ri in response.json()["responses"]])
    log.info("End batch patch query")
    log.info(f"{count_res}")
    return count_res

@hydra.main(config_path="../conf", config_name="config.yaml", version_base="1.2")
def main(cfg):
    log.info("Start")
    stapy.set_sta_url(cfg.data_api.base_url)

    thing_id = cfg.data_api.things.id

    nb_streams_per_call = cfg.data_api.datastreams.top
    top_observations = cfg.data_api.observations.top
    filter_cfg = cfg.data_api.get("filter", {})

    
    df_all = get_all_datastreams_data(thing_id=thing_id, nb_streams_per_call=nb_streams_per_call, top_observations=top_observations, filter_cfg=filter_cfg)
   
    df_all = qc_on_df(df_all, cfg=cfg)

    df_all = df_type_conversions(df_all)
    
    url="http://localhost:8080/FROST-Server/v1.1/$batch"
    counter = patch_qc_flags(df_all, url=url)

    print(f"{counter=}")
    print(f"{df_all.shape=}")
    print(f"{df_all.dtypes=}")

    log.info("End")


if __name__ == "__main__":
    log.debug("testing...")
    main()
