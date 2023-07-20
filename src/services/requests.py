from collections import Counter
import logging
from math import ceil
import json
from typing import Tuple, Literal
import pandas as pd
from requests import post

from stapy import Entity, Query

from models.enums import (
    Entities,
    Filter,
    Order,
    OrderOption,
    Properties,
    Qactions,
    Settings,
)
from services.config import filter_cfg_to_query
from services.df import datastreams_response_to_df, features_request_to_df, response_single_datastream_to_df
from utils.utils import convert_to_datetime, log, series_to_patch_dict


log = logging.getLogger(__name__)


def build_query_datastreams(entity_id: int) -> str:
    base_query = Query(Entity.Thing).entity_id(entity_id)
    out_query = base_query.select(
        Properties.NAME, Properties.IOT_ID, Entities.DATASTREAMS
    )
    additional_query = Qactions.EXPAND(
        [
            Entities.DATASTREAMS(
                [
                    Settings.COUNT("true"),
                    Qactions.EXPAND(
                        [
                            Entities.OBSERVEDPROPERTY(
                                [Qactions.SELECT([Properties.NAME, Properties.IOT_ID])]
                            ),
                            Entities.OBSERVATIONS(
                                [
                                    Settings.COUNT("true"),
                                    Qactions.SELECT([Properties.IOT_ID]),
                                    Settings.TOP(0),
                                ]
                            ),
                        ]
                    ),
                    Qactions.SELECT(
                        [
                            Properties.NAME,
                            Properties.IOT_ID,
                            Properties.DESCRIPTION,
                            Properties.UNITOFMEASUREMENT,
                            Entities.OBSERVEDPROPERTY,
                        ]
                    ),
                ]
            )
        ]
    )
    return out_query.get_query() + "&" + additional_query


def get_request(query: str) -> Tuple[int, dict]:
    request = Query(Entity.Thing).entity_id(0).get_with_retry(query)
    request_out = request.json()
    return request.status_code, request_out


def build_query_observations(
    filter_conditions: str | None,
    top_observations: int,
    expand_feature_of_interest: bool = True,
) -> Literal:
    Q_filter = ""
    if filter_conditions:
        Q_filter = "&" + Filter.FILTER(filter_conditions)
    Q_select = "&" + Qactions.SELECT(
        [
            Properties.IOT_ID,
            "result",
            Properties.PHENOMENONTIME,
            Entities.FEATUREOFINTEREST,
        ]
    )
    Q_exp = ""
    if expand_feature_of_interest:
        Q_exp = "&" + Qactions.EXPAND([Entities.FEATUREOFINTEREST])

    Q_out = (
        Query(Entity.Observation)
        .limit(top_observations)
        .select(Entities.FEATUREOFINTEREST)
        .get_query()
        + Q_filter
        + Q_select
        + Q_exp
    )
    return Q_out


def get_results_n_datastreams_query(
    entity_id: int,
    n: int,
    skip: int,
    top_observations: int,
    filter_condition: str,
    expand_feature_of_interest: bool = True,
) -> Literal:
    # TODO: cleanup!!
    idx_slice: int = 3
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
    nb_datastreams = (
        (
            Query(Entity.Datastream).get_with_retry(
                base_query.get_query() + "&" + add_query_nb
            )
        )
        .json()
        .get("Datastreams@iot.count")
    )
    return nb_datastreams


def response_observations_to_df(response: dict) -> pd.DataFrame:
    df_out = pd.DataFrame()
    
    return df_out


def response_datastreams_to_df(response: dict) -> pd.DataFrame:
    df_out = pd.DataFrame()
    for ds_i in response[Entities.DATASTREAMS]:
        if f"{Entities.OBSERVATIONS}@iot.nextLink" in ds_i:
            log.warning("Not all observations are extracted!")  # TODO: follow link!
        # df_i = datastreams_response_to_df(ds_i)
        df_i = response_single_datastream_to_df(ds_i)
        df_out = pd.concat([df_out, df_i], ignore_index=True)
    return df_out


def get_all_datastreams_data(
    thing_id, nb_streams_per_call, top_observations, filter_cfg
) -> pd.DataFrame:
    df_all = pd.DataFrame()
    nb_datastreams = get_nb_datastreams_of_thing(thing_id=thing_id)
    log.info(f"{nb_datastreams=}")
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
        df_response = response_datastreams_to_df(response)
        df_all = pd.concat([df_all, df_response], ignore_index=True)
        log.info(f"DF_ALL shape {df_all.shape}")
        # for ds_i in response[Entities.DATASTREAMS]:
        #     if f"{Entities.OBSERVATIONS}@iot.nextLink" in ds_i:
        #         log.warning("Not all observations are extracted!")  # TODO: follow link!
        # df_i = datastreams_request_to_df(response[Entities.DATASTREAMS])
        # log.debug(f"{df_i.shape[0]=}")
        # df_all = pd.concat([df_all, df_i], ignore_index=True)
    return df_all


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


def patch_qc_flags(df: pd.DataFrame, url) -> Counter:
    df["patch_dict"] = df[[Properties.IOT_ID, "qc_flag"]].apply(
        series_to_patch_dict, axis=1
    )

    final_json = {"requests": df["patch_dict"].to_list()}
    log.info("Start batch patch query")
    response = post(
        headers={"Content-Type": "application/json"},
        url=url,
        data=json.dumps(final_json),
    )
    count_res = Counter([ri["status"] for ri in response.json()["responses"]])
    log.info("End batch patch query")
    log.info(f"{count_res}")
    return count_res