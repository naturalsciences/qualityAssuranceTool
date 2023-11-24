import json
import logging
from collections import Counter
from typing import List, Literal, Tuple
from functools import partial

import pandas as pd
from requests import get, post
from stapy import Entity, Query

from models.enums import (
    Df,
    Entities,
    Filter,
    Order,
    OrderOption,
    Properties,
    Qactions,
    Settings,
)
from services.config import filter_cfg_to_query
from services.df import (
    df_type_conversions,
    features_request_to_df,
    response_single_datastream_to_df,
)
from utils.utils import (
    convert_to_datetime,
    get_absolute_path_to_base,
    log,
    series_to_patch_dict,
    update_response,
)

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


# not used
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
            Properties.RESULT,
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


def get_observations_count_thing_query(
    entity_id: int,
    filter_condition: str = "",
    skip_n: int = 0,
) -> Literal:
    # https://dev-sensors.naturalsciences.be/sta/v1.1/Things(1)/Datastreams?$expand=Observations($count=True;$top=0)&$select=Observations/@iot.count
    # expand_list = [
        # Entities.OBSERVATIONS(
            # [
                # Filter.FILTER(filter_condition),
                # Settings.COUNT("true"),
                # Settings.TOP(0),
            # ]
        # )
    # ]
    Q = Qactions.EXPAND(
        [
            Entities.DATASTREAMS(
                [
                    Settings.SKIP(skip_n),
                    Qactions.EXPAND(
                        [
                            Entities.OBSERVATIONS,
                        ]
                    ),
                    Qactions.SELECT(
                        [
                            "Observations/@iot.count",
                        ]
                    ),
                    # Qactions.EXPAND(expand_list),
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
 

def get_results_n_datastreams_query(
    entity_id: int,
    n: int | None = None,
    skip: int | None = None,
    top_observations: int | None = None,
    filter_condition: str = "",
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
                        Properties.RESULT,
                        Properties.PHENOMENONTIME,
                        Properties.QC_FLAG,
                    ]
                ),
                Qactions.EXPAND(
                    [
                        Entities.FEATUREOFINTEREST(
                            [
                                Qactions.SELECT(
                                    [Properties.COORDINATES, Properties.IOT_ID]
                                )
                            ]
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
    log.debug(f"Request {Q}")
    request = get_request(Q)
    # request = json.loads(Query(Entity.Thing).get_with_retry(complete_query).content)

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


def response_datastreams_to_df(response: dict) -> pd.DataFrame:
    df_out = pd.DataFrame()
    for ds_i in response[Entities.DATASTREAMS]:
        nextLink = ds_i.get(f"{Entities.OBSERVATIONS}@iot.nextLink", None)
        if nextLink:
            log.warning("Not all observations are extracted!")  # TODO: follow link!
        # df_i = datastreams_response_to_df(ds_i)
        df_i = response_single_datastream_to_df(ds_i)
        df_out = df_type_conversions(pd.concat([df_out, df_i], ignore_index=True))
    return df_out


# def get_datastream_based_observations_batch(datastreams: Sequence) -> pd.DataFrame:
#     df_out = pd.DataFrame()
#     id = 1
#     request = ""
#     d_batch = {
#         "id": id,
#         "atomicityGroup": f"Get_{id}",
#         "method": "get",
#         "url": request,
#     }
#     return df_out


def get_all_data(thing_id: int, filter_cfg: str):
    log.info(f"Retrieving data of Thing {thing_id}.")
    log.debug("Get all data of thing {thing_id} with filter {filter_cfg}")

    observations_count = 0
    skip_streams = 0
    query_observations_count = get_observations_count_thing_query(entity_id=thing_id, filter_condition=filter_cfg, skip_n=skip_streams)
    while query_observations_count:
        _, response_observations_count = get_results_n_datastreams(query_observations_count)
        observations_count += sum([ds_i["Observations@iot.count"] for ds_i in response_observations_count["Datastreams"]])
        skip_streams += len(response_observations_count["Datastreams"])
        query_observations_count = get_observations_count_thing_query(entity_id=thing_id, filter_condition=filter_cfg, skip_n=skip_streams)

    status_code, response = 0, {}
    query = get_results_n_datastreams_query(
        entity_id=thing_id, filter_condition=filter_cfg
    )

    status_code, response_i = get_results_n_datastreams(query)
    response = update_response(response, response_i)
    query = response_i.get(Entities.DATASTREAMS + "@iot.nextLink", None)

    while query:
        status_code, response_i = get_results_n_datastreams(query)
        if status_code != 200.0:
            raise RuntimeError(f"response with status code {status_code}.")
        # response[Entities.DATASTREAMS] = update_response(response.get(Entities.DATASTREAMS, []), response_i)
        response[Entities.DATASTREAMS] = (
            response.get(Entities.DATASTREAMS, []) + response_i["value"]
        )

        query = response_i.get("@iot.nextLink", None)
        response[Entities.DATASTREAMS + "@iot.nextLink"] = str(query)

    count_observations = 0
    for ds_i in response.get(Entities.DATASTREAMS, {}):  # type: ignore
        query = ds_i.get(Entities.OBSERVATIONS + "@iot.nextLink", None)
        while query:
            log.debug(
                f"Number of observations: {count_observations + len(ds_i[Entities.OBSERVATIONS])}"
            )
            status_code, response_i = get_results_n_datastreams(query)

            ds_i[Entities.OBSERVATIONS] = (
                ds_i.get(Entities.OBSERVATIONS, []) + response_i["value"]
            )
            query = response_i.get("@iot.nextLink", None)
            ds_i[Entities.OBSERVATIONS + "@iot.nextLink"] = query
        count_observations += len(ds_i[Entities.OBSERVATIONS])
        if len(ds_i[Entities.OBSERVATIONS]) > 0:
            log.info(f"Number of observations: {count_observations}")

    df_out = response_datastreams_to_df(response)
    log.info(f"Constructed dataframe of thing {thing_id}: {df_out.shape=}")
    return df_out


# def get_all_datastreams_data(
#     thing_id, nb_streams_per_call, top_observations, filter_cfg
# ) -> pd.DataFrame:
#     df_all = pd.DataFrame()
#     nb_datastreams = get_nb_datastreams_of_thing(thing_id=thing_id)
#     log.info(f"{nb_datastreams=}")
#     for i in range(ceil(nb_datastreams / nb_streams_per_call)):
#         log.info(f"nb {i} of {ceil(nb_datastreams/nb_streams_per_call)}")
#         query = get_results_n_datastreams_query(
#             entity_id=thing_id,
#             n=nb_streams_per_call,
#             skip=nb_streams_per_call * i,
#             top_observations=top_observations,
#             filter_condition=filter_cfg,
#             # expand_feature_of_interest=True,
#         )
#         status_code, response = get_results_n_datastreams(query)
#         if status_code != 200:
#             raise IOError(f"Status code: {status_code}")
#         df_response = response_datastreams_to_df(response)
#         df_all = pd.concat([df_all, df_response], ignore_index=True)
#         log.debug(f"DF_ALL shape {df_all.shape}")
#         # for ds_i in response[Entities.DATASTREAMS]:
#         #     if f"{Entities.OBSERVATIONS}@iot.nextLink" in ds_i:
#         #         log.warning("Not all observations are extracted!")  # TODO: follow link!
#         # df_i = datastreams_request_to_df(response[Entities.DATASTREAMS])
#         # log.debug(f"{df_i.shape[0]=}")
#         # df_all = pd.concat([df_all, df_i], ignore_index=True)
#     return df_all


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


def patch_qc_flags(
    df: pd.DataFrame,
    url: str,
    auth: Tuple[str, str] | None = None,
    columns: List[Df] = [Df.IOT_ID, Df.QC_FLAG],
    url_entity: Entities = Entities.OBSERVATIONS,
    json_body_template: str | None = None,
) -> Counter:
    df["patch_dict"] = df[columns].apply(
        partial(
            series_to_patch_dict,
            columns=columns,
            url_entity=url_entity,
            json_body_template=json_body_template,
        ),
        axis=1,
    )

    final_json = {"requests": df["patch_dict"].to_list()}
    log.info("Start batch patch query")
    response = post(
        headers={"Content-Type": "application/json"},
        url=url,
        data=json.dumps(final_json),
        auth=auth,
    )
    try:
        responses = response.json()["responses"]
    except:
        log.error("Something went wrong while posing. Is there a valid authentication?")
        log.error(f"{response}")
        raise IOError("{response}")

    count_res = Counter([ri["status"] for ri in responses])
    log.info("End batch patch query")
    log.info(f"{json.dumps(count_res)}")
    return count_res


def get_elev_netcdf() -> None:
    url_ETOPO = "https://www.ngdc.noaa.gov/thredds/fileServer/global/ETOPO2022/60s/60s_bed_elev_netcdf/ETOPO_2022_v1_60s_N90W180_bed.nc"
    filename_ETOPO = url_ETOPO.rsplit("/", 1)[1]
    local_file = (
        get_absolute_path_to_base().joinpath("resources").joinpath(filename_ETOPO)
    )

    if not local_file.exists():
        log.info("Downloading netCDF elevation file.")
        r = get(url_ETOPO, stream=True)
        with open(local_file, "wb") as f:
            for chunk in r.iter_content():
                f.write(chunk)
        log.info("Download completed.")
