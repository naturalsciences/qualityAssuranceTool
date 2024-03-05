import json
import logging
from collections import Counter
from functools import partial
from typing import List, Literal, Tuple

import pandas as pd
from requests import get, post
from stapy import Entity, Query, config
from tqdm import tqdm

from models.constants import TQDM_BAR_FORMAT, TQDM_DESC_FORMAT
from models.enums import Df, Entities
from models.enums import Entity as Entity_m
from models.enums import Filter, Order, OrderOption, Properties, Qactions
from models.enums import Query as Query_m
from models.enums import Settings
from services.df import df_type_conversions, response_single_datastream_to_df
from utils.utils import (convert_to_datetime, get_absolute_path_to_base, log,
                         series_to_patch_dict, update_response)

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
) -> Literal["str"]:
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
) -> str:
    observations = Entity_m(Entities.OBSERVATIONS)
    observations.settings = [Settings.COUNT("True")]
    observations.filter = filter_condition
    datastreams = Entity_m(Entities.DATASTREAMS)
    datastreams.settings = [Settings.SKIP(skip_n)]
    datastreams.selection = [Properties.OBSERVATIONS_COUNT]
    datastreams.expand = [observations]

    thing = Entity_m(Entities.THINGS)
    thing.id = entity_id
    thing.expand = [datastreams]
    thing.selection = [Entities.DATASTREAMS]
    query = Query_m(base_url=config.load_sta_url(), root_entity=thing)
    query_http = query.build()

    return query_http


def get_results_n_datastreams_query(
    entity_id: int,
    n: int | None = None,
    skip: int | None = None,
    top_observations: int | None = None,
    filter_condition: str = "",
    expand_feature_of_interest: bool = True,
) -> str:
    obs = Entity_m(Entities.OBSERVATIONS)
    obs.filter = filter_condition
    obs.settings = [Settings.TOP(top_observations)]
    obs.selection = [
        Properties.IOT_ID,
        Properties.RESULT,
        Properties.PHENOMENONTIME,
        Properties.QC_FLAG,
    ]
    foi = Entity_m(Entities.FEATUREOFINTEREST)
    foi.selection = [Properties.COORDINATES, Properties.IOT_ID]
    if expand_feature_of_interest:
        obs.expand = [foi]

    obsprop = Entity_m(Entities.OBSERVEDPROPERTY)
    obsprop.selection = [Properties.IOT_ID, Properties.NAME]

    ds = Entity_m(Entities.DATASTREAMS)
    ds.settings = [Settings.TOP(n), Settings.SKIP(skip)]
    ds.selection = [
        Properties.IOT_ID,
        Properties.UNITOFMEASUREMENT,
        Entities.OBSERVATIONS,
    ]
    ds.expand = [obs, obsprop]

    thing = Entity_m(Entities.THINGS)
    thing.id = entity_id
    thing.expand = [ds]
    thing.selection = [Entities.DATASTREAMS]
    query = Query_m(base_url=config.load_sta_url(), root_entity=thing)
    query_http = query.build()

    return query_http


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
    log.info("Building dataframe.")
    df_out = pd.DataFrame()
    for ds_i in response[Entities.DATASTREAMS]:
        nextLink = ds_i.get(f"{Entities.OBSERVATIONS}@iot.nextLink", None)
        if nextLink:
            log.warning("Not all observations are extracted!")  # TODO: follow link!
        # df_i = datastreams_response_to_df(ds_i)
        df_i = response_single_datastream_to_df(ds_i)
        df_out = df_type_conversions(pd.concat([df_out, df_i], ignore_index=True))
    log.info(f"Dataframe constructed with {df_out.shape[0]} rows.")
    return df_out


def get_total_observations_count(thing_id: int, filter_cfg: str) -> int:
    total_observations_count = 0
    skip_streams = 0
    query_observations_count = get_observations_count_thing_query(
        entity_id=thing_id, filter_condition=filter_cfg, skip_n=skip_streams
    )
    bool_nextlink = True
    while bool_nextlink:
        _, response_observations_count = get_results_n_datastreams(
            query_observations_count
        )
        total_observations_count += sum(
            [
                ds_i["Observations@iot.count"]
                for ds_i in response_observations_count["Datastreams"]
            ]
        )
        skip_streams += len(response_observations_count["Datastreams"])
        query_observations_count = get_observations_count_thing_query(
            entity_id=thing_id, filter_condition=filter_cfg, skip_n=skip_streams
        )
        bool_nextlink = response_observations_count.get(
            "Datastreams@iot.nextLink", False
        )
        log.debug(f"temp count: {total_observations_count=}")
    log.info(
        f"Total number of observations to be retrieved: {total_observations_count}"
    )
    return total_observations_count


def get_query_response(
    query: str, total_count: int | None = None, follow_obs_nextlinks: bool = True
) -> dict:
    status_code, response = 0, {}

    status_code, response_i = get_results_n_datastreams(query)
    response = update_response(response, response_i)
    # follow nextLinks (Datastreams)
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
    if total_count:
        pbar = tqdm(
            total=total_count,
            desc=TQDM_DESC_FORMAT.format("Observations count"),
            bar_format=TQDM_BAR_FORMAT,
        )
    count_observations = 0
    for ds_i in response.get(Entities.DATASTREAMS, {}):  # type: ignore
        query = ds_i.get(Entities.OBSERVATIONS + "@iot.nextLink", None)
        query = [None, query][follow_obs_nextlinks]
        while query:
            retrieved_nb_observations = count_observations + len(
                ds_i[Entities.OBSERVATIONS]
            )
            log.debug(f"Number of observations: {retrieved_nb_observations}")

            status_code, response_i = get_results_n_datastreams(query)

            ds_i[Entities.OBSERVATIONS] = (
                ds_i.get(Entities.OBSERVATIONS, []) + response_i["value"]
            )
            query = response_i.get("@iot.nextLink", None)
            query = [None, query][follow_obs_nextlinks]
            ds_i[Entities.OBSERVATIONS + "@iot.nextLink"] = query
        count_observations += len(ds_i[Entities.OBSERVATIONS])
        if total_count:
            pbar.update(len(ds_i[Entities.OBSERVATIONS]))
    if total_count:
        pbar.close()

    return response


def get_all_data(thing_id: int, filter_cfg: str):
    log.info(f"Retrieving data of Thing {thing_id}.")
    log.info(f"---- filter: {filter_cfg}")
    log.debug("Get all data of thing {thing_id} with filter {filter_cfg}")

    # get total count of observations to be retrieved
    total_observations_count = get_total_observations_count(
        thing_id=thing_id, filter_cfg=filter_cfg
    )

    # get the actual data
    query = get_results_n_datastreams_query(
        entity_id=thing_id, filter_condition=filter_cfg
    )

    # TODO: refactor:
    #       due to different response from query constructed as above and iot.nextLink, not possible in same loop
    #       should rewrite code to get consistent result?
    #       might not be possible as initial query return complete structure
    response = get_query_response(query, total_count=total_observations_count)

    df_out = response_datastreams_to_df(response)

    if df_out.isna().any().any():
        log.warning(f"The dataframe has NAN values.")
    if df_out.empty:
        log.warning(f"No data retrieved.")
        return df_out
    log.info(
        f"Quality flag counts as downloaded: {df_out[Df.QC_FLAG].value_counts(dropna=False).to_json()}"
    )
    log.debug(f"Columns of constructed df: {df_out.columns}.")
    log.debug(f"Datastreams observation types: {df_out[Df.OBSERVATION_TYPE].unique()}")
    return df_out


# NOT used, keep for later reference
# def get_features_of_interest(filter_cfg, top_observations):
#     filter_condition = filter_cfg_to_query(filter_cfg)
#     base_query = Query(Entity.FeatureOfInterest).get_query()
#     complete_query = (
#         base_query
#         + "?"
#         + Qactions.SELECT(
#             [Properties.IOT_ID, "feature/coordinates", Entities.OBSERVATIONS]
#         )
#         + "&"
#         + Qactions.EXPAND(
#             [
#                 Entities.OBSERVATIONS(
#                     [
#                         Qactions.SELECT([Properties.IOT_ID]),
#                         Settings.TOP(top_observations),
#                     ]
#                 )
#             ]
#         )
#     )
#     complete_query += "&" + Settings.TOP(top_observations)
#     log.info("Start request features")
#     log.debug(f"{complete_query}")
#     request_features = json.loads(
#         Query(Entity.FeatureOfInterest).get_with_retry(complete_query).content
#     )
#     log.info("End request features")
#
#     df_features = features_request_to_df(request_features)
#     features_observations_dict = {
#         fi.get(Properties.IOT_ID): [
#             oi.get(Properties.IOT_ID) for oi in fi.get(Entities.OBSERVATIONS)
#         ]
#         for fi in request_features["value"]
#     }
#     # possible to write to pickle?
#     # how to test if needed or not?
#     return features_observations_dict


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
    if df.empty:
        log.warning("No ouliers are flagged (empty DataFrame).")
        return Counter([])
    df["patch_dict"] = df[columns].apply(
        partial(
            series_to_patch_dict,
            columns=columns,
            url_entity=url_entity,
            json_body_template=json_body_template,
        ),
        axis=1,
    )
    if df["patch_dict"].empty:
        log.warning("Nothing to patch.")
        return Counter([])

    final_json = {"requests": df["patch_dict"].to_list()}
    log.info(f"Start batch patch query {url_entity}.")
    response = post(
        headers={"Content-Type": "application/json"},
        url=url,
        data=json.dumps(final_json),
        auth=auth,
    )
    try:
        responses = response.json()["responses"]
    except:
        log.error(
            "Something went wrong while posting. Is there a valid authentication?"
        )
        log.error(f"{response}")
        raise IOError("{response}")

    count_res = Counter([ri["status"] for ri in responses])
    log.info("End batch patch query")
    # log.info(f"{json.dumps(count_res)}")
    if set(count_res.keys()) != set(
        [
            200,
        ]
    ):
        log.error("Didn't succeed patching.")
    return count_res


import io


def download_as_bytes_with_progress(url: str) -> bytes:
    resp = get(url, stream=True)
    total = int(resp.headers.get("content-length", 0))
    bio = io.BytesIO()
    with tqdm(
        desc=TQDM_DESC_FORMAT.format("File download"),
        bar_format=TQDM_BAR_FORMAT,
        total=total,
        unit="b",
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for chunk in resp.iter_content(chunk_size=65536):
            bar.update(len(chunk))
            bio.write(chunk)
    return bio.getvalue()


def get_elev_netcdf() -> None:
    url_ETOPO = "https://www.ngdc.noaa.gov/thredds/fileServer/global/ETOPO2022/60s/60s_bed_elev_netcdf/ETOPO_2022_v1_60s_N90W180_bed.nc"
    filename_ETOPO = url_ETOPO.rsplit("/", 1)[1]
    local_file = (
        get_absolute_path_to_base().joinpath("resources").joinpath(filename_ETOPO)
    )

    if not local_file.exists():
        log.info("Downloading netCDF elevation file.")
        log.info(f"  file: {local_file}")
        r = get(url_ETOPO, stream=True)
        with open(local_file, "wb") as f:
            f.write(download_as_bytes_with_progress(url_ETOPO))
        log.info("Download completed.")
