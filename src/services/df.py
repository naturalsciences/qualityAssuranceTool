import logging
from copy import deepcopy
from typing import Sequence

import numpy as np
import pandas as pd
from shapely.wkt import loads

from models.enums import Df, Entities, Properties
from services.regions_query import (build_points_query, build_query_points,
                                    connect)
from utils.utils import convert_to_datetime

log = logging.getLogger(__name__)


def df_type_conversions(df):
    df_out = deepcopy(df)
    list_columns = [Df.OBSERVATION_TYPE, Df.UNITS, Df.QC_FLAG, Df.REGION, Df.SUB_REGION]
    for ci in set(list_columns).intersection(df.columns):
        mu0 = df_out[[ci]].memory_usage().get(ci)
        df_out[ci] = df_out[ci].astype("category")
        mu1 = df_out[[ci]].memory_usage().get(ci)
        if mu1 > mu0:
            log.warning("df type conversion might not reduce the memory usage!")

    for ci in set(list_columns).intersection(["bool"]):
        df_out[ci] = df_out[ci].astype("bool")

    return df_out


def features_request_to_df(request_features):
    data = []
    for fi in request_features["value"]:
        v = fi.get(Properties.IOT_ID)
        long, lat = fi.get("feature").get("coordinates")
        idx = [oi.get(Properties.IOT_ID) for oi in fi.get(Entities.OBSERVATIONS)]
        for idx_i in idx:
            data.append([idx_i, v, long, lat])
    df = pd.DataFrame(data, columns=[Df.IOT_ID, "feature_id", Df.LONG, Df.LAT])
    return df


def response_obs_to_df(response_obs: dict) -> pd.DataFrame:
    # MISSING UNITS, TYPE, ...
    df = pd.DataFrame()
    df = pd.DataFrame(response_obs["value"]).astype(
        {Properties.IOT_ID: int, Df.RESULT: float}
    )
    df[Properties.PHENOMENONTIME] = df[Properties.PHENOMENONTIME].apply(
        convert_to_datetime
    )

    df[[Df.LONG, Df.LAT]] = pd.DataFrame.from_records(
        df[str(Entities.FEATUREOFINTEREST)].apply(
            lambda x: x.get("feature").get("coordinates")
        )
    )
    del df[str(Entities.FEATUREOFINTEREST)]

    return df


def response_single_datastream_to_df(response_datastream: dict) -> pd.DataFrame:
    df = pd.DataFrame()
    observations_list = response_datastream.get(Entities.OBSERVATIONS, [])
    if observations_list:
        df_i = pd.DataFrame(observations_list).astype(
            {Properties.IOT_ID: int, Df.RESULT: float}
        )
        df_i[Df.DATASTREAM_ID] = int(response_datastream.get(Properties.IOT_ID, -1))
        df_i[Properties.PHENOMENONTIME] = df_i[Properties.PHENOMENONTIME].apply(
            convert_to_datetime
        )
        df_i[Df.OBSERVATION_TYPE] = response_datastream.get(Entities.OBSERVEDPROPERTY, {}).get(
            Properties.NAME
        )
        df_i[Df.OBSERVATION_TYPE] = df_i[Df.OBSERVATION_TYPE].astype("category")
        k1, k2 = Properties.UNITOFMEASUREMENT.split("/", 1)
        df_i[Df.UNITS] = response_datastream.get(k1, {}).get(k2)
        df_i[Df.UNITS] = df_i[Df.UNITS].astype("category")

        df_i[[Df.LONG, Df.LAT]] = pd.DataFrame.from_records(
            df_i[str(Entities.FEATUREOFINTEREST)].apply(
                lambda x: x.get("feature").get("coordinates")
            )
        )
        del df_i[str(Entities.FEATUREOFINTEREST)]
        # df_i.drop(columns=str(Entities.FEATUREOFINTEREST))
        df = pd.concat([df, df_i], ignore_index=True)

    return df


def response_datastreams_to_df(response: dict) -> pd.DataFrame:
    df_out = pd.DataFrame()
    for ds_i in response[Entities.DATASTREAMS]:
        if f"{Entities.OBSERVATIONS}@iot.nextLink" in ds_i:
            log.warning("Not all observations are extracted!")  # TODO: follow link!
        df_i = response_single_datastream_to_df(ds_i)
        log.debug(f"{df_i.shape[0]=}")
        df_out = pd.concat([df_out, df_i], ignore_index=True)
    return df_out


def datastreams_response_to_df(response_datastreams):
    df = pd.DataFrame()
    for di in response_datastreams:
        observations_list = di.get(Entities.OBSERVATIONS)
        if observations_list:
            df_i = pd.DataFrame(observations_list).astype(
                {Properties.IOT_ID: int, Df.RESULT: float}
            )
            df_i[Df.DATASTREAM_ID] = int(di.get(Properties.IOT_ID))
            df_i[Properties.PHENOMENONTIME] = df_i[Properties.PHENOMENONTIME].apply(
                convert_to_datetime
            )
            df_i[Df.OBSERVATION_TYPE] = di.get(Entities.OBSERVEDPROPERTY).get(
                Properties.NAME
            )
            df_i[Df.OBSERVATION_TYPE] = df_i[Df.OBSERVATION_TYPE].astype("category")
            k1, k2 = Properties.UNITOFMEASUREMENT.split("/", 1)
            df_i[Df.UNITS] = di.get(k1).get(k2)
            df_i[Df.UNITS] = df_i[Df.UNITS].astype("category")

            df_i[[Df.LONG, Df.LAT]] = pd.DataFrame.from_records(
                df_i[str(Entities.FEATUREOFINTEREST)].apply(
                    lambda x: x.get("feature").get("coordinates")
                )
            )
            del df_i[str(Entities.FEATUREOFINTEREST)]
            # df_i.drop(columns=str(Entities.FEATUREOFINTEREST))
            df = pd.concat([df, df_i], ignore_index=True)

    return df


def seavox_to_df(response_seavox: Sequence[Sequence[str]]) -> pd.DataFrame:
    df = pd.DataFrame()
    df[[Df.REGION, Df.SUB_REGION]] = pd.DataFrame.from_records(response_seavox)

    return df


# def test_patch_single(id, value):
#     a = Patch.observation(entity_id=id, result_quality=str(value))
#     return a

def query_region_from_xy(coords):
    points_q = build_points_query(coords)
    query = build_query_points(table="seavox_sea_areas", points_query=points_q, select="region, sub_region, ST_AsText(geom)")
    with connect() as c:
        with c.cursor() as cursor:
            results = []
            cursor.execute(query)
            res = cursor.fetchall()

    return res


def query_all_nan_regions(df):
    idx_nan = df.Region.isnull()
    points_nan = df.loc[idx_nan, [Df.LONG, Df.LAT]].to_numpy().tolist()
    if points_nan: 
        res = query_region_from_xy(points_nan)

        df_seavox = seavox_to_df([res_i[:2] for res_i in res])
        df.loc[idx_nan, [Df.REGION, Df.SUB_REGION]] = df_seavox

    return df


def intersect_df_region(df, max_queries, max_query_points):
    df_out = deepcopy(df)
    n = 0

    si = df.sindex

    while True:
        point_i = (
            df_out.loc[df_out.Region.isnull(), [Df.LONG, Df.LAT]].to_numpy().tolist()[:1]
        )
        res = query_region_from_xy(point_i)

        g_ref = loads(res[0][2])

        idx_gref = si.query(g_ref, predicate="intersects").tolist()

        df_out.loc[idx_gref, [Df.REGION, Df.SUB_REGION]] = res[0][:2]

        n += 1
        count_dict = df_out.Region.value_counts(dropna=False).to_dict()
        nb_nan = sum([count_dict.get(ki, 0) for ki in [None, np.nan]])
        if nb_nan <= max_query_points or n >= max_queries:
            break

    df_out = query_all_nan_regions(df_out)
    return df_out


