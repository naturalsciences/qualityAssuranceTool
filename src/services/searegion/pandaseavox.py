import numpy as np
from shapely.wkt import loads
from copy import deepcopy
from pandassta.df import Df


import pandas as pd


from typing import Sequence
from pandassta.df import df_type_conversions

from services.searegion.queryregion import build_points_query, build_query_points, connect


def seavox_to_df(response_seavox: Sequence[Sequence[str]]) -> pd.DataFrame:
    df = pd.DataFrame()
    df[[Df.REGION, Df.SUB_REGION]] = pd.DataFrame.from_records(response_seavox)

    return df


def query_region_from_xy(db_credentials, coords):
    points_q = build_points_query(coords)
    query = build_query_points(
        table="seavox_sea_areas",
        points_query=points_q,
        select="region, sub_region, ST_AsText(geom)",
    )
    with connect(db_credentials) as c:
        with c.cursor() as cursor:
            results = []
            cursor.execute(query)
            res = cursor.fetchall()

    return res


def query_all_nan_regions(db_credentials, df):
    points_nan = df.loc[df[Df.REGION].isnull(), [Df.LONG, Df.LAT]].drop_duplicates()
    if not points_nan.empty:
        res = query_region_from_xy(db_credentials, points_nan.to_numpy().tolist())

        df_seavox = seavox_to_df([res_i[:2] for res_i in res])
        df_seavox[[Df.LONG, Df.LAT]] = points_nan.to_numpy().tolist()
        df.update(
            df[[Df.LONG, Df.LAT]].merge(df_seavox, on=[Df.LONG, Df.LAT], how="left")
        )

    return df


def intersect_df_region(db_credentials, df, max_queries, max_query_points):
    df_out = deepcopy(df)
    if Df.REGION not in df_out:
        df_out[Df.REGION] = None

    n = 0

    si = df.sindex

    while True:
        df.info(f"Find seavox region of next point.")
        point_i = (
            df_out.loc[df_out.Region.isnull(), [Df.LONG, Df.LAT]]
            .sample(1)
            .to_numpy()
            .tolist()
        )
        res = query_region_from_xy(db_credentials, point_i)

        g_ref = loads(res[0][2])

        idx_gref = si.query(g_ref, predicate="intersects").tolist()

        df_out.loc[idx_gref, [Df.REGION, Df.SUB_REGION]] = res[0][:2]

        n += 1
        count_dict = df_out.Region.value_counts(dropna=False).to_dict()
        nb_nan = sum([count_dict.get(ki, 0) for ki in [None, np.nan]])
        if nb_nan <= max_query_points or n >= max_queries:
            break

    df_out = query_all_nan_regions(db_credentials, df_out)
    df_out = df_type_conversions(df_out)
    return df_out