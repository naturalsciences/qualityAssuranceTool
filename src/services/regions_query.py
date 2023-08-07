import logging
import time
from typing import Sequence

import psycopg2
import psycopg2.extensions
from shapely import Point, distance, intersects, set_srid
from shapely.wkt import loads

from services.config import DbCredentials

# from services.df import seavox_to_df

log = logging.getLogger(__name__)

def connect(db_credentials: DbCredentials) -> psycopg2.extensions.connection:
    log.debug("Get connection seavox db")
    try:
        connection = psycopg2.connect(
            database=db_credentials.database,
            user=db_credentials.user,
            password=db_credentials.passphrase,
            host=db_credentials.host,
            # port="8901"
            port=db_credentials.port,
        )
        return connection

    except psycopg2.Error as e:
        print("Error connecting to the database:", e)
        raise


def build_query_points(table: str, points_query: str, select: str) -> str:
    log.debug("create db query multiple points")
    query = f"""
    SELECT
        {select}
    FROM
        (VALUES
            {points_query}
        ) AS points_table (point_geom)
    LEFT JOIN
        {table} AS regions_table
    ON
        ST_Intersects(ST_SetSRID(regions_table.geom, 4326), points_table.point_geom);"""

    return query


def build_points_query(points: Sequence[Sequence[float]]) -> str:
    log.debug("create sub-query points creation")
    list_points = [f"(ST_SetSRID(ST_MakePoint({p[0]},{p[1]}), 4326))" for p in points]
    q_out = ",".join(list_points)
    return q_out


def main():
    t0 = time.time()
    points = [(3.0, 52), (2.9, 51.1), (89, 0.0), (52, 3.1254), (2, 2)] * 100
    points_q = build_points_query(
        [points[0]]
        )

    db_credentials: DbCredentials = DbCredentials(
        database= "seavox_areas",
        user= "sevox",
        host= "localhost",
        port= 5432,
        passphrase="ChangeMe"
    )

    query_0 = build_query_points(
        table="seavox_sea_areas",
        points_query=points_q,
        select="region, sub_region, ST_AsText(geom)",
    )
    with connect(db_credentials) as c:
        t1 = time.time()
        with c.cursor() as cursor:
            results = []
            cursor.execute(query_0)
            res = cursor.fetchall()

    t2 = time.time()
    region_0 = loads(res[0][2])
    t2_a = time.time()
    points_P = [set_srid(Point(pi), 4326) for pi in points]
    t2_b = time.time()

    testing_intersects = [intersects(region_0, pi) for pi in points_P]
    t2_c = time.time()
    testing_distance = [distance(region_0, pi) for pi in points_P]

    t3 = time.time()
    testing_contains = region_0.contains(points_P)
    t4 = time.time()
    print(f"points creation: {t2_b-t2_a}")
    print(f"interesects: {t2_c-t2_b}")
    print(f"distance: {t3-t2_c}")
    print(f"contains: {t4-t3}")

    print(f"done")
    # res_regions = list(map(lambda x: (x[0], x[1]), res))
    # t4 = time.time()
    # res_regions = list(
    #     zip(*list(zip(*res))[:2])
    # )  # same as "list(map(lambda x: (x[0], x[1]), res))"", but faster
    # t5 = time.time()
    # df = seavox_to_df(res_regions)
    # print(f"duration: {t3-t0}")
    # print(f"duration map: {t4-t3}")
    # print(f"duration zipzip: {t5-t4}")
    # print(res)
    # points_query = """
    #         (ST_MakePoint(4., 51.7)),
    #         (ST_MakePoint(5., 51.6)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(5., 51.27)),
    #         (ST_MakePoint(6., 51.5)),
    #         (ST_MakePoint(8., 51.17)),
    #         (ST_MakePoint(9., 51.5)),
    #         (ST_MakePoint(15., 52.5)),
    #         (ST_MakePoint(7., 55.34)),
    #         (ST_MakePoint(8., 42.5)),
    #         (ST_MakePoint(8., 61.5))
    # """
    # query = build_query_points(points_query)
    # with connect() as c:
    #     t2 = time.time()
    #     with c.cursor() as cursor:
    #         results = []
    #         cursor.execute(query)
    #         res = cursor.fetchall()
    # t1 = time.time()
    # print(res)
    # # with connect() as c:
    # #     with c.cursor() as cursor:
    # #         results = []
    # #         for point in points:
    # #             query = build_query("seavox_sea_areas", point)

    # #             cursor.execute(query)
    # #             regions = cursor.fetchall()
    # #             results.append({"point": point, "regions": regions})

    # print(f"duration: {t1-t0}")
    # print(f"duration: {t2-t0}")
    # t3 = time.time()
    # with connect() as c:
    #     with c.cursor() as cursor:
    #         results = []
    #         for point in points:
    #             query = build_query("seavox_sea_areas", point)

    #             cursor.execute(query)
    #             regions = cursor.fetchall()
    #             results.append({"point": point, "regions": regions})
    # t4 = time.time()
    # print(f"{t4-t3}")
    # #
    # # print(build_query("str", [1, 2]))
    # # print(build_query("str", (1, 2)))


if __name__ == "__main__":
    main()
