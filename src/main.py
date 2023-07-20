import hydra
import stapy
import logging
import pandas as pd
from collections import Counter
from services.config import filter_cfg_to_query

from services.df import df_type_conversions, seavox_to_df
from services.qc import qc_on_df
from services.regions_query import build_points_query, build_query_points, connect
from services.requests import get_all_datastreams_data
from services.requests import patch_qc_flags

from shapely import distance, Point, set_srid
from shapely.wkt import loads


# Type hinting often ignored
# name and COUNT are probably *known* variables names of python property
# might be solved with _name, _count, or NAME, COUNT. when all caps is used, the __str__ will need to be changed to lower
# doesn't work because other ARE with camelcase


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


@hydra.main(config_path="../conf", config_name="config.yaml", version_base="1.2")
def main(cfg):
    log.info("Start")
    stapy.set_sta_url(cfg.data_api.base_url)

    thing_id = cfg.data_api.things.id

    nb_streams_per_call = cfg.data_api.datastreams.top
    top_observations = cfg.data_api.observations.top
    filter_cfg = filter_cfg_to_query(cfg.data_api.get("filter", {}))

    df_all = get_all_datastreams_data(
        thing_id=thing_id,
        nb_streams_per_call=nb_streams_per_call,
        top_observations=top_observations,
        filter_cfg=filter_cfg,
    )
    df_all = df_type_conversions(df_all)

    # points_q = build_points_query(df_all[["long", "lat"]].to_numpy().tolist()[:1])
    # query = build_query_points(table="seavox_sea_areas", points_query=points_q, select="region, sub_region, ST_AsText(geom)")
    # with connect() as c:
    #     with c.cursor() as cursor:
    #         results = []
    #         cursor.execute(query)
    #         res = cursor.fetchall()
    #         
    # log.debug("starting points to Points")
    # Points = df_all[["long", "lat"]].apply(lambda x: Point(x["long"], x["lat"]),axis=1)
    # log.debug("Start distance calc")
    # g_ref = loads(res[0][2])
    # df_p = pd.DataFrame()
    # df_p["Points"] = Points
    # df_p["distance"] = df_p["Points"].apply(lambda x: distance(x, g_ref))
    # # distance_to_p = [distance(pi, g_ref) for pi in Points]
    # log.debug("transform to bool")
    # # bool_zone = [dpi == 0. for dpi in distance_to_p]
    # log.debug("set in df")
    # log.debug(f"{Counter((df_p['distance'] == 0.))}")
    # df_all.loc[df_p["distance"] == 0.,["Region", "Sub-region"]] = res[0][:2]
            
    log.debug("start seavox shizzle")
    points_q = build_points_query(df_all[["long", "lat"]].to_numpy().tolist())
    query = build_query_points(table="seavox_sea_areas", points_query=points_q, select="region, sub_region")
    with connect() as c:
        with c.cursor() as cursor:
            results = []
            cursor.execute(query)
            res = cursor.fetchall()

    df_seavox = seavox_to_df(res)
    log.debug("end seavox shizzle")


    df_all = pd.concat([df_all, df_seavox], axis=1)
    log.debug("done with df_all")

    # df_all = df_type_conversions(df_all)

    # df_all = qc_on_df(df_all, cfg=cfg)

    # url = "http://localhost:8080/FROST-Server/v1.1/$batch"
    # counter = patch_qc_flags(df_all, url=url)

    # print(f"{counter=}")
    # print(f"{df_all.shape=}")
    # print(f"{df_all.dtypes=}")

    log.info("End")


if __name__ == "__main__":
    log.debug("testing...")
    main()
