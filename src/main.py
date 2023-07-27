import hydra
import pandas as pd
import stapy
import geopandas as gpd
import logging
import time

from services.config import filter_cfg_to_query
from services.df import df_type_conversions
from services.df import intersect_df_region
from services.qc import qc_on_df, qc_region
from services.requests import get_all_data, get_all_datastreams_data, patch_qc_flags



log = logging.getLogger(__name__)


@hydra.main(config_path="../conf", config_name="config.yaml", version_base="1.2")
def main(cfg):
    t0 = time.time()
    log.info("Start")
    stapy.set_sta_url(cfg.data_api.base_url)

    thing_id = cfg.data_api.things.id

    nb_streams_per_call = cfg.data_api.datastreams.top
    top_observations = cfg.data_api.observations.top
    filter_cfg = filter_cfg_to_query(cfg.data_api.get("filter", {}))

    df_all = get_all_data(thing_id=thing_id, filter_cfg=filter_cfg)
    df_all = gpd.GeoDataFrame(df_all, geometry=gpd.points_from_xy(df_all.long, df_all.lat), crs="EPSG:4326")  # type: ignore

    df_all[["Region", "Sub-region"]] = None
    df_all = intersect_df_region(df_all, max_queries=2, max_query_points=100)
    df_all = df_type_conversions(df_all)

    t1 = time.time()
    qc_df = pd.DataFrame.from_dict(cfg.QC, orient="index")
    qc_df.index.name = "observation_type"

    for qc_type in qc_df.keys():
        qc_df[[f"qc_{'_'.join([qc_type, i])}" for i in ["min", "max"]]] = qc_df.pop(qc_type).apply(pd.Series)

    
    df_indexed = df_all.set_index(["observation_type", "@iot.id"])
    df_merge = df_indexed.merge(qc_df, on="observation_type", how="left")

    mask = ~df_merge["qc_range_max"].isnull()
    df_merge.loc[mask,"qc_flags"] = (df_merge[mask]["result"] < df_merge[mask]["qc_range_max"]) & (df_merge[mask]["result"] > df_merge[mask]["qc_range_min"])
    df_merge.loc[mask,"qc_flags"] = ((df_merge["result"] < df_merge["qc_range_max"]) & (df_merge["result"] > df_merge["qc_range_min"]))[mask]

    t2 = time.time()

    log.info(f"time merge shizzle {t2-t1}")

    # df_all = qc_region(df_all)
    # t3 = time.time()
    # df_all = qc_on_df(df_all, cfg=cfg)
    # t4 = time.time()
    # log.info(f"time old qc range {t4-t3}")
    # url = "http://localhost:8080/FROST-Server/v1.1/$batch"
    # counter = patch_qc_flags(df_all, url=url)
    tend = time.time()
    log.info(f"Total time: {tend-t0}")
    print(f"{df_merge.memory_usage()}")
    log.info("End")


if __name__ == "__main__":
    log.debug("testing...")
    main()
