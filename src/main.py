import logging
import time

import geopandas as gpd
import hydra
import pandas as pd
import stapy

from models.enums import Df, QualityFlags
from services.config import filter_cfg_to_query
from services.df import df_type_conversions, intersect_df_region
from services.qc import (calc_gradient_results, get_bool_range, get_null_mask,
                         qc_region)
from services.requests import get_all_data, patch_qc_flags

log = logging.getLogger(__name__)


@hydra.main(config_path="../conf", config_name="config.yaml", version_base="1.2")
def main(cfg):
    t0 = time.time()
    log.info("Start")
    
    # setup
    stapy.set_sta_url(cfg.data_api.base_url)

    thing_id = cfg.data_api.things.id

    filter_cfg = filter_cfg_to_query(cfg.data_api.get("filter", {}))

    # get data in dataframe
    df_all = get_all_data(thing_id=thing_id, filter_cfg=filter_cfg)
    df_all = gpd.GeoDataFrame(df_all, geometry=gpd.points_from_xy(df_all.long, df_all.lat), crs="EPSG:4326")  # type: ignore

    ## find region
    df_all[[Df.REGION, Df.SUB_REGION]] = None
    df_all = intersect_df_region(df_all, max_queries=2, max_query_points=100)
    df_all = df_type_conversions(df_all)

    df_all = qc_region(df_all)
    df_all = calc_gradient_results(df_all, Df.DATASTREAM_ID)

    t1 = time.time()
    # get qc check df (try to find clearer name)
    qc_df = pd.DataFrame.from_dict(cfg.QC, orient="index")
    qc_df.index.name = Df.OBSERVATION_TYPE

    ## setup needed columns. Should these be removed?
    for qc_type in qc_df.keys():
        qc_df[[f"qc_{'_'.join([qc_type, i])}" for i in ["min", "max"]]] = qc_df.pop(qc_type).apply(pd.Series)

    
    df_merge = df_all.merge(qc_df, on=Df.OBSERVATION_TYPE, how="left")
    df_merge.set_index(Df.IOT_ID)

    mask = get_null_mask(df_merge, "range")
    BOOL_tmp = get_bool_range(df_merge.loc[mask], qc_on=Df.RESULT, qc_type="range")

    df_BOOL = pd.DataFrame(index=df_merge.index)
    df_BOOL["BOOL"] = True
    df_BOOL["BOOL_tested"] = False
    df_BOOL["BOOL_tmp"] = BOOL_tmp
    df_BOOL.loc[BOOL_tmp.index, "BOOL_tested"] = True
    df_BOOL["BOOL"] = (df_BOOL["BOOL"] & df_BOOL["BOOL_tmp"]) | ~df_BOOL["BOOL_tested"].astype(bool)


    mask = get_null_mask(df_merge, Df.GRADIENT)
    BOOL_tmp = get_bool_range(df_merge.loc[mask], qc_on=Df.GRADIENT, qc_type="range")
    df_BOOL.loc[BOOL_tmp.index, "BOOL_tested"] = True
    df_BOOL["BOOL"] = (df_BOOL["BOOL"] & df_BOOL["BOOL_tmp"]) | ~df_BOOL["BOOL_tested"].astype(bool)

    t2 = time.time()

    df_BOOL["BOOL"] = df_BOOL["BOOL"] & df_BOOL["BOOL_tested"]
    df_merge.loc[df_BOOL["BOOL"], Df.QC_FLAG] = QualityFlags.GOOD

    # attempt dependent qc checks
    

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
    print(f"{sum([di.memory_usage().sum() for di in [df_all, df_all, df_merge, qc_df]])*1.e-6}")
    log.info("End")


if __name__ == "__main__":
    log.debug("testing...")
    main()
