import logging
import time

import geopandas as gpd
import hydra
import pandas as pd
import stapy

from models.enums import Df, QualityFlags
from services.config import QCconf, filter_cfg_to_query
from services.df import intersect_df_region
from services.qc import (calc_gradient_results, get_bool_out_of_range,
                         get_bool_spacial_outlier_compared_to_median,
                         get_qc_flag_from_bool, qc_dependent_quantity_base,
                         qc_dependent_quantity_secondary, qc_region)
from services.requests import get_all_data, patch_qc_flags

log = logging.getLogger(__name__)


@hydra.main(config_path="../conf", config_name="config.yaml", version_base="1.2")
def main(cfg: QCconf):
    t0 = time.time()
    log.info("Start")

    # setup
    t_df0 = time.time()
    stapy.set_sta_url(cfg.data_api.base_url)

    thing_id = cfg.data_api.things.id

    filter_cfg = filter_cfg_to_query(cfg.data_api.filter)

    # get data in dataframe
    df_all = get_all_data(thing_id=thing_id, filter_cfg=filter_cfg)
    nb_observations = df_all.shape[0]
    df_all = gpd.GeoDataFrame(df_all, geometry=gpd.points_from_xy(df_all[Df.LONG], df_all[Df.LAT]), crs="EPSG:4326")  # type: ignore
    # get qc check df (try to find clearer name)
    qc_df = pd.DataFrame.from_dict(cfg.QC, orient="index")
    qc_df.index.name = Df.OBSERVATION_TYPE

    t_region1 = time.time()

    ## setup needed columns. Should these be removed?
    t_ranges0 = time.time()
    for qc_type in qc_df.keys():
        qc_df[[f"qc_{'_'.join([qc_type, i])}" for i in ["min", "max"]]] = qc_df.pop(
            qc_type
        ).apply(pd.Series)

    df_all = calc_gradient_results(df_all, Df.DATASTREAM_ID)
    df_merge = df_all.merge(qc_df, on=Df.OBSERVATION_TYPE, how="left")
    df_merge.set_index(Df.IOT_ID)

    t_df1 = time.time()
    t_qc0 = time.time()
    ## find region
    t_region0 = time.time()
    df_all = intersect_df_region(df_all, max_queries=5, max_query_points=20)
    df_all = qc_region(df_all)

    ## outliers location
    KNOTS_TO_KM_HOUR = 1.852
    KNOTS_TO_M_S = KNOTS_TO_KM_HOUR * 1.0e3 / 3600.0
    bool_outlier = get_bool_spacial_outlier_compared_to_median(
        df_all, max_dx_dt=13.0 * KNOTS_TO_M_S, time_window="5min"
    )
    df_all.update(
        get_qc_flag_from_bool(
            df_all,
            bool_=bool_outlier,
            flag_on_true=QualityFlags.BAD,
            update_verified=False,
        )[[Df.QC_FLAG]]
    )

    if nb_observations != df_merge.shape[0]:
        raise RuntimeError("Not all observations are included in the dataframe.")

    bool_range = get_bool_out_of_range(df=df_merge, qc_on=Df.RESULT, qc_type="range")
    df_merge.update(
        get_qc_flag_from_bool(
            df_merge,
            bool_=bool_range,
            flag_on_true=QualityFlags.BAD,
            update_verified=True,
        )
    )
    # df_merge = set_qc_flag_range_check(
    # df_merge, qc_type="range", qc_on=Df.RESULT, flag_on_fail=QualityFlags.BAD
    # )
    bool_gradient = get_bool_out_of_range(
        df=df_merge, qc_on=Df.GRADIENT, qc_type="gradient"
    )
    df_merge.update(
        get_qc_flag_from_bool(
            df_merge,
            bool_=bool_gradient,
            flag_on_true=QualityFlags.BAD,
            update_verified=True,
        )
    )

    # df_merge = set_qc_flag_range_check(
    # df_merge, qc_type="range", qc_on=Df.GRADIENT, flag_on_fail=QualityFlags.BAD
    # )
    t_ranges1 = time.time()

    t_flag_ranges0 = time.time()
    df_merge[Df.VALID] = df_merge[Df.VALID] & df_merge[Df.VERIFIED].astype(bool)
    df_merge.loc[df_merge[Df.VALID], Df.QC_FLAG] = QualityFlags.GOOD  # type:ignore

    t_flag_ranges1 = time.time()

    t_dependent0 = time.time()
    df_merge = qc_dependent_quantity_base(df_merge, independent=69, dependent=124)
    df_merge = qc_dependent_quantity_secondary(
        df_merge, independent=69, dependent=124, range_=(5.0, 10)
    )
    t_dependent1 = time.time()

    log.info(f"{df_merge[Df.QC_FLAG].value_counts(dropna=False).to_json()=}")
    log.info(
        f"{df_merge[[Df.OBSERVATION_TYPE, Df.QC_FLAG]].value_counts(dropna=False).to_json()=}"
    )

    cfg_dependent = cfg.QC_dependent
    t_qc1 = time.time()
    t_patch0 = time.time()
    t3 = time.time()
    url = "http://localhost:8080/FROST-Server/v1.1/$batch"
    counter = patch_qc_flags(df_merge.reset_index(), url=url)
    t_patch1 = time.time()
    tend = time.time()
    log.info(f"df requests/construction duration: {t_df1 - t_df0}")
    # log.info(f"Region check duration: {t_region1 - t_region0}")
    log.info(f"Ranges check duration: {t_ranges1 - t_ranges0}")
    log.info(f"Flagging ranges duration: {t_flag_ranges1 - t_flag_ranges0}")
    log.info(f"Total QC check duration: {t_qc1 - t_qc0}")
    log.info(f"Patch duration: {t_patch1 - t_patch0}")
    log.info(f"Total duration: {tend-t0}")
    log.info("End")


if __name__ == "__main__":
    log.debug("testing...")
    main()
