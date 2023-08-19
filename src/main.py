import logging
import time

import geopandas as gpd
import hydra
import pandas as pd
import stapy

from models.enums import Df, QualityFlags
from services.config import QCconf, filter_cfg_to_query
from services.df import intersect_df_region
from services.qc import (
    CAT_TYPE,
    calc_gradient_results,
    get_bool_depth_below_threshold,
    get_bool_land_region,
    get_bool_null_region,
    get_bool_out_of_range,
    get_bool_spacial_outlier_compared_to_median,
    get_qc_flag_from_bool,
    qc_dependent_quantity_base,
    qc_dependent_quantity_secondary,
)
from services.qc import update_flag_history_series
from services.requests import get_all_data, get_elev_netcdf, patch_qc_flags

log = logging.getLogger(__name__)


@hydra.main(config_path="../conf", config_name="config.yaml", version_base="1.2")
def main(cfg: QCconf):
    t0 = time.time()
    log.info("Start")

    history_series = pd.Series()

    # setup
    t_df0 = time.time()
    stapy.set_sta_url(cfg.data_api.base_url)

    thing_id = cfg.data_api.things.id

    get_elev_netcdf()
    filter_cfg = filter_cfg_to_query(cfg.data_api.filter)

    # get data in dataframe
    df_all = get_all_data(thing_id=thing_id, filter_cfg=filter_cfg)
    nb_observations = df_all.shape[0]
    df_all = gpd.GeoDataFrame(df_all, geometry=gpd.points_from_xy(df_all[Df.LONG], df_all[Df.LAT]), crs=cfg.location.crs)  # type: ignore
    # get qc check df (try to find clearer name)
    qc_df = pd.DataFrame.from_dict(cfg.QC, orient="index")
    qc_df.index.name = Df.OBSERVATION_TYPE

    ## setup needed columns. Should these be removed?
    t_ranges0 = time.time()
    for qc_type in qc_df.keys():
        qc_df[[f"qc_{'_'.join([qc_type, i])}" for i in ["min", "max"]]] = qc_df.pop(
            qc_type
        ).apply(pd.Series)

    df_all = calc_gradient_results(df_all, Df.DATASTREAM_ID)

    t_df1 = time.time()
    t_qc0 = time.time()
    ## find region
    t_region0 = time.time()
    df_all = intersect_df_region(
        db_credentials=cfg.location.connection,
        df=df_all,
        max_queries=5,
        max_query_points=20,
    )

    bool_nan = get_bool_null_region(df_all)
    df_all[Df.QC_FLAG] = (
        df_all[Df.QC_FLAG]
        .combine(
            get_qc_flag_from_bool(
                bool_=bool_nan,
                flag_on_true=QualityFlags.PROBABLY_GOOD,
            ),
            max,
            fill_value=QualityFlags.NO_QUALITY_CONTROL,
        )
        .astype(CAT_TYPE)
    )
    history_series = update_flag_history_series(
        history_series,
        test_name="Region nan",
        bool_=bool_nan,
        flag_on_true=QualityFlags.PROBABLY_GOOD,
    )

    bool_mainland = get_bool_land_region(df_all)
    df_all[Df.QC_FLAG] = (
        df_all[Df.QC_FLAG]
        .combine(
            get_qc_flag_from_bool(
                bool_=bool_mainland,
                flag_on_true=QualityFlags.BAD,
            ),
            max,
            fill_value=QualityFlags.NO_QUALITY_CONTROL,
        )
        .astype(CAT_TYPE)
    )
    history_series = update_flag_history_series(
        history_series,
        test_name="Region mainland",
        bool_=bool_mainland,
        flag_on_true=QualityFlags.BAD,
    )

    bool_depth_above_0 = ~get_bool_depth_below_threshold(df_all, threshold=0.0)
    df_all[Df.QC_FLAG] = (
        df_all[Df.QC_FLAG]
        .combine(
            get_qc_flag_from_bool(
                bool_=bool_depth_above_0,
                flag_on_true=QualityFlags.BAD,
            ),
            max,
            fill_value=QualityFlags.NO_QUALITY_CONTROL,
        )
        .astype(CAT_TYPE)
    )
    history_series = update_flag_history_series(
        history_series,
        test_name="Depth",
        bool_=bool_depth_above_0,
        flag_on_true=QualityFlags.BAD,
    )

    ## outliers location
    bool_outlier = get_bool_spacial_outlier_compared_to_median(
        df_all, max_dx_dt=cfg.location.max_dx_dt, time_window=cfg.location.time_window
    )
    df_all[Df.QC_FLAG] = (
        df_all[Df.QC_FLAG]
        .combine(
            get_qc_flag_from_bool(
                bool_=bool_outlier,
                flag_on_true=QualityFlags.BAD,
            ),
            max,
            fill_value=QualityFlags.NO_QUALITY_CONTROL,
        )
        .astype(CAT_TYPE)
    )
    history_series = update_flag_history_series(
        history_series,
        test_name="Location outlier",
        bool_=bool_outlier,
        flag_on_true=QualityFlags.BAD,
    )

    t_region1 = time.time()
    df_all = df_all.merge(qc_df, on=Df.OBSERVATION_TYPE, how="left")
    df_all.set_index(Df.IOT_ID)
    if nb_observations != df_all.shape[0]:
        raise RuntimeError("Not all observations are included in the dataframe.")

    bool_range = get_bool_out_of_range(df=df_all, qc_on=Df.RESULT, qc_type="range")
    df_all[Df.QC_FLAG] = (
        df_all[Df.QC_FLAG]
        .combine(
            get_qc_flag_from_bool(
                bool_=bool_range,
                flag_on_true=QualityFlags.BAD,
                flag_on_false=QualityFlags.GOOD,
            ),
            max,
            fill_value=QualityFlags.NO_QUALITY_CONTROL,
        )
        .astype(CAT_TYPE)
    )
    history_series = update_flag_history_series(
        history_series,
        test_name="Range",
        bool_=bool_range,
        flag_on_true=QualityFlags.BAD,
    )

    bool_gradient = get_bool_out_of_range(
        df=df_all, qc_on=Df.GRADIENT, qc_type="gradient"
    )
    df_all[Df.QC_FLAG] = (
        df_all[Df.QC_FLAG]
        .combine(
            get_qc_flag_from_bool(
                bool_=bool_gradient,
                flag_on_true=QualityFlags.BAD,
                flag_on_false=QualityFlags.GOOD,
            ),
            max,
            fill_value=QualityFlags.NO_QUALITY_CONTROL,
        )
        .astype(CAT_TYPE)
    )
    history_series = update_flag_history_series(
        history_series,
        test_name="Gradient",
        bool_=bool_gradient,
        flag_on_true=QualityFlags.BAD,
    )

    t_ranges1 = time.time()

    t_flag_ranges0 = time.time()

    t_flag_ranges1 = time.time()

    t_dependent0 = time.time()
    # TODO: not yet in flag_history
    for dependent_i in cfg.QC_dependent:
        independent = dependent_i.independent
        dependent = dependent_i.dependent
        dt_tolerance = dependent_i.dt_tolerance

        base_flags = qc_dependent_quantity_base(
            df_all,
            independent=independent,
            dependent=dependent,
            dt_tolerance=dt_tolerance,
        )
        df_all[Df.QC_FLAG].update(base_flags)
        df_all = qc_dependent_quantity_secondary(
            df_all,
            independent=independent,
            dependent=dependent,
            range_=tuple(dependent_i.QC.range),
            dt_tolerance=cfg.QC_dependent[0].dt_tolerance,
        )
    t_dependent1 = time.time()

    log.info(f"{df_all[Df.QC_FLAG].value_counts(dropna=False).to_json()=}")
    log.info(
        f"{df_all[[Df.OBSERVATION_TYPE, Df.QC_FLAG]].value_counts(dropna=False).to_json()=}"
    )

    t_qc1 = time.time()
    t_patch0 = time.time()
    t3 = time.time()
    url = "http://localhost:8080/FROST-Server/v1.1/$batch"
    counter = patch_qc_flags(df_all.reset_index(), url=url)
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
