from functools import partial
import logging
import time
from pathlib import Path

import geopandas as gpd
import hydra
import pandas as pd
import stapy
from dotenv import load_dotenv

from models.enums import Df, Entities, QualityFlags
from services.config import QCconf, filter_cfg_to_query
from services.qc import QCFlagConfig, get_bool_depth_above_treshold
from services.df import intersect_df_region
from services.qc import (
    CAT_TYPE,
    calc_gradient_results,
    # get_bool_depth_below_threshold,
    get_bool_exceed_max_acceleration,
    get_bool_exceed_max_velocity,
    get_bool_land_region,
    get_bool_null_region,
    get_bool_out_of_range,
    get_bool_spacial_outlier_compared_to_median,
    get_qc_flag_from_bool,
    qc_dependent_quantity_base,
    qc_dependent_quantity_secondary,
    update_flag_history_series,
)
from services.requests import get_all_data, get_elev_netcdf, patch_qc_flags

log = logging.getLogger(__name__)

load_dotenv()

RESET_FLAGS = False
QUIT_AFTER_RESET = False


@hydra.main(config_path="../conf", config_name="config.yaml", version_base="1.2")
def main(cfg: QCconf):
    log_extra = logging.getLogger(name="extra")
    log_extra.setLevel(logging.INFO)
    rootlog = logging.getLogger()
    extra_log_file = Path(
        getattr(rootlog.handlers[1], "baseFilename", "./")
    ).parent.joinpath("history.log")
    file_handler_extra = logging.FileHandler(extra_log_file)
    file_handler_extra.setFormatter(rootlog.handlers[0].formatter)
    log_extra.addHandler(file_handler_extra)
    t0 = time.time()
    log.info("Start")

    history_series = pd.Series()

    # setup
    log.info("Setup")
    t_df0 = time.time()
    stapy.set_sta_url(cfg.data_api.base_url)
    url_batch = cfg.data_api.base_url + "/$batch"

    auth_tuple = (
        getattr(cfg.data_api, "auth", {}).get("username", None),
        getattr(cfg.data_api, "auth", {}).get("passphrase", None),
    )
    auth_in = [None, auth_tuple][all(auth_tuple)]

    thing_id = cfg.data_api.things.id

    get_elev_netcdf()
    filter_cfg = filter_cfg_to_query(cfg.data_api.filter)

    # get data in dataframe
    df_all = get_all_data(thing_id=thing_id, filter_cfg=filter_cfg)
    if df_all.shape == (0, 0):
        log.warning("No data, nothing to do")
        return 0
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

    ## reset flags
    if RESET_FLAGS:
        log.warning("Flags are reset!")
        df_all[Df.QC_FLAG] = QualityFlags.NO_QUALITY_CONTROL
        counter_reset = patch_qc_flags(
            df_all.reset_index(),
            url=url_batch,
            auth=auth_in,
        )
        if QUIT_AFTER_RESET:
            return 0

    ## find region

    t_region0 = time.time()
    if getattr(cfg, "location", {}).get("connection", None):
        df_all = intersect_df_region(
            db_credentials=cfg.location.connection,
            df=df_all,
            max_queries=5,
            max_query_points=20,
        )

        qc_flag_config_nan_region = QCFlagConfig(
            "Region nan",
            get_bool_null_region,
            max,
            QualityFlags.PROBABLY_GOOD,
            QualityFlags.NO_QUALITY_CONTROL,
        )
        df_all[Df.QC_FLAG] = qc_flag_config_nan_region.execute(df_all)

        history_series = update_flag_history_series(
            history_series, qc_flag_config_nan_region
        )

        qc_flag_config_land_region = QCFlagConfig(
            "Region mainland",
            get_bool_land_region,
            max,
            QualityFlags.BAD,
            QualityFlags.NO_QUALITY_CONTROL,
        )
        df_all[Df.QC_FLAG] = qc_flag_config_land_region.execute(df_all)
        history_series = update_flag_history_series(
            history_series, qc_flag_config_land_region
        )

        qc_flag_config_depth_above_threshold = QCFlagConfig(
            "Depth",
            partial(get_bool_depth_above_treshold, threshold=0.0),
            max,
            QualityFlags.BAD,
            QualityFlags.NO_QUALITY_CONTROL,
        )
        df_all[Df.QC_FLAG] = qc_flag_config_depth_above_threshold.execute(df_all)
        history_series = update_flag_history_series(
            history_series, qc_flag_config_depth_above_threshold
        )

    # find geographical outliers
    qc_flag_config_outlier = QCFlagConfig(
        "spacial_outliers",
        bool_function=partial(
            get_bool_spacial_outlier_compared_to_median,
            max_dx_dt=cfg.location.max_dx_dt,
            time_window=cfg.location.time_window,
        ),
        bool_merge_function=max,
        flag_on_true=QualityFlags.BAD,
        flag_on_nan=QualityFlags.PROBABLY_GOOD,
    )
    df_all[Df.QC_FLAG] = qc_flag_config_outlier.execute(df_all)
    # bool_outlier = get_bool_spacial_outlier_compared_to_median(
    # df_all, max_dx_dt=cfg.location.max_dx_dt, time_window=cfg.location.time_window  # type: ignore
    # )
    # df_all[Df.QC_FLAG] = (
    # df_all[Df.QC_FLAG]
    # .combine(
    # get_qc_flag_from_bool(
    # bool_=bool_outlier,
    # flag_on_true=QualityFlags.BAD,
    # ),
    # max,
    # fill_value=QualityFlags.PROBABLY_GOOD,
    # )
    # .astype(CAT_TYPE)
    # )
    log.info(
        f"Detected number of spacial outliers: {df_all.loc[qc_flag_config_outlier.bool_series].shape[0]}."
    )
    # log.debug(f"Indices of first elements of a flagged *block*: {get_start_flagged_blocks(df_all, bool_outlier)}") # type: ignore

    history_series = update_flag_history_series(history_series, qc_flag_config_outlier)

    features_body_template = '{"properties": {"resultQuality": "{value}"}}'

    counter_flag_outliers = patch_qc_flags(
        df_all.reset_index(),
        url=url_batch,
        auth=auth_in,
        columns=[Df.FEATURE_ID, Df.QC_FLAG],
        url_entity=Entities.FEATURESOFINTEREST,
        json_body_template=features_body_template,
    )

    ## velocity
    qc_flag_config_velocity = QCFlagConfig(
        "Velocity limit",
        bool_function=partial(
            get_bool_exceed_max_velocity, max_velocity=cfg.location.max_dx_dt
        ),
        bool_merge_function=max,
        flag_on_true=QualityFlags.BAD,
        flag_on_nan=QualityFlags.NO_QUALITY_CONTROL,
    )
    df_all.loc[
        ~qc_flag_config_outlier.bool_series, Df.QC_FLAG
    ] = qc_flag_config_velocity.execute(df_all.loc[~qc_flag_config_outlier.bool_series])

    history_series = update_flag_history_series(history_series, qc_flag_config_velocity)
    ## acceleration
    qc_flag_config_acceleration = QCFlagConfig(
        "Acceleration limit",
        partial(
            get_bool_exceed_max_acceleration, max_acceleration=cfg.location.max_ddx_dtdt
        ),
        max,
        QualityFlags.BAD,
        flag_on_nan=QualityFlags.NO_QUALITY_CONTROL,
    )
    df_all.loc[
        ~qc_flag_config_outlier.bool_series, Df.QC_FLAG
    ] = qc_flag_config_acceleration.execute(
        df_all.loc[~qc_flag_config_outlier.bool_series]
    )

    history_series = update_flag_history_series(
        history_series, qc_flag_config_acceleration
    )

    t_region1 = time.time()
    df_all = df_all.merge(qc_df, on=Df.OBSERVATION_TYPE, how="left")
    df_all.set_index(Df.IOT_ID)
    if nb_observations != df_all.shape[0]:
        raise RuntimeError("Not all observations are included in the dataframe.")

    qc_flag_config_range = QCFlagConfig(
        label="Range",
        bool_function=partial(get_bool_out_of_range, qc_on=Df.RESULT, qc_type="range"),
        bool_merge_function=max,
        flag_on_true=QualityFlags.BAD,
        flag_on_false=QualityFlags.PROBABLY_GOOD,
        flag_on_nan=QualityFlags.NO_QUALITY_CONTROL,
    )
    df_all[Df.QC_FLAG] = qc_flag_config_range.execute(df_all)

    history_series = update_flag_history_series(history_series, qc_flag_config_range)

    qc_flag_config_gradient = QCFlagConfig(
        label="Gradient",
        bool_function=partial(get_bool_out_of_range, qc_on=Df.RESULT, qc_type="gradient"),
        bool_merge_function=max,
        flag_on_true=QualityFlags.BAD,
        flag_on_false=QualityFlags.PROBABLY_GOOD,
        flag_on_nan=QualityFlags.NO_QUALITY_CONTROL,
    )
    df_all[Df.QC_FLAG] = qc_flag_config_gradient.execute(df_all)

    history_series = update_flag_history_series(history_series, qc_flag_config_gradient)

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
        secondary_flags = qc_dependent_quantity_secondary(
            df_all,
            independent=independent,
            dependent=dependent,
            range_=tuple(dependent_i.QC.range),  # type: ignore
            dt_tolerance=cfg.QC_dependent[0].dt_tolerance,
        )
        df_all[Df.QC_FLAG].update(secondary_flags)
    t_dependent1 = time.time()

    log.info(f"{df_all[Df.QC_FLAG].value_counts(dropna=False).to_json()=}")
    log.info(f"Observation types flagged as {QualityFlags.PROBABLY_BAD} or worse.")
    for obst_i in df_all.loc[
        ((df_all[Df.QC_FLAG] >= QualityFlags.PROBABLY_BAD) & (~qc_flag_config_outlier.bool_series)),
        Df.OBSERVATION_TYPE,
    ].unique():
        log.info(f"{'.'*10}{obst_i}")

    t_qc1 = time.time()
    t_patch0 = time.time()
    t3 = time.time()
    # url = "http://192.168.0.25:8080/FROST-Server/v1.1/$batch"
    auth_tuple = (
        getattr(cfg.data_api, "auth", {}).get("username", None),
        getattr(cfg.data_api, "auth", {}).get("passphrase", None),
    )
    auth_in = [None, auth_tuple][all(auth_tuple)]
    counter = patch_qc_flags(
        df_all.reset_index(),
        url=url_batch,
        auth=auth_in,
    )
    t_patch1 = time.time()
    tend = time.time()
    log.info(f"df requests/construction duration: {(t_df1 - t_df0):.2f}")
    log.info(f"Region check duration: {(t_region1 - t_region0):.2f}")
    log.info(f"Ranges check duration: {(t_ranges1 - t_ranges0):.2f}")
    log.info(f"Flagging ranges duration: {(t_flag_ranges1 - t_flag_ranges0):.2f}")
    log.info(f"Total QC check duration: {(t_qc1 - t_qc0):.2f}")
    log.info(f"Patch duration: {(t_patch1 - t_patch0):.2f}")
    log.info(f"Total duration: {(tend-t0):.2f}")
    log.info("End")
    log_extra.debug(history_series.to_json())


if __name__ == "__main__":
    log.debug("testing...")
    main()
