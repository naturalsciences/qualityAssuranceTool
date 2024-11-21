import logging
import os
import sys
import threading
import time
from datetime import datetime
from functools import partial
from pathlib import Path
from urllib.parse import urljoin

import geopandas as gpd
import hydra
import pandas as pd
import aenum
from df_qc_tools.config import QCconf, filter_cfg_to_query
from df_qc_tools.qc import (
    FEATURES_BODY_TEMPLATE,
    QCFlagConfig,
    calc_gradient_results,
    calc_zscore_results,
    get_bool_depth_above_treshold,
    get_bool_exceed_max_acceleration,
    get_bool_exceed_max_velocity,
    get_bool_land_region,
    get_bool_null_region,
    get_bool_out_of_range,
    get_bool_spacial_outlier_compared_to_median,
    qc_dependent_quantity_base,
    qc_dependent_quantity_secondary,
    update_flag_history_series,
)
from dotenv import load_dotenv
from omegaconf import OmegaConf
from pandassta.df import (
    Df,
    QualityFlags,
    get_dt_velocity_and_acceleration_series,
    df_type_conversions,
)
from pandassta.sta import Entities, Properties, Settings
from pandassta.sta_requests import (
    config,
    get_all_data,
    get_elev_netcdf,
    patch_qc_flags,
    set_sta_url,
    Entity,
    Query,
)
from searegion_detection.pandaseavox import intersect_df_region

log = logging.getLogger(__name__)

load_dotenv()


def get_date_from_string(
    str_in: str, str_format_in: str = "%Y-%m-%d %H:%M", str_format_out: str = "%Y%m%d"
) -> str:
    date_out = datetime.strptime(str(str_in), str_format_in)
    return date_out.strftime(str_format_out)


OmegaConf.register_new_resolver("datetime_to_date", get_date_from_string, replace=True)


def get_thing_ds_summary(thing_id: int) -> pd.DataFrame:
    if not getattr(Properties, "INSTRDATAID", None):
        Properties_ = aenum.extend_enum(
            Properties, "INSTRDATAID", "properties/InstrDataItemID"
        )
    obsprop = Entity(Entities.OBSERVEDPROPERTY)
    obsprop.selection = [Properties.NAME]

    obs = Entity(Entities.OBSERVATIONS)
    obs.settings = [Settings.COUNT("true"), Settings.TOP(0)]
    obs.selection = [Properties.IOT_ID]

    ds = Entity(Entities.DATASTREAMS)
    ds.settings = [Settings.COUNT("true")]
    ds.expand = [obsprop, obs]
    ds.selection = [
        Properties.NAME,
        Properties.IOT_ID,
        Properties.DESCRIPTION,
        Properties.UNITOFMEASUREMENT,
        Entities.OBSERVEDPROPERTY,
        Properties.INSTRDATAID,  # type: ignore
    ]
    thing = Entity(Entities.THINGS)
    thing.id = thing_id
    thing.expand = [ds]
    thing.selection = [Entities.DATASTREAMS]
    query = Query(base_url=config.load_sta_url(), root_entity=thing)
    query_http = query.build()

    response = (query.get_with_retry()).json()
    df = df_type_conversions(pd.DataFrame.from_dict(response[Entities.DATASTREAMS]))
    df = df.join(pd.DataFrame(df.pop("properties").values.tolist()))
    df_tmp = pd.DataFrame(df.pop(str(Entities.OBSERVEDPROPERTY)).values.tolist())
    obs_name_column = df_tmp.columns[0]
    # df = df.join(pd.DataFrame(df.pop(str(Entities.OBSERVEDPROPERTY)).values.tolist()), rsuffix=str(Entities.OBSERVEDPROPERTY))
    df = df.join(df_tmp, rsuffix=str(Entities.OBSERVEDPROPERTY))

    return df


def write_datastreamid_yaml_template(thing_id, file: Path) -> None:
    """
    Function to write a yaml template to configure the ranges and other QC settings.

    Args:
        thing_id (_type_): _description_
        file (Path): _description_

    Returns:
        _type_: _description_
    """
    df = get_thing_ds_summary(thing_id=thing_id)

    def row_formatted(row):
        out = (
            ""
            f"  {row[Properties.IOT_ID]}: # name: {row[Properties.NAME]}, type: {row['nameObservedProperty']}, unit: {row['unitOfMeasurement']['name']}, instrdataid: {row['InstrDataItemID']}\n"
            f"    range:\n"
            f"      -\n"
            f"      -\n"
        )
        return out

    out = df.apply(row_formatted, axis=1).sum()
    with open(file, "w") as f:
        f.writelines(out)


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

    def custom_exception_handler(exc_type, exc_value, exc_traceback):
        # Log the exception
        log.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

        # Call the default exception hook (prints the traceback and exits)
        sys.__excepthook__(exc_type, exc_value, exc_traceback)

    sys.excepthook = custom_exception_handler

    t0 = time.time()
    docker_image_tag = os.environ.get("IMAGE_TAG", None)
    if docker_image_tag:
        log.info(f"Docker image tag: {docker_image_tag}.")
    git_hash = os.environ.get("GIT_HASH", None)
    if git_hash:
        log.info(f"Current git commit hash: {git_hash}.")

    log.info("Start")
    history_series = pd.Series()

    # setup
    log.info("Setup")
    t_df0 = time.time()
    config.filename = Path("outputs/.staconf.ini")
    set_sta_url(cfg.data_api.base_url)
    url_batch = urljoin(cfg.data_api.base_url + "/", "$batch")

    auth_tuple = (
        getattr(cfg.data_api, "auth", {}).get("username", None),
        getattr(cfg.data_api, "auth", {}).get("passphrase", None),
    )
    auth_in = [None, auth_tuple][all(auth_tuple)]

    thing_id = cfg.data_api.things.id

    filter_cfg = filter_cfg_to_query(cfg.data_api.filter)
    filter_cfg_datastreams = filter_cfg_to_query(
        cfg.data_api.filter, level=Entities.DATASTREAMS
    )

    # get data in dataframe
    # write_datastreamid_yaml_template(thing_id=thing_id, file=Path("/tmp/test.yaml"))

    df_all = get_all_data(
        thing_id=thing_id,
        filter_cfg=filter_cfg,
        filter_cfg_datastreams=filter_cfg_datastreams,
        count_observations=cfg.other.count_observations,
    )

    if df_all.empty:
        log.warning("Terminating script.")
        return 0

    nb_observations = df_all.shape[0]
    df_all = gpd.GeoDataFrame(df_all, geometry=gpd.points_from_xy(df_all[Df.LONG], df_all[Df.LAT]), crs=cfg.location.crs)  # type: ignore
    # get qc check df (try to find clearer name)
    qc_config_dict = {li.get("id"): li for li in cfg.QC}  # type: ignore
    qc_df = pd.DataFrame.from_dict(qc_config_dict, orient="index")
    qc_df = qc_df.drop(columns="id")
    ## Is changing this suffusient to correctit?
    # qc_df.index.name = Df.OBSERVATION_TYPE
    qc_df.index.name = Df.DATASTREAM_ID

    ## setup needed columns. Should these be removed?
    t_ranges0 = time.time()
    for qc_type in qc_df.keys():
        qc_df[[f"qc_{'_'.join([qc_type, i])}" for i in ["min", "max"]]] = qc_df.pop(
            qc_type
        ).apply(pd.Series)

    # qc_df_global = pd.DataFrame.from_dict(cfg.QC_global, orient="index")
    # for qc_type in qc_df_global.keys():
    #     qc_df_global[[f"qc_{'_'.join([qc_type, i])}" for i in ["min", "max"]]] = qc_df_global.pop(
    #         qc_type
    #     ).apply(pd.Series)
    # qc_df[["qc_zscore_min", "qc_zscore_max"]] = cfg.QC_global["global"].zscore

    df_all = calc_gradient_results(df_all, Df.DATASTREAM_ID)
    df_all = calc_zscore_results(df_all, Df.DATASTREAM_ID)

    t_df1 = time.time()
    t_qc0 = time.time()

    ## reset flags
    RESET_OVERWRITE_FLAGS = cfg.reset.overwrite_flags
    RESET_OBSERVATION_FLAGS = cfg.reset.observation_flags
    RESET_FEATURE_FLAGS = cfg.reset.feature_flags
    QUIT_AFTER_RESET = cfg.reset.exit

    if RESET_OVERWRITE_FLAGS or RESET_FEATURE_FLAGS or RESET_OBSERVATION_FLAGS:
        df_all[Df.QC_FLAG] = QualityFlags.NO_QUALITY_CONTROL
        log.warning("QC flags will we overwritten!")
    if RESET_OBSERVATION_FLAGS:
        log.warning("Flags will be reset!")
        df_all[Df.QC_FLAG] = QualityFlags.NO_QUALITY_CONTROL
        counter_reset = patch_qc_flags(
            df_all.reset_index(),
            url=url_batch,
            auth=auth_in,
        )
    if RESET_FEATURE_FLAGS:
        counter_reset_features = patch_qc_flags(
            df_all.reset_index(),
            url=url_batch,
            auth=auth_in,
            columns=[Df.FEATURE_ID, Df.QC_FLAG],
            url_entity=Entities.FEATURESOFINTEREST,
            json_body_template=FEATURES_BODY_TEMPLATE,
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

        get_elev_netcdf()
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
        # bool_function=lambda x: pd.Series(False, index=x.index), # easiest method to disable this
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

    log.info(
        f"Detected number of spacial outliers: {df_all.loc[qc_flag_config_outlier.bool_series].shape[0]}."
    )

    history_series = update_flag_history_series(history_series, qc_flag_config_outlier)

    counter_flag_outliers = threading.Thread(
        target=patch_qc_flags,
        name="Patch_qc_spacial_outliers",
        kwargs={
            "df": df_all.reset_index(),
            # "df": df_all.reset_index(),
            "url": url_batch,
            "auth": auth_in,
            "columns": [Df.FEATURE_ID, Df.QC_FLAG],
            "url_entity": Entities.FEATURESOFINTEREST,
            "json_body_template": FEATURES_BODY_TEMPLATE,
        },
    )
    counter_flag_outliers.start()

    df_all = df_all.sort_values(Df.TIME)
    ## velocity and acceleration calculations
    series_dt_velocity_and_acceleration = get_dt_velocity_and_acceleration_series(
        df_all.loc[~qc_flag_config_outlier.bool_series].sort_values(
            Df.TIME
        )  #  type: ignore
    )

    ## velocity
    qc_flag_config_velocity = QCFlagConfig(
        "Velocity limit",
        bool_function=partial(
            get_bool_exceed_max_velocity,
            max_velocity=cfg.location.max_dx_dt,
            velocity_series=series_dt_velocity_and_acceleration[1],
            dt_series=series_dt_velocity_and_acceleration[0],
        ),
        bool_merge_function=max,
        flag_on_true=QualityFlags.BAD,
        flag_on_nan=QualityFlags.NO_QUALITY_CONTROL,
    )
    output_qc_velocity = qc_flag_config_velocity.execute(
        df_all.loc[~qc_flag_config_outlier.bool_series]
    )
    if qc_flag_config_velocity.bool_series.any():
        log.warning(
            f"Velocities {qc_flag_config_velocity.bool_series.sum()} exceeding the limiting value detected!"
        )
        log.warning(
            f"Max velocity value: {series_dt_velocity_and_acceleration[1].abs().max():.2f}"
        )

    # history_series = update_flag_history_series(history_series, qc_flag_config_velocity)

    ## acceleration
    qc_flag_config_acceleration = QCFlagConfig(
        "Acceleration limit",
        partial(
            get_bool_exceed_max_acceleration,
            max_acceleration=cfg.location.max_ddx_dtdt,
            acceleration_series=series_dt_velocity_and_acceleration[2],
            dt_series=series_dt_velocity_and_acceleration[0],
        ),
        max,
        QualityFlags.BAD,
        flag_on_nan=QualityFlags.NO_QUALITY_CONTROL,
    )
    output_qc_acceleration = qc_flag_config_acceleration.execute(
        df_all.loc[~qc_flag_config_outlier.bool_series]
    )
    if qc_flag_config_acceleration.bool_series.any():
        log.warning(
            f"Accelerations {qc_flag_config_acceleration.bool_series.sum()} exceeding the limiting value detected!"
        )
        log.warning(
            f"Max acceleration value: {series_dt_velocity_and_acceleration[2].abs().max():.2f}"
        )

    # history_series = update_flag_history_series(
    # history_series, qc_flag_config_acceleration
    # )

    t_region1 = time.time()

    df_all = df_all.merge(qc_df, on=qc_df.index.name, how="left")
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
        bool_function=partial(
            get_bool_out_of_range, qc_on=Df.RESULT, qc_type="gradient"
        ),
        bool_merge_function=max,
        flag_on_true=QualityFlags.BAD,
        flag_on_false=QualityFlags.PROBABLY_GOOD,
        flag_on_nan=QualityFlags.NO_QUALITY_CONTROL,
    )
    df_all[Df.QC_FLAG] = qc_flag_config_gradient.execute(df_all)

    history_series = update_flag_history_series(history_series, qc_flag_config_gradient)

    qc_flag_config_zscore = QCFlagConfig(
        label="zscore",
        bool_function=partial(get_bool_out_of_range, qc_on=Df.ZSCORE, qc_type="zscore"),
        bool_merge_function=max,
        flag_on_true=QualityFlags.BAD,
        flag_on_false=QualityFlags.PROBABLY_GOOD,
        flag_on_nan=QualityFlags.NO_QUALITY_CONTROL,
    )
    df_all[Df.QC_FLAG] = qc_flag_config_zscore.execute(df_all)

    history_series = update_flag_history_series(history_series, qc_flag_config_zscore)

    t_ranges1 = time.time()

    t_flag_ranges0 = time.time()

    t_flag_ranges1 = time.time()

    t_dependent0 = time.time()
    # TODO: not yet in flag_history
    for dependent_i in cfg.QC_dependent:
        independent = dependent_i.independent
        dependent = dependent_i.dependent
        dt_tolerance = dependent_i.dt_tolerance
        if isinstance(dependent, str) and ',' in dependent:
            for dependent_ii in [int(ii) for ii in dependent.split(",")]:
                base_flags = qc_dependent_quantity_base(
                    df_all,
                    independent=independent,
                    dependent=dependent_ii,
                    dt_tolerance=dt_tolerance,
                )
                df_all.update({Df.QC_FLAG: base_flags})  # type: ignore
                secondary_flags = qc_dependent_quantity_secondary(
                    df_all,
                    independent=independent,
                    dependent=dependent_ii,
                    range_=tuple(dependent_i.QC.range),  # type: ignore
                    dt_tolerance=cfg.QC_dependent[0].dt_tolerance,
                )
                df_all.update({Df.QC_FLAG: secondary_flags})  # type: ignore

        else:
            base_flags = qc_dependent_quantity_base(
                df_all,
                independent=independent,
                dependent=dependent,
                dt_tolerance=dt_tolerance,
            )
            df_all.update({Df.QC_FLAG: base_flags})  # type: ignore
            secondary_flags = qc_dependent_quantity_secondary(
                df_all,
                independent=independent,
                dependent=dependent,
                range_=tuple(dependent_i.QC.range),  # type: ignore
                dt_tolerance=cfg.QC_dependent[0].dt_tolerance,
            )
            df_all.update({Df.QC_FLAG: secondary_flags})  # type: ignore
    t_dependent1 = time.time()

    log.info(f"{df_all[Df.QC_FLAG].value_counts(dropna=False).to_json()=}")
    log.info(f"Observation types flagged as {QualityFlags.PROBABLY_BAD} or worse.")
    for obst_i in df_all.loc[
        (
            (df_all[Df.QC_FLAG] >= QualityFlags.PROBABLY_BAD)
            & (~qc_flag_config_outlier.bool_series)
        ),
        Df.OBSERVATION_TYPE,
    ].unique():
        log.info(f"{'.'*10}{obst_i}")

    t_qc1 = time.time()
    t_patch0 = time.time()
    t3 = time.time()
    auth_tuple = (
        getattr(cfg.data_api, "auth", {}).get("username", None),
        getattr(cfg.data_api, "auth", {}).get("passphrase", None),
    )
    auth_in = [None, auth_tuple][all(auth_tuple)]
    log_waiting_status = True
    while counter_flag_outliers.is_alive():
        if log_waiting_status:
            log.info("Waiting spacial outlier flag patching.")
            log_waiting_status = False
        time.sleep(5)
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
