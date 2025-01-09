import copy
import logging
import os
import queue
import sys
import threading
import time
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from urllib.parse import urljoin

import aenum
import geopandas as gpd
import hydra
import pandas as pd
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
    get_qc_flag_from_bool,
    qc_dependent_quantity_base,
    qc_dependent_quantity_secondary,
    update_flag_history_series,
)
from dotenv import load_dotenv
from omegaconf import OmegaConf, dictconfig
from pandassta.df import (
    CAT_TYPE,
    Df,
    QualityFlags,
    df_type_conversions,
    get_dt_velocity_and_acceleration_series,
)
from pandassta.sta import Entities, Properties, Settings
from pandassta.sta_requests import (
    Entity,
    Query,
    config,
    create_patch_json,
    get_all_data,
    get_elev_netcdf,
    patch_qc_flags,
    set_dryrun_var,
    set_sta_url,
    write_patch_to_file,
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


def combine_df_all_w_dependency_output(
    df: pd.DataFrame, dependency_flags: pd.Series
) -> pd.Series:
    df["idx"] = df.index
    df = df.set_index(Df.IOT_ID)
    df[Df.QC_FLAG] = df[Df.QC_FLAG].combine(dependency_flags.astype(CAT_TYPE), max, QualityFlags.NO_QUALITY_CONTROL).astype(CAT_TYPE)  # type: ignore

    df_out = df.set_index("idx", drop=True)
    df_out.index.name = None
    return df_out[Df.QC_FLAG]


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
    set_dryrun_var(getattr(cfg.data_api, "dry_run", False))
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

    list_independent_ids = [getattr(li, "independent") for li in cfg.QC_dependent]
    list_independent_ids = [
        ids_i
        for ids_i in list_independent_ids
        if ids_i
        in getattr(cfg.data_api.filter, Entities.DATASTREAMS, {}).get(
            "ids", list_independent_ids
        )
    ]
    assert len(list_independent_ids) == len(
        set(list_independent_ids)
    ), "Independent ids must be unique."

    qc_dep_stabilize_configs = [
        li for li in cfg.QC_dependent if getattr(li, "dt_stabilization", None)
    ]
    qc_df_dep_stabilize = pd.DataFrame.from_dict(
        {getattr(c_i, "independent", {}): c_i for c_i in qc_dep_stabilize_configs},
        orient="index",
    )
    qc_df_dep_stabilize[["QC_range_min", "QC_range_max"]] = pd.DataFrame(
        qc_df_dep_stabilize["QC"].apply(lambda x: x["range"]).tolist(),
        index=qc_df_dep_stabilize.index,
    )

    cfg_indep_time = [
        ci for ci in cfg.QC_dependent if getattr(ci, "dt_stabilization", None)
    ]
    width_hours_window = max(
        [pd.Timedelta(ci.dt_stabilization) for ci in cfg_indep_time]
    )
    datastreams_window_list = [ci.independent for ci in cfg_indep_time]

    filter_window = copy.deepcopy(cfg.data_api.filter)
    filter_range = getattr(filter_window, Df.TIME).get("range", "")
    format_range = getattr(filter_window, Df.TIME).get("format", "%Y-%m-%d %H:%M")
    filter_range[0] = datetime.strftime(
        datetime.strptime(filter_range[0], format_range) - width_hours_window,
        format_range,
    )

    filter_window_cfg = filter_cfg_to_query(filter_window)
    filter_window_cfg_datastreams = f"{Properties.IOT_ID} in {str(tuple(datastreams_window_list)).replace(',)',')')}"

    queue_independent_timewindow = queue.Queue()
    thread_df_independent_timewindow = threading.Thread(
        target=get_all_data,
        name="independent_timewindow",
        kwargs={
            "thing_id": thing_id,
            "filter_cfg": filter_window_cfg,
            "filter_cfg_datastreams": filter_window_cfg_datastreams,
            "count_observations": False,
            "message_str": f"Get data independent time window.",
            "result_queue": queue_independent_timewindow,
        },
    )
    thread_df_independent_timewindow.start()

    def limit_value_fctn(group):
        g = (group[Df.RESULT] > group["QC_range_min"]) & (
            group[Df.RESULT] < group["QC_range_max"]
        )
        group["WITHIN_LIMITS"] = g

        group["dt"] = group[Df.TIME].diff().fillna(pd.Timedelta(seconds=0))
        group["cumsum"] = (group["dt"]).cumsum()

        tmp_down = group["cumsum"].where(group["WITHIN_LIMITS"])
        tmp_down.iloc[0] = pd.Timedelta(seconds=0)
        group["time_down"] = group["cumsum"] - tmp_down.ffill()

        tmp_up = group["cumsum"].where(
            (group["time_down"] > group["max_allowed_downtime"])
        )
        tmp_up.iloc[0] = pd.Timedelta(seconds=0)
        group["time_up_since"] = group["cumsum"] - tmp_up.ffill()

        # Group by consecutive WITHIN_LIMITS values
        group["block_id"] = (
            group["WITHIN_LIMITS"] != group["WITHIN_LIMITS"].shift()
        ).cumsum()

        # Calculate max downtime per "down" block and propagate within the block
        group["max_downtime"] = pd.Timedelta(seconds=0)  # Initialize column
        for block_id, sub_group in group.groupby("block_id"):
            if not sub_group["WITHIN_LIMITS"].iloc[0]:  # If the block is a "down" block
                max_down = sub_group["time_down"].max()
                group.loc[sub_group.index, "max_downtime"] = max_down

        group[Df.QC_FLAG] = get_qc_flag_from_bool(
            group["time_up_since"] < group["dt_stabilization"],
            flag_on_true=QualityFlags.BAD,
            flag_on_false=QualityFlags.NO_QUALITY_CONTROL,
        ).astype(CAT_TYPE)
        return group

    queue_all = queue.Queue()
    thread_df_all = threading.Thread(
        target=get_all_data,
        name="all_data",
        kwargs={
            "thing_id": thing_id,
            "filter_cfg": filter_cfg,
            "filter_cfg_datastreams": filter_cfg_datastreams,
            "count_observations": cfg.other.count_observations,
            "message_str": f"Get all data.",
            "result_queue": queue_all,
        },
    )
    thread_df_all.start()
    thread_df_all.join()
    df_all = queue_all.get()

    thread_df_independent_timewindow.join()
    df_independent_timewindow = queue_independent_timewindow.get()
    df_independent_timewindow = df_independent_timewindow.merge(
        qc_df_dep_stabilize.drop(columns=["dependent"]).rename(
            columns={"independent": "datastream_id"}
        ),
        on="datastream_id",
    )
    df_independent_grouped = df_independent_timewindow.sort_values(Df.TIME).groupby(
        by=[Df.DATASTREAM_ID], group_keys=False
    )

    df_independent_tmp = df_independent_grouped[
        [
            str(Df.IOT_ID),
            str(Df.RESULT),
            str(Df.DATASTREAM_ID),
            str(Df.TIME),
            "max_allowed_downtime",
            "dt_stabilization",
            "QC_range_min",
            "QC_range_max",
        ]
    ].apply(limit_value_fctn)

    if df_all.empty:
        log.warning("Terminating script.")
        return 0

    df_all_w_dependent = df_all.merge(
        df_independent_tmp, on=Df.IOT_ID, how="left", suffixes=("", "_independent")
    )
    df_all_w_dependent[Df.QC_FLAG + "_independent"] = df_all_w_dependent[Df.QC_FLAG + "_independent"].fillna(QualityFlags.NO_QUALITY_CONTROL)
    df_all_w_dependent[Df.QC_FLAG] = df_all_w_dependent[Df.QC_FLAG].combine(df_all_w_dependent[Df.QC_FLAG +"_independent"], max, QualityFlags.NO_QUALITY_CONTROL).astype(CAT_TYPE)  # type: ignore
    
    for cfg_dep_i in qc_dep_stabilize_configs:
        independent_i = getattr(cfg_dep_i, "independent")
        dependent_list_i = [int(dep_i) for dep_i in str(getattr(cfg_dep_i, "dependent", [])).split(",")]
        tolerance_i = getattr(cfg_dep_i, "dt_tolerance")

        for dependent_ii in dependent_list_i:
            stabilize_flags_ii = qc_dependent_quantity_base(
                df_all_w_dependent,
                independent=independent_i,
                dependent=dependent_ii,
                dt_tolerance=tolerance_i,
                return_only_dependent=True,
            )
            df_all[Df.QC_FLAG] = combine_df_all_w_dependency_output(
                df_all, stabilize_flags_ii
            )

    nb_observations = df_all.shape[0]
    df_all = gpd.GeoDataFrame(df_all, geometry=gpd.points_from_xy(df_all[Df.LONG], df_all[Df.LAT]), crs=cfg.location.crs)  # type: ignore
    # get qc check df (try to find clearer name)
    qc_config_dict = {getattr(li, "id"): li for li in cfg.QC}
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
            "bool_write_patch_to_file": cfg.other.write_flags_to_json,
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
        if isinstance(dependent, str) and "," in dependent:
            for dependent_ii in [int(ii) for ii in dependent.split(",")]:
                base_flags = qc_dependent_quantity_base(
                    df_all,
                    independent=independent,
                    dependent=dependent_ii,
                    dt_tolerance=dt_tolerance,
                )
                # df_all.update({Df.QC_FLAG: base_flags})  # type: ignore
                df_all[Df.QC_FLAG] = combine_df_all_w_dependency_output(
                    df_all, base_flags
                )
                secondary_flags = qc_dependent_quantity_secondary(
                    df_all,
                    independent=independent,
                    dependent=dependent_ii,
                    range_=tuple(dependent_i.QC.range),  # type: ignore
                    dt_tolerance=cfg.QC_dependent[0].dt_tolerance,
                )
                df_all[Df.QC_FLAG] = combine_df_all_w_dependency_output(
                    df_all, secondary_flags
                )

        else:
            base_flags = qc_dependent_quantity_base(
                df_all,
                independent=independent,
                dependent=dependent,
                dt_tolerance=dt_tolerance,
            )
            df_all[Df.QC_FLAG] = combine_df_all_w_dependency_output(df_all, base_flags)
            secondary_flags = qc_dependent_quantity_secondary(
                df_all,
                independent=independent,
                dependent=dependent,
                range_=tuple(dependent_i.QC.range),  # type: ignore
                dt_tolerance=cfg.QC_dependent[0].dt_tolerance,
            )
            df_all[Df.QC_FLAG] = combine_df_all_w_dependency_output(
                df_all, secondary_flags
            )
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

    if cfg.other.write_flags_to_json:
        write_patch_to_file(
            create_patch_json(
                df=df_all,
                columns=[Df.IOT_ID, Df.QC_FLAG],
                url_entity=Entities.OBSERVATIONS,
            ),
            file_path=Path(log.root.handlers[1].baseFilename).parent,  # type: ignore
            log_level="INFO",
        )
        write_patch_to_file(
            create_patch_json(
                df=df_all,
                columns=[Df.FEATURE_ID, Df.QC_FLAG],
                url_entity=Entities.FEATURESOFINTEREST,
            ),
            file_path=Path(log.root.handlers[1].baseFilename).parent,  # type: ignore
            log_level="INFO",
        )

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
        bool_write_patch_to_file=cfg.other.write_flags_to_json,
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
