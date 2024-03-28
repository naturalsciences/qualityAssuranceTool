import json
import logging
from copy import deepcopy
from dataclasses import dataclass, field
import operator
from typing import Callable

import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
from scipy import stats
from tqdm import tqdm

from pandassta.df import (Df, QualityFlags, get_acceleration_series, get_distance_geopy_series,
                         get_velocity_series)
from pandassta.df import CAT_TYPE
from pandassta.logging_constants import TQDM_DESC_FORMAT
from pandassta.logging_constants import TQDM_BAR_FORMAT

log = logging.getLogger(__name__)


# not used, kept as reference
# def get_null_mask(df: pd.DataFrame, qc_type: str) -> pd.Series:
#     mask_out = (
#         ~df[[f"qc_{'_'.join([qc_type, i])}" for i in ["min", "max"]]]
#         .isnull()
#         .any(axis=1)
#     )
#     return mask_out


def get_bool_out_of_range(
    df: pd.DataFrame, qc_on: str | tuple, qc_type: str
) -> pd.Series:
    qc_type_min = f"qc_{qc_type}_min"
    qc_type_max = f"qc_{qc_type}_max"

    columns0 = list(df.columns)
    nb_levels = df.columns.nlevels
    for ci in [qc_type_max, qc_type_min]:
        ci_i = ci
        if nb_levels > 1:
            ci_i =(ci,) + ("",)*(nb_levels-1)
        if ci_i not in columns0:
            columns0 += [ci_i]
    df = df.reindex(columns=columns0)
    mask_max_not_null = ~df[qc_type_max].isnull()
    mask_min_not_null = ~df[qc_type_min].isnull()
    s_bool_out = (df.loc[mask_max_not_null, qc_on] > df.loc[mask_max_not_null, qc_type_max]) | (df.loc[mask_min_not_null, qc_on] < df.loc[mask_min_not_null, qc_type_min])  # type: ignore

    return s_bool_out


def get_bool_null_region(df: pd.DataFrame) -> pd.Series:
    return df[Df.REGION].isnull()


def get_bool_land_region(df: pd.DataFrame) -> pd.Series:
    bool_mainland = df[Df.REGION].str.lower().str.contains("mainland").fillna(False)  # type: ignore
    return bool_mainland


def qc_region(
    df: gpd.GeoDataFrame,
    flag_none: QualityFlags = QualityFlags.PROBABLY_BAD,
    flag_mainland: QualityFlags = QualityFlags.BAD,
) -> gpd.GeoDataFrame:
    df_out = deepcopy(df)

    bool_nan = get_bool_null_region(df_out)
    df_out.loc[bool_nan.index, Df.QC_FLAG] = get_qc_flag_from_bool(
        bool_=bool_nan,
        flag_on_true=flag_none,
    )[Df.QC_FLAG]

    bool_mainland = get_bool_land_region(df_out)
    df_out.loc[bool_mainland.index, Df.QC_FLAG] = get_qc_flag_from_bool(
        bool_=bool_mainland,
        flag_on_true=flag_mainland,
    )[Df.QC_FLAG]

    df_out[Df.QC_FLAG] = df_out[Df.QC_FLAG].astype(CAT_TYPE)  # type: ignore
    log.info(f"Flags set: {df_out.loc[bool_mainland | bool_nan, [Df.QC_FLAG, Df.REGION]].value_counts(dropna=False)}")  # type: ignore
    return df_out


# TODO: refactor, complete df is not needed
def calc_gradient_results(df: pd.DataFrame, groupby: Df) -> pd.DataFrame:
    log.info(f"Start gradient calculations per {groupby}.")

    def grad_function(group):
        nb_row, nb_columns = group.shape
        if nb_row < 2:
            group[Df.GRADIENT] = None
            return group
        g = np.gradient(
            group.result, group.phenomenonTime.astype("datetime64[s]").astype("int64")
        )
        group[Df.GRADIENT] = g
        return group

    df_tmp = df.sort_values(Df.TIME)
    df_grouped = df.groupby(by=[groupby], group_keys=False)
    df_tmp = df_grouped[[str(Df.RESULT), str(Df.TIME)]].apply(
        grad_function
    )  # casted to string to avoid type error
    df_out = df.join(df_tmp[Df.GRADIENT])
    return df_out

    
def calc_zscore_results(df: pd.DataFrame, groupby: Df) -> pd.DataFrame:
    def mod_z(df_: pd.DataFrame) -> pd.Series:
        # transformed, _ = stats.yeojohnson(col.values)
        # col[col.columns[0]] = transformed.ravel()
        roll = df_.sort_values(Df.TIME).rolling("60min", on=Df.TIME, center=True)
        col = df_[Df.RESULT]
        df_["median"] = roll[Df.RESULT].median()
        df_["abs_dev"] = (df_[Df.RESULT] - df_["median"]).abs()
        # med_abs_dev = np.median(np.abs(col - med_col)) 
        # med_abs_dev = np.abs(roll[Df.RESULT] - med_col).median() # type: ignore
        roll = df_.sort_values(Df.TIME).rolling("60min", on=Df.TIME, center=True)
        df_["med_abs_dev"] = roll["abs_dev"].median()
        mod_z = 0.6745 * ((col - df_["median"]) / df_["med_abs_dev"])
        mod_z.loc[df_["med_abs_dev"] == 0] = ((col - df_["median"]) / (1.253314*roll["abs_dev"].mean()))
        # return np.abs(mod_z)
        return mod_z
    def _calc_zscore_results(df, groupby):
        group = df.groupby(by=groupby, group_keys=False)
        # group = df[[Df.TIME, Df.RESULT, groupby]].groupby(by=groupby, group_keys=False)
        # z = group[[Df.TIME, Df.RESULT]].rolling("1h", on=Df.TIME, center=True).apply(stats.zscore)
        z = group[[Df.TIME, Df.RESULT]].apply(mod_z)
        # z = group[[Df.RESULT]].apply(stats.zscore)
        return z
    df[Df.ZSCORE] = _calc_zscore_results(df, groupby=[Df.DATASTREAM_ID])
    # roll_median = df.loc[:, [Df.RESULT, Df.DATASTREAM_ID, Df.TIME]].sort_values(Df.TIME).rolling("1h", on=Df.TIME, center=True).median()
    # df.loc[:, [Df.RESULT, Df.TIME]].sort_values(Df.TIME)
    # df[Df.ZSCORE]
    # roll_median = roll.median()
    return df


def dependent_quantity_merge_asof(
    df: pd.DataFrame, independent: int, dependent: int, dt_tolerance: str
) -> pd.DataFrame:
    df_indep = (
        df.loc[df[Df.DATASTREAM_ID] == independent]
        .sort_values(Df.TIME)
        .set_index(Df.TIME)
    )
    df_dep = (
        df.loc[df[Df.DATASTREAM_ID] == dependent]
        .sort_values(Df.TIME)
        .set_index(Df.TIME)
    )

    # df_merged = pd.merge_asof(df_indep, df_dep, left_index=True, right_index=True, tolerance=pd.Timedelta('0.5s'), suffixes=[f"_{i}" for i in [independent, dependent]])
    df_merged = pd.merge_asof(
        df_dep,
        df_indep,
        left_index=True,
        right_index=True,
        tolerance=pd.Timedelta("0.5s"),
        suffixes=[f"_{i}" for i in [dependent, independent]],
    )
    df_merged = pd.DataFrame(
        df_merged.values,
        index=df_merged.index,
        columns=df_merged.columns.str.rsplit("_", expand=True, n=1),
    )
    # df_merged = df_type_conversions(df_merged)

    return df_merged


def dependent_quantity_pivot(
    df: pd.DataFrame, independent: int, dependent: int, dt_tolerance: str
) -> pd.DataFrame:
    # merge_asof is used, but creates a pivot-like table
    df_merged = dependent_quantity_merge_asof(
        df, independent=independent, dependent=dependent, dt_tolerance=dt_tolerance
    )
    return df_merged


def strip_df_to_minimal_required_dependent_quantity(df, independent, dependent):
    df_out = deepcopy(
        df.reset_index().loc[
            df[Df.DATASTREAM_ID].isin([independent, dependent]),
            [
                Df.TIME,
                Df.DATASTREAM_ID,
                Df.RESULT,
                Df.QC_FLAG,
                Df.OBSERVATION_TYPE,
                Df.IOT_ID,
            ],
        ]
    )
    return df_out


def get_bool_flagged_dependent_quantity(
    df: pd.DataFrame, independent: int, dependent: int, dt_tolerance: str
) -> pd.Series:
    df_tmp = strip_df_to_minimal_required_dependent_quantity(
        df, independent=independent, dependent=dependent
    )

    df_pivot = dependent_quantity_pivot(
        df_tmp, independent=independent, dependent=dependent, dt_tolerance=dt_tolerance
    )

    mask = ~df_pivot[Df.QC_FLAG, str(independent)].isin(
        [QualityFlags.NO_QUALITY_CONTROL, QualityFlags.GOOD]
    )
    bool_ = df[Df.IOT_ID].isin(df_pivot.loc[mask, (Df.IOT_ID, str(dependent))].values)  # type: ignore
    return bool_


def qc_dependent_quantity_base(
    df: pd.DataFrame,
    independent: int,
    dependent: int,
    dt_tolerance: str,
    flag_when_missing: QualityFlags | None = QualityFlags.BAD,
) -> pd.Series:
    df_tmp = strip_df_to_minimal_required_dependent_quantity(
        df, independent=independent, dependent=dependent
    )

    df_pivot = dependent_quantity_pivot(
        df_tmp, independent=independent, dependent=dependent, dt_tolerance=dt_tolerance
    )

    mask = ~df_pivot[Df.QC_FLAG, str(independent)].isin(
        [QualityFlags.NO_QUALITY_CONTROL, QualityFlags.GOOD]
    )
    df_pivot.loc[mask, (Df.QC_FLAG, str(dependent))] = df_pivot[mask][
        (Df.QC_FLAG, str(independent))
    ]

    df_unpivot = df_pivot.loc[mask].stack(future_stack=True).dropna(subset=Df.IOT_ID).reset_index().set_index(Df.IOT_ID)  # type: ignore
    # df_unpivot = df_pivot.loc[mask].stack().reset_index().set_index(Df.IOT_ID)
    df = df.set_index(Df.IOT_ID)
    # TODO: refactor
    mask_unpivot_notnan = ~df_unpivot[Df.QC_FLAG].isna()
    idx_unpivot_notnan = df_unpivot.loc[mask_unpivot_notnan, Df.QC_FLAG].index.astype(
        int
    )  # the conversion to int is needed because nan is float, and this column is set to float for some reason
    idx_unpivot_nan = df_unpivot.loc[~mask_unpivot_notnan, Df.QC_FLAG].index.astype(int)
    df.loc[idx_unpivot_notnan, Df.QC_FLAG] = df_unpivot.loc[
        idx_unpivot_notnan, Df.QC_FLAG
    ]
    s_out = df.loc[idx_unpivot_notnan, Df.QC_FLAG]
    if flag_when_missing:
        df.loc[idx_unpivot_nan, Df.QC_FLAG] = flag_when_missing  # type: ignore
        s_out = df.loc[idx_unpivot_notnan.union(idx_unpivot_nan), Df.QC_FLAG]
    return s_out  # type: ignore


def qc_dependent_quantity_secondary(
    df: pd.DataFrame,
    independent: int,
    dependent: int,
    range_: tuple[float, float],
    dt_tolerance: str,
) -> pd.Series:
    df_tmp = strip_df_to_minimal_required_dependent_quantity(
        df, independent=independent, dependent=dependent
    )

    df_pivot = dependent_quantity_pivot(
        df_tmp, dependent=dependent, independent=independent, dt_tolerance=dt_tolerance
    )

    df_pivot[["qc_drange_min", "qc_drange_max"]] = range_
    bool_qc = get_bool_out_of_range(
        df_pivot, (Df.RESULT, str(independent)), qc_type="drange"
    )
    df_pivot.loc[bool_qc, (Df.QC_FLAG, str(dependent))] = QualityFlags.BAD  # type: ignore Don"t know how to fix this

    df_pivot = df_pivot.drop(["qc_drange_min", "qc_drange_max"], axis=1, level=0)
    df_unpivot = df_pivot.stack(future_stack=True).dropna(subset=Df.IOT_ID).reset_index().set_index(Df.IOT_ID)  # type: ignore
    # df_unpivot = df_pivot.stack(future_stack=True).reset_index().set_index(Df.IOT_ID)  # type: ignore
    df = df.set_index(Df.IOT_ID)
    df.loc[df_unpivot.index, Df.QC_FLAG] = df_unpivot[Df.QC_FLAG]
    s_out = df.loc[df_unpivot.index, Df.QC_FLAG]
    return s_out  # type: ignore


def get_qc_flag_from_bool(
    bool_: pd.Series,
    flag_on_true: QualityFlags,
    flag_on_false: QualityFlags | None = None,
) -> pd.Series:
    qc_flag_series = pd.Series(flag_on_true, index=bool_.index, dtype=CAT_TYPE).loc[  # type: ignore
        bool_
    ]
    if flag_on_false:
        qc_flag_series = pd.concat(
            [
                qc_flag_series,
                pd.Series(flag_on_false, index=bool_.index, dtype=CAT_TYPE).loc[~bool_],  # type: ignore
            ]
        )
    return qc_flag_series


# TODO: refactor
def get_bool_spacial_outlier_compared_to_median(
    df: gpd.GeoDataFrame, max_dx_dt: float, time_window: str
) -> pd.Series:
    def delta(x):
        return np.max(x) - np.min(x)

    log.info("Start calculating spacial outliers.")
    df_time_sorted = df.sort_values(Df.TIME)
    # df_time_sorted["dt"] = df_time_sorted.loc[:, Df.TIME].dt.total_seconds()
    # df_time_sorted["dt"] = df_time_sorted[Df.TIME].diff().fillna(pd.to_timedelta("0")).dt.total_seconds()  # type: ignore
    df_time_sorted["dt"] = (df_time_sorted[Df.TIME] - df_time_sorted[Df.TIME].min()).dt.total_seconds()  # type: ignore

    bool_series_lat_eq_long = df_time_sorted[Df.LAT] == df_time_sorted[Df.LONG]
    bool_series_lat_or_long_zero = (df_time_sorted[Df.LAT] == 0) | (
        df_time_sorted[Df.LONG] == 0
    )
    entries_excluded_from_calculations = (
        bool_series_lat_eq_long | bool_series_lat_or_long_zero
    )

    log.debug(
        f"{bool_series_lat_eq_long.value_counts(dropna=False)=} (excluded from median calculations)"
        f"{bool_series_lat_or_long_zero.value_counts(dropna=False)=} (excluded from median calculations)"
        f"{entries_excluded_from_calculations.value_counts(dropna=False)=} (excluded from median calculations)"
    )

    tqdm.pandas(
        total=df.shape[0],
        desc=TQDM_DESC_FORMAT.format("Rolling median"),
        bar_format=TQDM_BAR_FORMAT,
    )
    log.debug("Start rolling median calculations.")
    rolling_median = (
        df_time_sorted.loc[
            ~entries_excluded_from_calculations, [Df.TIME, Df.LONG, Df.LAT]
        ]
        .sort_values(Df.TIME)
        .rolling(time_window, on=Df.TIME, center=True)
        .median()
        # .progress_apply(np.median)  # type: ignore
    )

    tqdm.pandas(
        total=df.shape[0],
        desc=TQDM_DESC_FORMAT.format("Rolling time"),
        bar_format=TQDM_BAR_FORMAT,
    )
    log.debug("Start rolling time calculations.")
    # calculates the time delta in each windows
    # rolling_time = (
    #     df_time_sorted.loc[:, [Df.TIME, "dt"]]
    #     .sort_values(Df.TIME)
    #     .rolling(time_window, on=Df.TIME, center=True)
    #     .apply(delta)
    # )
    # rolling_time = (rolling_t.max() - rolling_t.min()).dt
    rolling_t = (
        df_time_sorted.loc[:, [Df.TIME, "dt"]]
        .rolling(time_window, on=Df.TIME, center=True)
    )
    rolling_time = (rolling_t.max() - rolling_t.min())

    rolling_time[Df.TIME] =  df_time_sorted[Df.TIME]

    rolling_median = rolling_median.reindex(index=rolling_time.index, fill_value=None)
    rolling_median.loc[entries_excluded_from_calculations, Df.TIME] = rolling_time.loc[
        entries_excluded_from_calculations, Df.TIME
    ]
    rolling_median = rolling_median.ffill().bfill()

    ref_point = gpd.GeoDataFrame(  # type: ignore
        rolling_median,
        geometry=gpd.points_from_xy(
            rolling_median.loc[:, Df.LONG],
            rolling_median.loc[:, Df.LAT],
            crs="EPSG:4326",
        ),
    )
    ref_point["dt"] = rolling_time["dt"]
    df_time_sorted["geo_ref"] = ref_point.geometry
    df_time_sorted = gpd.GeoDataFrame(df_time_sorted)
    distance = get_distance_geopy_series(
        df_time_sorted, column1="geometry", column2="geo_ref"
    )
    bool_series = distance.values > (ref_point["dt"] * max_dx_dt).values  # type: ignore
    bool_out = pd.Series(bool_series, index=df_time_sorted.index)
    return bool_out


def get_bool_exceed_max_velocity(
    df: gpd.GeoDataFrame,
    max_velocity: float,
    velocity_series: pd.Series | None = None,
    dt_series: pd.Series | None = None,
) -> pd.Series:
    log.info("Calculating velocity outliers.")
    if velocity_series is None:
        dt_series, velocity_series = get_velocity_series(df, return_dt=True)
    if dt_series is None:
        bool_velocity = (
            velocity_series.abs() > max_velocity
        )  # *dt_series/(dt_series+pd.Timedelta())
    else:
        dt_plus_accuracy = (
            dt_series
            + ((dt_series.dropna() - dt_series.dropna().astype(int)) == 0).astype(int)
            * 1.0
        )
        bool_velocity = (
            velocity_series.abs()
            > (max_velocity / dt_series * dt_plus_accuracy)
            .rename("velocity")
            .loc[velocity_series.index]
        )
    # df["idx_"] = df.index
    # df_tmp = df.set_index(Df.FEATURE_ID)
    # df_tmp["bool_velocity"] = bool_velocity
    # bool_out = df_tmp.set_index("idx_")["bool_velocity"]
    # return bool_out
    return bool_velocity.loc[~velocity_series.isnull()]


def get_bool_exceed_max_acceleration(
    df: gpd.GeoDataFrame,
    max_acceleration: float,
    acceleration_series: pd.Series | None = None,
    dt_series: pd.Series | None = None,
) -> pd.Series:
    log.info("Calculating acceleration outliers.")
    if acceleration_series is None:
        dt_series, acceleration_series = get_acceleration_series(df, return_dt=True)
    if dt_series is None:
        bool_acceleration = acceleration_series.abs() > max_acceleration
    else:
        dt_plus_accuracy = (
            dt_series
            + ((dt_series.dropna() - dt_series.dropna().astype(int)) == 0).astype(int)
            * 1.0
        )
        bool_acceleration = (
            acceleration_series.abs()
            > (max_acceleration / dt_series**2 * dt_plus_accuracy**2)
            .rename("acceleration")
            .loc[acceleration_series.index]
        )

    # df["idx_"] = df.index
    # df_tmp = df.set_index(Df.FEATURE_ID)
    # df_tmp["bool_acceleration"] = bool_acceleration
    # bool_out = df_tmp.set_index("idx_")["bool_acceleration"]
    # return bool_out
    return bool_acceleration.loc[~acceleration_series.isnull()]


def get_depth_from_etop(lat: pd.Series, lon: pd.Series):
    datasetx = xr.open_dataset("./resources/ETOPO_2022_v1_60s_N90W180_bed.nc")

    coords_dataarray = xr.DataArray(
        list(zip(lat, lon)), dims=["points", "coords"], name="coords"
    )
    z_values = (
        datasetx["z"]
        .sel(lat=coords_dataarray[:, 0], lon=coords_dataarray[:, 1], method="nearest")
        .values
    )

    return z_values


def get_bool_depth_below_threshold(df: pd.DataFrame, threshold: float) -> pd.Series:
    mask_is_none = df[Df.REGION].isnull()  # type: ignore
    df_coords_none_unique = df.loc[mask_is_none, [Df.LONG, Df.LAT]]  # type: ignore
    bool_depth = (
        get_depth_from_etop(
            lat=df_coords_none_unique[Df.LAT],  # type: ignore
            lon=df_coords_none_unique[Df.LONG],  # type: ignore
        )
        < threshold
    )
    bool_out = pd.Series(bool_depth, index=df.loc[mask_is_none].index)  # type: ignore
    return bool_out


def get_bool_depth_above_treshold(df: pd.DataFrame, threshold: float) -> pd.Series:
    bool_out = get_bool_depth_below_threshold(df, threshold=threshold)
    return ~bool_out


@dataclass
class QCFlagConfig:
    label: str
    bool_function: Callable
    bool_merge_function: Callable
    flag_on_true: QualityFlags
    flag_on_false: QualityFlags | None = None
    flag_on_nan: QualityFlags | None = None
    bool_series: pd.Series = field(default_factory=pd.Series)
    series_out: pd.Series = field(default_factory=pd.Series)

    def execute(self, df: pd.DataFrame | gpd.GeoDataFrame):
        self.bool_series = self.bool_function(df)
        series_out = (
            df[Df.QC_FLAG].combine(  # type: ignore
                get_qc_flag_from_bool(
                    bool_=self.bool_series,
                    flag_on_true=self.flag_on_true,
                    flag_on_false=self.flag_on_false,
                ),  # type: ignore
                self.bool_merge_function,
                fill_value=self.flag_on_nan,  # type: ignore
            )
        ).astype(CAT_TYPE)
        log.info(f"Execution {self.label} qc result: {self.bool_series.sum()} True")
        self.series_out = series_out # type: ignore
        return self.series_out


def combine_dicts(a, b, op=operator.add):
    return a | b | dict([(k, op(a[k], b[k])) for k in set(b) & set(a)])


def merge_json_str(jsonstr1: str, jsonstr2: str) -> str:
    d1 = json.loads(jsonstr1)
    d2 = json.loads(jsonstr2)
    # d_out = {key: value for (key, value) in (d1.items() + d2.items())}
    d_out = combine_dicts(d1, d2)

    jsonstr_out = json.dumps(d_out)
    return jsonstr_out


def update_flag_history_series(flag_history_series, flag_config: QCFlagConfig):
    hist_tmp = pd.Series(
        json.dumps({str(flag_config.flag_on_true): [flag_config.label]}),
        index=flag_config.bool_series.loc[flag_config.bool_series].index,
    )

    history_series = hist_tmp.combine(
        flag_history_series, merge_json_str, fill_value=json.dumps({})
    )
    return history_series


FEATURES_BODY_TEMPLATE = '{"properties": {"resultQuality": "{value}"}}'
