import logging
from copy import deepcopy
from itertools import compress

import geopandas as gpd
import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype
from shapely.geometry import Point

from models.enums import Df, QualityFlags
from qc_functions.functions import min_max_check_values

log = logging.getLogger(__name__)


CAT_TYPE = CategoricalDtype(list(QualityFlags), ordered=True)


def get_null_mask(df: pd.DataFrame, qc_type: str) -> pd.Series:
    mask_out = (
        ~df[[f"qc_{'_'.join([qc_type, i])}" for i in ["min", "max"]]]
        .isnull()
        .any(axis=1)
    )
    return mask_out


def get_bool_out_of_range(
    df: pd.DataFrame, qc_on: str | tuple, qc_type: str
) -> pd.Series:
    qc_type_min = f"qc_{qc_type}_min"
    qc_type_max = f"qc_{qc_type}_max"

    # if isinstance(qc_on, tuple):
    #     qc_type_min = (qc_type_max, qc_on[1])
    #     qc_type_max= (qc_type_min, qc_on[1])

    s_bool_out = ((df[qc_on] > df[qc_type_max]) & ~df[qc_type_max].isnull()) | (
        (df[qc_on] < df[qc_type_min]) & ~df[qc_type_min].isnull()
    )
    return s_bool_out


# TODO: refactor
def qc_df(df_in, function):
    # http://vocab.nerc.ac.uk/collection/L20/current/
    df_out = deepcopy(df_in)
    df_out["bool"] = function(df_out[Df.RESULT].array)
    df_out.loc[~df_out["bool"], Df.QC_FLAG] = QualityFlags.PROBABLY_GOOD
    df_out.loc[df_out["bool"], Df.QC_FLAG] = QualityFlags.PROBABLY_BAD
    return df_out


##  TODO: refactor
# def qc_on_df(df: pd.DataFrame, cfg: dict[Df, dict]) -> pd.DataFrame:
#     df_out = deepcopy(df)
#     # df_out["bool"] = None
#     # df_out[Df.QC_FLAG] = None
#     for _, row in (
#         df_out[[Df.DATASTREAM_ID, Df.UNITS, Df.OBSERVATION_TYPE]]
#         .drop_duplicates()
#         .iterrows()
#     ):
#         d_id_i, u_i, ot_i = row.values
#         df_sub = df_out.loc[df_out[Df.DATASTREAM_ID] == d_id_i]
#         cfg_ds_i = cfg.get("QC", {}).get(ot_i, {})
#         if cfg_ds_i:
#             min_, max_ = cfg_ds_i.get("range", (0, 0))
#             function_i = partial(min_max_check_values, min_=min_, max_=max_)
#             df_sub = qc_df(df_sub, function_i)
#             df_out.loc[df_sub.index] = df_sub
#     return df_out


def get_bool_null_region(df: pd.DataFrame) -> pd.Series:
    return df[Df.REGION].isnull()


def get_bool_land_region(df: pd.DataFrame) -> pd.Series:
    bool_mainland = df[Df.REGION].str.lower().str.contains("mainland").fillna(False)  # type: ignore
    return bool_mainland


def qc_region(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df_out = deepcopy(df)

    bool_nan = get_bool_null_region(df_out)
    df_out.loc[bool_nan.index, Df.QC_FLAG] = get_qc_flag_from_bool(
        df=df_out.loc[bool_nan.index],
        bool_=bool_nan,
        flag_on_true=QualityFlags.PROBABLY_BAD,
        update_verified=False,
    )[Df.QC_FLAG]

    bool_mainland = get_bool_land_region(df_out)
    df_out.loc[bool_mainland.index, Df.QC_FLAG] = get_qc_flag_from_bool(
        df=df_out.loc[bool_mainland.index],
        bool_=bool_mainland,
        flag_on_true=QualityFlags.BAD,
        update_verified=False,
    )[Df.QC_FLAG]

    df_out[Df.QC_FLAG] = df_out[Df.QC_FLAG].astype(CAT_TYPE)  # type: ignore
    log.info(f"Flags set: {df_out.loc[bool_mainland | bool_nan, [Df.QC_FLAG, Df.REGION]].value_counts(dropna=False)}")  # type: ignore
    return df_out


# TODO: refactor, complete df is not needed
def calc_gradient_results(df: pd.DataFrame, groupby: Df):
    log.debug(f"Start gradient calculations per {groupby}.")

    def grad_function(group):
        g = np.gradient(
            group.result, group.phenomenonTime.astype("datetime64[s]").astype("int64")
        )
        group[Df.GRADIENT] = g
        return group

    df_out = df.sort_values(Df.TIME)
    df_out = df.groupby([groupby], group_keys=False).apply(grad_function)
    return df_out


def dependent_quantity_merge_asof(df: pd.DataFrame, independent, dependent):
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

    return df_merged


def dependent_quantity_pivot(df: pd.DataFrame, independent, dependent):
    # merge_asof is used, but creates a pivot-like table
    df_merged = dependent_quantity_merge_asof(
        df, independent=independent, dependent=dependent
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


def qc_dependent_quantity_base(df: pd.DataFrame, independent: int, dependent: int):
    df_tmp = strip_df_to_minimal_required_dependent_quantity(
        df, independent=independent, dependent=dependent
    )

    df_pivot = dependent_quantity_pivot(
        df_tmp, independent=independent, dependent=dependent
    )

    mask = ~df_pivot[Df.QC_FLAG, str(independent)].isin(
        [QualityFlags.NO_QUALITY_CONTROL, QualityFlags.GOOD]
    )
    df_pivot.loc[mask, (Df.QC_FLAG, str(dependent))] = df_pivot[mask][
        (Df.QC_FLAG, str(independent))
    ]

    df_unpivot = df_pivot.stack().reset_index().set_index(Df.IOT_ID)
    df = df.set_index(Df.IOT_ID)
    df.loc[df_unpivot.index, Df.QC_FLAG] = df_unpivot[Df.QC_FLAG]
    df.loc[df[Df.QC_FLAG].isna(), Df.QC_FLAG] = QualityFlags.BAD  # type: ignore
    return df.reset_index()


def qc_dependent_quantity_secondary(
    df: pd.DataFrame, independent: int, dependent: int, range_: tuple[float, float]
):
    df_tmp = strip_df_to_minimal_required_dependent_quantity(
        df, independent=independent, dependent=dependent
    )

    df_pivot = dependent_quantity_pivot(
        df_tmp, dependent=dependent, independent=independent
    )

    df_pivot[["qc_drange_min", "qc_drange_max"]] = range_
    bool_qc = get_bool_out_of_range(
        df_pivot, (Df.RESULT, str(independent)), qc_type="drange"
    )
    df_pivot.loc[bool_qc, (Df.QC_FLAG, str(dependent))] = QualityFlags.BAD  # type: ignore Don"t know how to fix this

    df_pivot = df_pivot.drop(["qc_drange_min", "qc_drange_max"], axis=1)
    df_unpivot = df_pivot.stack().reset_index().set_index(Df.IOT_ID)
    df = df.set_index(Df.IOT_ID)
    df.loc[df_unpivot.index, Df.QC_FLAG] = df_unpivot[Df.QC_FLAG]
    return df.reset_index()


def get_qc_flag_from_bool(
    df: pd.DataFrame,
    bool_: pd.Series,
    flag_on_true: QualityFlags,
    update_verified: bool,
) -> pd.DataFrame:
    df.loc[bool_.index, Df.VERIFIED] = True
    df[Df.VALID] = (df.get(Df.VALID, True) & bool_) | ~df[Df.VERIFIED].astype(bool)  # type: ignore

    df.loc[bool_ & (df[Df.QC_FLAG] < QualityFlags(flag_on_true)), Df.QC_FLAG] = QualityFlags(flag_on_true)  # type: ignore
    df[Df.QC_FLAG] = df[Df.QC_FLAG].astype(CAT_TYPE)

    columns_out = list(
        compress([Df.QC_FLAG, Df.VALID, Df.VERIFIED], [True, True, update_verified])
    )
    return df[columns_out]


# test needed!
def set_qc_flag_range_check(
    df: pd.DataFrame, qc_type: str, qc_on: Df, flag_on_fail: QualityFlags
) -> pd.DataFrame:
    df_out = deepcopy(df)
    df_out[Df.QC_FLAG] = df_out[Df.QC_FLAG].astype(CAT_TYPE)
    # mask = get_null_mask(df_out, qc_type)
    # bool_tmp = get_bool_out_of_range(df_out.loc[mask], qc_on=qc_on, qc_type=qc_type)
    bool_tmp = get_bool_out_of_range(df_out, qc_on=qc_on, qc_type=qc_type)

    df_tmp = get_qc_flag_from_bool(
        # df_out.loc[mask],
        df_out,
        bool_=bool_tmp,
        flag_on_true=QualityFlags.BAD,
        update_verified=True,
    )
    df_out.loc[df_tmp.index, df_tmp.columns] = df_tmp

    return df_out


# TODO: add check distance between consecutive points
def get_bool_spacial_outlier_compared_to_median(
    df: gpd.GeoDataFrame, max_dx_dt: float, time_window: str
) -> pd.Series:
    rolling_median = (
        df.loc[:, [Df.TIME, Df.LONG, Df.LAT]]
        .sort_values(Df.TIME)
        .rolling(time_window, on=Df.TIME)
        .apply(np.median)
    )
    ref_point = gpd.GeoDataFrame(  # type: ignore
        rolling_median,
        geometry=gpd.points_from_xy(
            rolling_median.loc[:, Df.LONG], rolling_median.loc[:, Df.LAT]
        ),
    ).set_crs("EPSG:4326")
    bool_series = (
        df.sort_values(Df.TIME)
        .loc[:, "geometry"]
        .to_crs("EPSG:4087")
        .distance(ref_point.loc[:, "geometry"].to_crs("EPSG:4087"))
        > pd.Timedelta(time_window).total_seconds() * max_dx_dt
    )
    return bool_series
