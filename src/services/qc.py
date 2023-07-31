import logging
from copy import deepcopy

import geopandas as gpd
import numpy as np
import pandas as pd

from models.enums import QualityFlags
from qc_functions.functions import min_max_check_values

from models.enums import Df

from pandas.api.types import CategoricalDtype

log = logging.getLogger(__name__)


CAT_TYPE = CategoricalDtype(list(QualityFlags), ordered=True)

def get_null_mask(df: pd.DataFrame, qc_type: str) -> pd.Series:
    mask_out = (
        ~df[[f"qc_{'_'.join([qc_type, i])}" for i in ["min", "max"]]]
        .isnull()
        .any(axis=1)
    )
    return mask_out


def get_bool_range(df: pd.DataFrame, qc_on: str | tuple, qc_type: str) -> pd.Series:
    qc_type_min = f"qc_{qc_type}_min"
    qc_type_max= f"qc_{qc_type}_max"

    # if isinstance(qc_on, tuple):
    #     qc_type_min = (qc_type_max, qc_on[1])
    #     qc_type_max= (qc_type_min, qc_on[1])

    s_bool_out = (df[qc_on] <= df[qc_type_max]) & (
        df[qc_on] >= df[qc_type_min]
    )
    return s_bool_out


# TODO: refactor
def qc_df(df_in, function):
    # http://vocab.nerc.ac.uk/collection/L20/current/
    df_out = deepcopy(df_in)
    df_out["bool"] = function(df_out[Df.RESULT].array)
    df_out.loc[df_out["bool"], Df.QC_FLAG] = QualityFlags.PROBABLY_GOOD
    df_out.loc[~df_out["bool"], Df.QC_FLAG] = QualityFlags.PROBABLY_BAD
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


def qc_region(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df_out = deepcopy(df)

    bool_nan = df_out.Region.isnull()
    df_out.loc[bool_nan, Df.QC_FLAG] = QualityFlags.PROBABLY_BAD  # type: ignore

    bool_mainland = df_out.Region.str.lower().str.contains("mainland").fillna(False)
    df_out.loc[bool_mainland, Df.QC_FLAG] = QualityFlags.BAD  # type: ignore

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


def qc_dependent_quantity_base(df: pd.DataFrame, independent: int, dependent: int):
    df_pivot = df.pivot(
        index=[Df.TIME],
        columns=[Df.DATASTREAM_ID],
        values=[Df.RESULT, Df.QC_FLAG, Df.OBSERVATION_TYPE, Df.IOT_ID],
    )
    mask = ~df_pivot[Df.QC_FLAG, independent].isin(["0", "1", "2"])
    df_pivot.loc[mask, (Df.QC_FLAG, dependent)] = df_pivot[mask][
        (Df.QC_FLAG, independent)
    ]

    df_unpivot = df_pivot.stack().reset_index().set_index(Df.IOT_ID)
    df = df.set_index(Df.IOT_ID)
    df.loc[df_unpivot.index, Df.QC_FLAG] = df_unpivot[Df.QC_FLAG]
    return df


def qc_dependent_quantity_secondary(
    df: pd.DataFrame, independent: int, dependent: int, range_: tuple[float, float]
):
    df_pivot = df.pivot(
        index=[Df.TIME],
        columns=[Df.DATASTREAM_ID],
        values=[Df.RESULT, Df.QC_FLAG, Df.OBSERVATION_TYPE, Df.IOT_ID],
    )

    df_pivot[["qc_drange_min", "qc_drange_max"]] = range_
    bool_qc = get_bool_range(df_pivot, (Df.RESULT, independent), qc_type="drange")
    df_pivot.loc[~bool_qc, (Df.QC_FLAG, dependent)] = QualityFlags.BAD #type: ignore Don"t know how to fix this 

    df_pivot = df_pivot.drop(["qc_drange_min", "qc_drange_max"], axis=1)
    df_unpivot = df_pivot.stack().reset_index().set_index(Df.IOT_ID)
    df = df.set_index(Df.IOT_ID)
    df.loc[df_unpivot.index, Df.QC_FLAG] = df_unpivot[Df.QC_FLAG]
    return df


# test needed!
def set_qc_flag_range_check(df: pd.DataFrame, qc_type: str, qc_on: Df, flag_on_fail: QualityFlags, flag_on_succes: QualityFlags | None = None) -> pd.DataFrame:
    df_out = deepcopy(df)
    df_out[Df.QC_FLAG] = df_out[Df.QC_FLAG].astype(CAT_TYPE)
    mask = get_null_mask(df_out, qc_type)
    bool_tmp = get_bool_range(df_out.loc[mask], qc_on=Df.RESULT, qc_type=qc_type)

    df_out.loc[ bool_tmp.index, Df.VERIFIED] = True
    df_out[Df.VALID] = (df_out.get(Df.VALID, True) & bool_tmp) | ~df_out[Df.VERIFIED].astype(bool) # type: ignore

    df_out.loc[(mask & (df_out[Df.QC_FLAG] < flag_on_fail)), Df.QC_FLAG] = QualityFlags(flag_on_fail) # type: ignore
    df_out[Df.QC_FLAG] = df_out[Df.QC_FLAG].astype(CAT_TYPE)
    
    return df_out
