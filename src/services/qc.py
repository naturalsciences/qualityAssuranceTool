import logging
from copy import deepcopy
from functools import partial

import geopandas as gpd
import numpy as np
import pandas as pd

from models.enums import QualityFlags
from qc_functions.functions import min_max_check_values

log = logging.getLogger(__name__)


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
    df_out["bool"] = function(df_out["result"].array)
    df_out.loc[df_out["bool"], "qc_flag"] = QualityFlags.PROBABLY_GOOD
    df_out.loc[~df_out["bool"], "qc_flag"] = QualityFlags.PROBABLY_BAD
    return df_out


# TODO: refactor
def qc_on_df(df: pd.DataFrame, cfg: dict[str, dict]) -> pd.DataFrame:
    df_out = deepcopy(df)
    # df_out["bool"] = None
    # df_out["qc_flag"] = None
    for _, row in (
        df_out[["datastream_id", "units", "observation_type"]]
        .drop_duplicates()
        .iterrows()
    ):
        d_id_i, u_i, ot_i = row.values
        df_sub = df_out.loc[df_out["datastream_id"] == d_id_i]
        cfg_ds_i = cfg.get("QC", {}).get(ot_i, {})
        if cfg_ds_i:
            min_, max_ = cfg_ds_i.get("range", (0, 0))
            function_i = partial(min_max_check_values, min_=min_, max_=max_)
            df_sub = qc_df(df_sub, function_i)
            df_out.loc[df_sub.index] = df_sub
    return df_out


def qc_region(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df_out = deepcopy(df)

    bool_nan = df_out.Region.isnull()
    df_out.loc[bool_nan, "qc_flag"] = QualityFlags.PROBABLY_BAD  # type: ignore

    bool_mainland = df_out.Region.str.lower().str.contains("mainland").fillna(False)
    df_out.loc[bool_mainland, "qc_flag"] = QualityFlags.BAD  # type: ignore

    return df_out


def calc_gradient_results(df, groupby):
    log.debug(f"Start gradient calculations per {groupby}.")

    def grad_function(group):
        g = np.gradient(
            group.result, group.phenomenonTime.astype("datetime64[s]").astype("int64")
        )
        group["gradient"] = g
        return group

    # np.gradient(df_idexed.result.values, df_idexed.index.get_level_values("phenomenonTime").astype('datetime64[s]').astype('int64'))

    # df_idexed.result.groupby(level=["datastream_id"], group_keys=False).apply(lambda x: pd.DataFrame(np.gradient(x, x.index.get_level_values("phenomenonTime").astype("datetime64[s]").astype('int64'), axis=0)))

    # df['wc'].groupby(level = ['model'], group_keys=False)
    #   .apply(lambda x: #do all the columns at once, specifying the axis in gradient
    #          pd.DataFrame(np.gradient(x, x.index.get_level_values(0), axis=0),
    #                       columns=x.columns, index=x.index))

    df_out = df.groupby([groupby], group_keys=False).apply(grad_function)
    return df_out


def qc_dependent_quantity_base(df: pd.DataFrame, independent: int, dependent: int):
    df_pivot = df.pivot(
        index=["phenomenonTime"],
        columns=["datastream_id"],
        values=["result", "qc_flag", "observation_type", "@iot.id"],
    )
    mask = ~df_pivot["qc_flag", independent].isin(["0", "1", "2"])
    df_pivot.loc[mask, ("qc_flag", dependent)] = df_pivot[mask][
        ("qc_flag", independent)
    ]

    df_unpivot = df_pivot.stack().reset_index().set_index("@iot.id")
    df = df.set_index("@iot.id")
    df.loc[df_unpivot.index, "qc_flag"] = df_unpivot["qc_flag"]
    return df


def qc_dependent_quantity_secondary(
    df: pd.DataFrame, independent: int, dependent: int, range_: tuple[float, float]
):
    df_pivot = df.pivot(
        index=["phenomenonTime"],
        columns=["datastream_id"],
        values=["result", "qc_flag", "observation_type", "@iot.id"],
    )

    df_pivot[["qc_drange_min", "qc_drange_max"]] = range_
    bool_qc = get_bool_range(df_pivot, ("result", independent), qc_type="drange")
    df_pivot.loc[~bool_qc, ("qc_flag", dependent)] = QualityFlags.BAD #type: ignore Don"t know how to fix this 

    df_pivot = df_pivot.drop(["qc_drange_min", "qc_drange_max"], axis=1)
    df_unpivot = df_pivot.stack().reset_index().set_index("@iot.id")
    df = df.set_index("@iot.id")
    df.loc[df_unpivot.index, "qc_flag"] = df_unpivot["qc_flag"]
    return df
