import pandas as pd
import geopandas as gpd
import numpy as np
from copy import deepcopy
from functools import partial
import logging

from models.enums import QualityFlags
from qc_functions.functions import min_max_check_values

log = logging.getLogger(__name__)


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
            min_, max_ = cfg_ds_i.get(
                "range", (0,0)
            )
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
        group["grad"] = g
        return group

    # np.gradient(df_idexed.result.values, df_idexed.index.get_level_values("phenomenonTime").astype('datetime64[s]').astype('int64'))

    # df_idexed.result.groupby(level=["datastream_id"], group_keys=False).apply(lambda x: pd.DataFrame(np.gradient(x, x.index.get_level_values("phenomenonTime").astype("datetime64[s]").astype('int64'), axis=0)))

    # df['wc'].groupby(level = ['model'], group_keys=False)
    #   .apply(lambda x: #do all the columns at once, specifying the axis in gradient
    #          pd.DataFrame(np.gradient(x, x.index.get_level_values(0), axis=0),
    #                       columns=x.columns, index=x.index))

    df_out = df.groupby([groupby], group_keys=False).apply(grad_function)
    return df_out
