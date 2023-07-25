import pandas as pd
import geopandas as gpd
from copy import deepcopy
from functools import partial

from models.enums import QualityFlags
from qc_functions.functions import min_max_check_values


def qc_df(df_in, function):
    # http://vocab.nerc.ac.uk/collection/L20/current/
    df_out = deepcopy(df_in)
    df_out["bool"] = function(df_out["result"].array)
    df_out.loc[df_out["bool"], "qc_flag"] = QualityFlags.PROBABLY_GOOD
    df_out.loc[~df_out["bool"], "qc_flag"] = QualityFlags.PROBABLY_BAD
    return df_out


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
                "range"
            )  # type:ignore  Don't know why this is an issue
            function_i = partial(min_max_check_values, min_=min_, max_=max_)
            df_sub = qc_df(df_sub, function_i)
            df_out.loc[df_sub.index] = df_sub
    return df_out


def qc_region(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df_out = deepcopy(df)

    bool_nan = df_out.Region.isnull()
    df_out.loc[bool_nan, "qc_flag"] = QualityFlags.PROBABLY_BAD # type: ignore

    bool_mainland = df_out.Region.str.lower().str.contains("mainland").fillna(False)
    df_out.loc[bool_mainland, "qc_flag"] = QualityFlags.BAD # type: ignore

    return df_out
