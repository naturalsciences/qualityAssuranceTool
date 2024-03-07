from pathlib import Path
from typing import List

import hydra
import numpy as np
import pandas as pd
from hydra.utils import get_original_cwd
from omegaconf import OmegaConf

from models.enums import Df, Entities, Properties, Settings
from services.config import filter_cfg_to_query
from services.qc import QualityFlags
from services.requests import (
    Entity,
    Query,
    config,
    get_query_response,
    response_datastreams_to_df,
    set_sta_url,
)
from utils.utils import get_date_from_string

OmegaConf.register_new_resolver("datetime_to_date", get_date_from_string, replace=True)


def datastream_id_in_list_filter_conditions(list_stream_id: list) -> str:
    out = ""
    tmp_list = []
    for id_i in list_stream_id:
        tmp_list.append(f"{Df.IOT_ID} eq {id_i}")
    out = " or ".join(tmp_list)
    return out


def get_results_specified_datastreams_query(
    entity_id: int,
    n: int | None = None,
    skip: int | None = None,
    top_observations: int | None = None,
    filter_condition_observations: str = "",
    filter_conditions_datastreams="",
) -> Query:
    foi = Entity(Entities.FEATUREOFINTEREST)
    foi.selection = [Properties.COORDINATES, Properties.IOT_ID]
    obs = Entity(Entities.OBSERVATIONS)
    obs.expand = [foi]
    obs.filter = filter_condition_observations
    obs.settings = [Settings.TOP(top_observations), Settings.COUNT("false")]
    obs.selection = [
        Properties.IOT_ID,
        Properties.RESULT,
        Properties.PHENOMENONTIME,
        Properties.QC_FLAG,
    ]
    obsprop = Entity(Entities.OBSERVEDPROPERTY)
    obsprop.selection = [Properties.IOT_ID, Properties.NAME]
    sens = Entity(Entities.SENSOR)
    sens.selection = [Properties.NAME, Properties.IOT_ID, Properties.DESCRIPTION]
    ds = Entity(Entities.DATASTREAMS)
    ds.filter = filter_conditions_datastreams
    ds.settings = [Settings.TOP(n), Settings.SKIP(skip)]
    ds.selection = [
        Properties.IOT_ID,
        Properties.NAME,
        Properties.DESCRIPTION,
        Properties.UNITOFMEASUREMENT,
        Entities.OBSERVATIONS,
    ]
    ds.expand = [obs, obsprop, sens]
    thing = Entity(Entities.THINGS)
    thing.id = entity_id
    thing.selection = [Entities.DATASTREAMS]
    thing.expand = [ds]
    query = Query(base_url=config.load_sta_url(), root_entity=thing)

    return query


def get_unique_value_series(series):
    unique_values = series.dropna().unique()

    try:
        out = unique_values[0]
        assert len(unique_values) == 1
        return out
    except IndexError:
        return pd.NA


def time_conversions(df: pd.DataFrame) -> pd.DataFrame:
    df[Df.TIME + "_round"] = df[Df.TIME].dt.round("1s")
    df["dt"] = np.abs((df[Df.TIME] - df[Df.TIME + "_round"]).dt.total_seconds())
    return df


def wrapper_pivot_df(df: pd.DataFrame) -> pd.DataFrame:
    pivoted = df.pivot(
        index=[Df.TIME + "_round", "dt", Df.LAT, Df.LONG, Df.IOT_ID],
        columns=[
            Df.DATASTREAM_ID,
            Properties.DESCRIPTION,
            str(Entities.SENSOR),
            Df.OBSERVATION_TYPE,
            Df.UNITS,
        ],
        values=[Df.QC_FLAG, Df.RESULT],
    )
    return pivoted


def get_flag_columns(df: pd.DataFrame, level: int = 0) -> pd.Index:
    cq = df.columns[df.columns.get_level_values(level).isin([Df.QC_FLAG])]
    return cq


# refactoring needed, too complicated function
def get_agg_data_from_pivoted(
    pivoted: pd.DataFrame, flag_columns: pd.Index | List[str]
) -> pd.DataFrame:
    pivoted[flag_columns] = pivoted[flag_columns].fillna(9).map(QualityFlags)
    datastreams = pivoted.columns.get_level_values(Df.DATASTREAM_ID).unique()

    pivoted = pivoted.droplevel(Df.IOT_ID)
    df_out = pd.DataFrame()
    df_out_coordinates = (
        pivoted.reset_index([Df.LAT, Df.LONG])[[Df.LAT, Df.LONG]]
        .sort_values([Df.TIME + "_round", "dt"])
        .groupby(Df.TIME + "_round")
        .first()
    )
    pivoted = pivoted.drop(index=[Df.LAT, Df.LONG])
    column_time_round = tuple([Df.TIME + "_round"] + ["" for i in pivoted.columns.levels[1:]])  # type: ignore
    pivoted.to_csv("raw_data.csv")
    df_out = pd.DataFrame()
    # df_out = deepcopy(df_out_coordinates)
    for ds_i in datastreams:
        df_i = pivoted[
            pivoted.columns[pivoted.columns.get_level_values(Df.DATASTREAM_ID) == ds_i]
        ]
        df_i = df_i.dropna(
            how="all",
            subset=df_i.columns[df_i.columns.get_level_values(0) == Df.RESULT],
        )
        df_i = df_i.reset_index([Df.LAT, Df.LONG], drop=True)
        qc_c_i = df_i.columns[df_i.columns.get_level_values(0) == Df.QC_FLAG]
        assert len(qc_c_i) == 1
        df_i = df_i.sort_values([Df.TIME + "_round", qc_c_i[0], "dt"]).reset_index(
            "dt", drop=True
        )
        df_i = df_i.reset_index()
        df_out_i = df_i.groupby([column_time_round]).first()

        df_out = pd.concat([df_out, df_out_i], axis=1, sort=True)

    df_out = df_out.sort_index(axis=1, level=0)
    df_out.columns = df_out.columns.swaplevel(0, 1)  # type: ignore
    df_out = df_out.sort_index(axis=1, level=0)
    df_out = pd.concat([df_out_coordinates, df_out], axis=1, sort=False)

    df_out.index.name = Df.TIME
    return df_out


def get_agg_from_response(response: dict) -> pd.DataFrame:
    df = response_datastreams_to_df(response)
    df = time_conversions(df)

    pivoted = wrapper_pivot_df(df)
    cq = get_flag_columns(pivoted)
    df_out = get_agg_data_from_pivoted(pivoted, flag_columns=cq)

    return df_out


@hydra.main(
    config_path="../conf", config_name="config_aums_request.yaml", version_base="1.2"
)
def main(cfg): # pragma: no cover
    config.filename = Path(get_original_cwd()).joinpath("outputs/.stapy.ini")
    set_sta_url(cfg.data_api.base_url)

    filter_obs_cfg = filter_cfg_to_query(cfg.data_api.filter)

    filter_ds_cfg = datastream_id_in_list_filter_conditions(
        cfg.data_api.filter.datastreams
    )
    query = get_results_specified_datastreams_query(
        cfg.data_api.things.id,
        filter_condition_observations=filter_obs_cfg,
        filter_conditions_datastreams=filter_ds_cfg,
    )

    response = get_query_response(query)
    df_out = get_agg_from_response(response)

    df_out.to_csv(cfg.csv_file)


if __name__ == "__main__":
    main()
