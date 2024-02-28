from pathlib import Path
from typing import Literal
from functools import partial

import hydra
import pandas as pd
import stapy
from hydra.utils import get_original_cwd
from omegaconf import OmegaConf
from stapy import Entity, Query

from models.enums import (
    Df,
    Entities,
    Filter,
    Properties,
    Qactions,
    Settings,
    QualityFlags,
)
from services.config import filter_cfg_to_query
from services.df import df_type_conversions
from services.qc import CAT_TYPE
from services.requests import get_query_response, response_datastreams_to_df
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
) -> Literal["str"]:
    # TODO: cleanup!!
    expand_list = [
        Entities.OBSERVATIONS(
            [
                Filter.FILTER(filter_condition_observations),
                Settings.COUNT("false"),
                Settings.TOP(top_observations),
                Qactions.SELECT(
                    [
                        Properties.IOT_ID,
                        Properties.RESULT,
                        Properties.PHENOMENONTIME,
                        Properties.QC_FLAG,
                    ]
                ),
                Qactions.EXPAND(
                    [
                        Entities.FEATUREOFINTEREST(
                            [
                                Qactions.SELECT(
                                    [Properties.COORDINATES, Properties.IOT_ID]
                                )
                            ]
                        )
                    ]
                ),
            ]
        ),
        Entities.OBSERVEDPROPERTY(
            [
                Qactions.SELECT(
                    [
                        Properties.IOT_ID,
                        Properties.NAME,
                    ]
                )
            ]
        ),
        Entities.SENSOR(
            [
                Qactions.SELECT(
                    [
                        Properties.NAME,
                        Properties.IOT_ID,
                        Properties.DESCRIPTION,
                    ]
                )
            ]
        ),
    ]
    Q = Qactions.EXPAND(
        [
            Entities.DATASTREAMS(
                [
                    Filter.FILTER(filter_conditions_datastreams),
                    Settings.TOP(n),
                    Settings.SKIP(skip),
                    Qactions.SELECT(
                        [
                            Properties.IOT_ID,
                            Properties.NAME,
                            Properties.DESCRIPTION,
                            Properties.UNITOFMEASUREMENT,
                            Entities.OBSERVATIONS,
                        ]
                    ),
                    Qactions.EXPAND(expand_list),
                ]
            )
        ]
    )
    Q_out = (
        Query(Entity.Thing)
        .entity_id(entity_id)
        .select(Entities.DATASTREAMS)
        .get_query()
        + "&"
        + Q
    )
    return Q_out


def get_unique_value_series(series):
    unique_values = series.dropna().unique()

    try:
        out = unique_values[0]
        assert len(unique_values) == 1
        return out
    except IndexError:
        return pd.NA


def get_nearest(df: pd.DataFrame, key: str | Df, value_in: pd.Timestamp | float) -> pd.DataFrame :
    a = df.iloc[(df[key]-value_in).abs().argsort()[:1]]
    return a


@hydra.main(
    config_path="../conf", config_name="config_aums_request.yaml", version_base="1.2"
)
def main(cfg):
    stapy.config.filename = Path(get_original_cwd()).joinpath("outputs/.stapy.ini")
    stapy.set_sta_url(cfg.data_api.base_url)

    thing_id = cfg.data_api.things.id

    filter_ = filter_cfg_to_query(cfg.data_api.filter)
    df = pd.read_csv(Path(get_original_cwd()).joinpath("tests/resources/df_testing.csv"))
    df[Df.QC_FLAG] = df[Df.QC_FLAG].apply(QualityFlags).astype(CAT_TYPE) #type: ignore
    df = df_type_conversions(df)
    df[Df.TIME] = pd.to_datetime(df[Df.TIME])
    df[Df.TIME + "_round"] = pd.to_datetime(df[Df.TIME + "_round"])
    df[Df.LAT] = df[Df.LAT].round(5)
    df[Df.LONG] = df[Df.LONG].round(5)
    df[Df.QC_FLAG] = df[Df.QC_FLAG].astype(str)

    pivoted = df.pivot(
        index=[Df.TIME, Df.TIME + "_round", Df.LAT, Df.LONG, Df.IOT_ID],
        columns=[
            Df.DATASTREAM_ID,
            Properties.DESCRIPTION,
            str(Entities.SENSOR),
            Df.OBSERVATION_TYPE,
            Df.UNITS,
        ],
        values=[Df.RESULT, Df.QC_FLAG],
    )
    tmp_df = (
        pivoted.reset_index()
        .sort_index(axis=1)
        .drop(columns=[Df.IOT_ID, Df.TIME], level=0)
    )
    # nearest neighbor for coordinates
    # unique for other?
    grouped_data = (
        tmp_df.xs(Df.RESULT, level=0, axis=1)
        .groupby(by=[Df.TIME + "_round", Df.LAT, Df.LONG, Df.QC_FLAG])
        .mean()
    )
    grouped_qc_flag = (
        tmp_df.xs(Df.QC_FLAG, level=0, axis=1)
        .groupby(by=[Df.TIME + "_round", Df.LAT, Df.LONG, Df.QC_FLAG])
        .max()
    )
    grouped = (
        # tmp_df.loc[tmp_df[Df.QC_FLAG] <= QualityFlags.PROBABLY_GOOD]
        tmp_df
        # .drop(columns=[Df.QC_FLAG], index=1)
        .groupby(by=[Df.TIME + "_round", Df.LAT, Df.LONG, Df.QC_FLAG])
        .mean()
    )
    grouped.columns = grouped.columns.swaplevel(0, 1)  # type: ignore
    grouped = grouped.sort_index(axis=1, level=0)

    grouped.to_csv("/tmp/testing_.csv")


if __name__ == "__main__":
    main()
