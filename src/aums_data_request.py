from pathlib import Path
from typing import Literal

import hydra
import pandas as pd
import stapy
from hydra.utils import get_original_cwd
from omegaconf import OmegaConf
from stapy import Entity, Query

from models.enums import Df, Entities, Filter, Properties, Qactions, Settings
from services.config import filter_cfg_to_query
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
    expand_feature_of_interest: bool = True,
) -> Literal["str"]:
    # TODO: cleanup!!
    idx_slice: int = 3
    if expand_feature_of_interest:
        idx_slice = 4
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
            ][:idx_slice]
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


@hydra.main(
    config_path="../conf", config_name="config_aums_request.yaml", version_base="1.2"
)
def main(cfg):
    stapy.config.filename = Path(get_original_cwd()).joinpath("outputs/.stapy.ini")
    stapy.set_sta_url(cfg.data_api.base_url)

    thing_id = cfg.data_api.things.id

    filter_ = filter_cfg_to_query(cfg.data_api.filter)
    # counting observations per stream increases computational time drastically
    query_ds_info = get_results_specified_datastreams_query(
        cfg.data_api.things.id, top_observations=0, filter_condition_observations=filter_
    )
    ds_info = get_query_response(query_ds_info, follow_obs_nextlinks=False)
    df_info = pd.DataFrame(ds_info["Datastreams"])
    df_info["unitOfMeasurement"] = (
        df_info["unitOfMeasurement"]
        .apply(lambda x: x.get("name", None))
        .astype("string")
    )

    df_info = pd.concat(
        [
            df_info,
            df_info["ObservedProperty"].apply(pd.Series).add_prefix("ObservedProperty_"),
            df_info["Sensor"].apply(pd.Series).add_prefix("Sensor_"),
        ],
        axis=1,
    ).drop(
        columns=["Observations", "Observations@iot.navigationLink", "Sensor", "ObservedProperty"]
    )


    df_info["description"] = df_info["description"].astype("string")

    column_order = ["@iot.id", "name", "description", "Sensor_name", "ObservedProperty_name", "unitOfMeasurement"]
    column_remaining = list(set(df_info.columns).difference(column_order))

    df_info = df_info[column_order + column_remaining]
    df_info["Sensor_name"] = df_info["Sensor_name"].str.replace("11BU RV Belgica ", "")

    df_info.to_csv("/tmp/testing.csv")
    return 0

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
    df = response_datastreams_to_df(response)
    df[Df.TIME + "_round"] = df[Df.TIME].dt.round("1s")

    pivoted = df.pivot(
        index=[Df.TIME, Df.TIME + "_round", Df.LAT, Df.LONG, Df.IOT_ID],
        columns=[Df.DATASTREAM_ID, Df.OBSERVATION_TYPE, Df.UNITS],
        values=[Df.QC_FLAG, Df.RESULT],
    )
    grouped = (
        pivoted.reset_index()
        .sort_index(axis=1)
        .drop(columns=[Df.IOT_ID, Df.TIME], level=0)
        .groupby(by=[Df.TIME + "_round", Df.LAT, Df.LONG])
        .agg(get_unique_value_series)
    )
    grouped.columns = grouped.columns.swaplevel(0, 1)  # type: ignore
    grouped = grouped.sort_index(axis=1, level=0)

    grouped.to_csv(cfg.csv_file)


if __name__ == "__main__":
    main()
