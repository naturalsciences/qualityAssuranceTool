import copy
import json
import logging
import operator
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import List, Tuple

import numpy as np
from geopandas import GeoDataFrame, points_from_xy
from geopy import distance as geopy_distance
from pandas import DataFrame, Series
from stapy import Entity, Query
from tqdm import tqdm

from models.constants import (ISO_STR_FORMAT, ISO_STR_FORMAT2, TQDM_BAR_FORMAT,
                              TQDM_DESC_FORMAT)
from models.enums import Df, Entities, Properties

log = logging.getLogger(__name__)


def convert_to_datetime(value: str) -> datetime:
    try:
        d_out = datetime.strptime(value, ISO_STR_FORMAT)
        return d_out
    except ValueError as e:
        d_out = datetime.strptime(value, ISO_STR_FORMAT2)
        return d_out
    except Exception as e:
        log.exception(e)
        raise e


def extend_summary_with_result_inspection(summary_dict: dict[str, list]):
    log.debug(f"Start extending summary.")
    summary_out = copy.deepcopy(summary_dict)
    nb_streams = len(summary_out.get(Entities.DATASTREAMS, []))
    for i, dsi in enumerate(summary_dict.get(Entities.DATASTREAMS, [])):
        log.debug(f"Start extending datastream {i+1}/{nb_streams}.")
        iot_id_list = summary_dict.get(Entities.DATASTREAMS, []).get(dsi).get(Properties.iot_id)  # type: ignore
        results = np.empty(0)
        for iot_id_i in iot_id_list:
            results_ = (
                Query(Entity.Datastream)
                .entity_id(iot_id_i)
                .sub_entity(Entity.Observation)
                .select(Properties.RESULT)
                .get_data_sets()
            )
            results = np.concatenate([results, results_])
        min = np.min(results)
        max = np.max(results)
        mean = np.mean(results)
        median = np.median(results)
        nb = np.shape(results)[0]

        extended_sumary = {
            "min": min,
            "max": max,
            "mean": mean,
            "median": median,
            "nb": nb,
        }
        summary_out.get(Entities.DATASTREAMS).get(dsi)[Properties.RESULT] = extended_sumary  # type: ignore
    return summary_out


def series_to_patch_dict(
    x,
    group_per_x=1000,
    url_entity: Entities = Entities.OBSERVATIONS,
    columns: List[Df] = [Df.IOT_ID, Df.QC_FLAG],
    json_body_template: str | None = None,
):
    # qc_flag is hardcoded!
    # atomicityGroup seems to improve performance, but amount of groups seems irrelevant (?)
    # UNLESS multiple runs are done simultaneously?
    body_default = '{"resultQuality": "{value}"}'
    if not json_body_template:
        json_body_template = body_default

    def create_json(template, value):
        # Load the JSON template
        template_json = json.loads(template)

        def replace_value(template_json, value):
            # Replace the placeholder with the given value
            for key, val in template_json.items():
                if isinstance(val, dict):
                    replace_value(val, value)
                elif isinstance(val, str) and "{value}" in val:
                    if isinstance(value, int):
                        template_json[key] = int(val.format(value=value))
                    elif isinstance(value, float):
                        template_json[key] = float(val.format(value))
                    else:
                        template_json[key] = val.format(value=value)

        replace_value(template_json, value)
        return template_json

    d_out = {
        "id": str(x.name + 1),
        "atomicityGroup": f"Group{(int(x.name/group_per_x)+1)}",
        "method": "patch",
        "url": f"{url_entity}({x.get(columns[0])})",
        "body": create_json(json_body_template, int(str(x.get(columns[1])))),
    }
    return d_out


def update_response(
    d: dict[str, int | float | str | list], u: dict[str, str | list]
) -> dict[str, int | float | str | list]:
    common_keys = set(d.keys()).intersection(u.keys())

    assert all([type(d[k]) == type(u[k]) for k in common_keys])

    for k, v in u.items():
        if isinstance(v, list) and k in d.keys():
            d[k] = sum([d[k], v], [])
        else:
            d[k] = v
    return d


def find_nearest_idx(array, value):
    # array = np.asarray(array)
    idx = (np.abs(array - value)).argmin()
    return idx


def get_absolute_path_to_base():
    current_file = Path(__file__)
    idx_src = current_file.parts.index("src")
    out = current_file.parents[len(current_file.parts) - idx_src - 1]
    return out


def combine_dicts(a, b, op=operator.add):
    return a | b | dict([(k, op(a[k], b[k])) for k in set(b) & set(a)])


def merge_json_str(jsonstr1: str, jsonstr2: str) -> str:
    d1 = json.loads(jsonstr1)
    d2 = json.loads(jsonstr2)
    # d_out = {key: value for (key, value) in (d1.items() + d2.items())}
    d_out = combine_dicts(d1, d2)

    jsonstr_out = json.dumps(d_out)
    return jsonstr_out


def get_dt_series(df: DataFrame) -> Series:
    dt = (df[Df.TIME].shift(-1) - df[Df.TIME]).dt.total_seconds().abs()
    return dt


def get_distance_projection_series(df: DataFrame) -> Series:
    geodf = GeoDataFrame(  # type: ignore
        df.loc[:, [Df.TIME, Df.LAT, Df.LONG]],
        geometry=points_from_xy(df.loc[:, Df.LONG], df.loc[:, Df.LAT]),
    ).set_crs("EPSG:4326")
    distance = (
        geodf.to_crs("EPSG:4087").distance(geodf.to_crs("EPSG:4087").shift(-1)).abs()  # type: ignore
    )
    return distance


def get_distance_geopy_series(
    df: GeoDataFrame, column1: str = "geometry", column2: str = "None"
) -> Series:
    df_copy = copy.deepcopy(df)

    def get_distance_geopy_i(row_i, column1=column1, column2=column2):
        point1 = row_i[column1]
        point2 = row_i[column2]
        if not point2:
            return None
        lat1: float = point1.y
        lon1: float = point1.x
        lat2: float = point2.y
        lon2: float = point2.x
        return geopy_distance.distance((lat1, lon1), (lat2, lon2)).meters

    if column2 == "None":
        column2 = "geometry_shifted"
        shifted_geometry_values = copy.deepcopy(df["geometry"].shift(-1)).values  # type: ignore
        df_copy[column2] = shifted_geometry_values
    log.info("Start distance calculations.")
    tqdm.pandas(
        total=df.shape[0],
        bar_format=TQDM_BAR_FORMAT,
        desc=TQDM_DESC_FORMAT.format("Calculate distance"),
    )
    distances_series = df_copy.progress_apply(  # type: ignore
        partial(get_distance_geopy_i, column1=column1, column2=column2), axis=1
    )
    return distances_series  # type: ignore


def get_velocity_series(df: GeoDataFrame, return_dt=False) -> Series | Tuple[Series, Series]:
    log.info("Velocity calculations.")
    # df_sorted = df.set_index(Df.FEATURE_ID).sort_values(Df.TIME)
    df_sorted = df.sort_values(Df.TIME).drop_duplicates(subset=[Df.TIME, Df.FEATURE_ID])
    dt = get_dt_series(df_sorted)
    distance = get_distance_geopy_series(df_sorted)  # type: ignore
    velocity = distance / dt

    velocity = velocity.bfill().replace(np.inf, np.NAN)
    if return_dt:
        return (dt.rename("dt"), velocity.rename("velocity"))
    return velocity


def get_acceleration_series(df: GeoDataFrame, return_dt=False) -> Series | Tuple[Series, Series]:
    log.info("Acceleration calculations.")
    df_sorted = df.sort_values(Df.TIME).drop_duplicates(subset=[Df.TIME, Df.FEATURE_ID])
    dt = get_dt_series(df_sorted)
    dt, velocity = get_velocity_series(df, return_dt=True)  # type: ignore

    accdt = velocity.shift(-1) - velocity
    acc = accdt / dt
    acc = acc.bfill().replace(np.inf, np.NAN)
    if return_dt:
        return (dt.rename("dt"), acc.rename("acceleration))"))
    return acc.rename("acceleration))")


def get_dt_and_distance_series(df: GeoDataFrame) -> Tuple[Series, Series]:
    log.info("Distance calculations.")
    # df_tmp = df.sort_values(Df.TIME).groupby(Df.FEATURE_ID).first()
    df_tmp = df.sort_values(Df.TIME).drop_duplicates(subset=[Df.TIME, Df.FEATURE_ID])
    dt = get_dt_series(df_tmp)
    distance = get_distance_geopy_series(df_tmp)  # type: ignore
    return (dt, distance)
 

def get_dt_velocity_and_acceleration_series(df: GeoDataFrame) -> Tuple[Series, Series, Series]:
    log.info("Velocity and acceleration calculations.")
    dt, distance = get_dt_and_distance_series(df)

    velocity = distance / dt
    # velocity = (distance / dt).bfill()

    accdt = velocity.shift(-1) - velocity
    acc = accdt / dt

    velocity = velocity.bfill().replace(np.inf, np.NAN)
    velocity = velocity.bfill().replace(-np.inf, np.NAN)
    acc = acc.bfill().replace(np.inf, np.NAN)
    acc = acc.bfill().replace(-np.inf, np.NAN)
    velocity_out = Series(index=df.index)
    velocity_out.loc[velocity.index] = velocity
    velocity_out = velocity_out.rename("velocity")

    acc_out = Series(index=df.index)
    acc_out.loc[acc.index] = acc
    acc_out = acc_out.rename("acceleration")

    dt_out = Series(index=df.index)
    dt_out.loc[dt.index] = dt
    dt_out = dt_out.rename("dt")
    
    return (dt, velocity_out, acc_out)
