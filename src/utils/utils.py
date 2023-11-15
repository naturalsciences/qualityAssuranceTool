import copy
import json
import logging
import operator
from datetime import datetime
from pathlib import Path

import numpy as np
from geopandas import GeoDataFrame, points_from_xy
from pandas import DataFrame, Series
from stapy import Entity, Query

from models.constants import ISO_STR_FORMAT, ISO_STR_FORMAT2
from models.enums import Df, Entities, Properties

log = logging.getLogger(__name__)


def convert_to_datetime(value):
    try:
        d_out = datetime.strptime(value, ISO_STR_FORMAT)
    except ValueError:
        d_out = datetime.strptime(value, ISO_STR_FORMAT2)
    return d_out


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


def series_to_patch_dict(x, group_per_x=1000):
    # qc_fla is hardcoded!
    # atomicityGroup seems to improve performance, but amount of groups seems irrelevant (?)
    # UNLESS multiple runs are done simultaneously?
    d_out = {
        "id": str(x.name + 1),
        "atomicityGroup": f"Group{(int(x.name/group_per_x)+1)}",
        "method": "patch",
        "url": f"Observations({x.get(Properties.IOT_ID)})",
        "body": {"resultQuality": str(x.get(Df.QC_FLAG))},
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


def get_distance_series(df: DataFrame) -> Series:
    geodf = GeoDataFrame(  # type: ignore
        df.loc[:, [Df.TIME, Df.LAT, Df.LONG]],
        geometry=points_from_xy(df.loc[:, Df.LONG], df.loc[:, Df.LAT]),
    ).set_crs("EPSG:4326")
    distance = (
        geodf.to_crs("EPSG:4087").distance(geodf.to_crs("EPSG:4087").shift(-1)).abs() #type: ignore
    )
    return distance


def get_velocity_series(df: DataFrame) -> Series:
    dt = get_dt_series(df)
    distance = get_distance_series(df)
    velocity = distance / dt

    return velocity


def get_acceleration_series(df: DataFrame) -> Series:
    dt = get_dt_series(df)
    distance = get_distance_series(df)

    accdt = distance.shift(-1) - distance
    acc = accdt / dt
    return acc
