import copy
import logging
from datetime import datetime

import numpy as np
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
