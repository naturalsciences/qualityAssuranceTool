import logging
import copy
from typing import Tuple
import numpy as np
from functools import reduce
from requests import Response
from stapy import Query, Entity
from enums import Properties, Entities, Settings, Qactions


log = logging.getLogger(__name__)

def build_query_datastreams(entity_id: int) -> str:
    base_query = Query(Entity.Thing).entity_id(entity_id)
    out_query = base_query.select(
        Properties.NAME, Properties.IOT_ID, Entities.DATASTREAMS
    )
    additional_query = Qactions.EXPAND(
        [
            Entities.DATASTREAMS(
                [
                    Settings.COUNT("true"),
                    Qactions.EXPAND(
                        [
                            Entities.OBSERVEDPROPERTY(
                                [Qactions.SELECT([Properties.NAME, Properties.IOT_ID])]
                            ),
                            Entities.OBSERVATIONS(
                                [
                                    Settings.COUNT("true"),
                                    Qactions.SELECT([Properties.IOT_ID]),
                                    Settings.TOP(0),
                                ]
                            ),
                        ]
                    ),
                    Qactions.SELECT(
                        [
                            Properties.NAME,
                            Properties.IOT_ID,
                            Properties.DESCRIPTION,
                            Properties.UNITOFMEASUREMENT,
                            Entities.OBSERVEDPROPERTY,
                        ]
                    ),
                ]
            )
        ]
    ) 
    return out_query.get_query() + "&" + additional_query

def get_request(query: str) -> Tuple[int, dict]:
    request: Response = Query(Entity.Thing).entity_id(0).get_with_retry(query)
    request_out = request.json()
    return request.status_code, request_out

def inspect_datastreams_thing(entity_id: int) -> dict:
    out_query = build_query_datastreams(entity_id=entity_id)
    log.debug(f"Start inspecting entity {entity_id}.")
    log.debug(f"Start getting query.")
    status: int
    request: dict[Entities, list[dict]]
    status, request = get_request(out_query)
    # request = json.loads(
    #     Query(Entity.Thing).entity_id(entity_id).get_with_retry(out_query).content
    # )
    log.debug(f"Start reformatting query.")
    observ_properties, observ_count = zip(
        *[
            (
                ds.get(Entities.OBSERVEDPROPERTY, {}).get(Properties.NAME),
                ds.get("observations@iot.count"),
            )
            for ds in request.get(Entities.DATASTREAMS, [])
        ]
    )

    # observ_count = [ds.get("OBSERVATIONS@iot.COUNT") for ds in request.get("DATASTREAMS")]
    out = {k: request[k] for k in request.keys() if Entities.DATASTREAMS not in k}
    out[Entities.OBSERVATIONS] = { #type:ignore
        Settings.COUNT: sum(observ_count), #type:ignore
        Entities.OBSERVATIONS: list(set(observ_properties)),
    }

    # only_results =

    def update_datastreams(ds_dict, ds_new):
        ds_out = copy.deepcopy(ds_dict)
        if ds_new.get("observations@iot.count") > 0:
            ds_name = f"{ds_new.get(Properties.NAME)} -- {ds_new.get(Entities.OBSERVEDPROPERTY, {}).get(Properties.NAME)}"
            update_dsi_dict = {
                Properties.IOT_ID: ds_out.get(ds_new[Properties.NAME], {}).get(
                    Properties.IOT_ID, list()
                )
                + [ds_new.get(Properties.IOT_ID)],
                "unitOfMeasurement": ds_out.get(ds_new[Properties.NAME], {}).get(
                    "unitOfMeasurement", list()
                )
                + [ds_new.get("unitOfMeasurement").get(Properties.NAME)],
                # "description": ds_out.get(ds_new['name'], {}).get("description", list()) + [
                #     ds_new.get("description")],
                # "OBSERVEDPROPERTY": ds_out.get(ds_new['name'], {}).get("OBSERVEDPROPERTY", list()) + [ds_new.get("OBSERVEDPROPERTY")],
            }
            update_ds_dict = {
                ds_name: ds_dict.get(ds_new.get("name"), copy.deepcopy(update_dsi_dict))
            }
            update_ds_dict[ds_name].update(update_dsi_dict)
            ds_out.update(update_ds_dict)
        return ds_out

    log.debug(f"Start reducing query.")
    out[Entities.DATASTREAMS] = reduce(
        update_datastreams, [{}] + request.get(Entities.DATASTREAMS) #type:ignore
    )
    log.debug(f"Return result inspection.")
    return out

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
                .select("result")
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
        summary_out.get(Entities.DATASTREAMS).get(dsi)["results"] = extended_sumary  # type: ignore
    return summary_out
