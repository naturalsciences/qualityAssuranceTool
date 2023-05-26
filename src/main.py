import hydra
import stapy
from stapy import Query, Entity, Patch
from functools import reduce
import copy
import json
from enum import auto
from strenum import StrEnum
import numpy as np
import pandas as pd
import logging
from functools import partial
from typing import Callable
from math import ceil


class BaseQueryStrEnum(StrEnum):
    def __str__(self):
        return f"${self.name}"


class Properties(StrEnum):
    description = auto()
    unitOfMeasurement = 'unitOfMeasurement/name'
    name = auto()
    iot_id = "@iot.id"


class Settings(BaseQueryStrEnum):
    top = auto()
    skip = auto()
    count = auto()

    def __call__(self, value):
        return f"{self}={str(value)}"


class Entities(StrEnum):
    Datastreams = auto()
    ObservedProperty = auto()
    Observations = auto()

    def __call__(self, args: list[Properties] | list['Qactions']):
        out = f"{self}({';'.join(args)})"
        return out


class Qactions(BaseQueryStrEnum):
    expand = auto()
    select = auto()

    def __call__(self, arg: Entities | Properties | list[Properties] | list[Entities]):
        out = ""
        if isinstance(arg, list):
            str_arg = ','.join(arg)
            out = f"{str(self)}={str_arg}"
        return out


def inspect_datastreams_thing(entity_id: int) -> (str, list[dict[str,str | int]]):
    log.debug(f"Start inspecting entity {entity_id}.")
    base_query = Query(Entity.Thing).entity_id(entity_id)
    out_query = base_query.select(Properties.name, Properties.iot_id, Entities.Datastreams)
    additional_query = Qactions.expand([
        Entities.Datastreams(
            [
                Settings.count('true'),
                Qactions.expand([
                    Entities.ObservedProperty([Qactions.select([Properties.name,
                                                                Properties.iot_id])]),
                    Entities.Observations([Settings.count('true'),
                                           Qactions.select([Properties.iot_id]),
                                           Settings.top(0)])
                ]),
                Qactions.select([Properties.name,
                                 Properties.iot_id,
                                 Properties.description,
                                 Properties.unitOfMeasurement,
                                 Entities.ObservedProperty]),
            ]
        )
    ])
    log.debug(f"Start getting query.")
    request = json.loads(
        out_query.get_with_retry(
            out_query.get_query() + '&' + additional_query
        ).content)
    log.debug(f"Start reformatting query.")
    observ_properties, observ_count = zip(*[(ds.get(Entities.ObservedProperty).get(Properties.name), ds.get("Observations@iot.count")) for ds in request.get(Entities.Datastreams)])

    # observ_count = [ds.get("Observations@iot.count") for ds in request.get("Datastreams")]
    out = {k: request[k] for k in request.keys() if Entities.Datastreams not in k}
    out[Entities.Observations] = {
        Settings.count: sum(observ_count),
        Entities.Observations: list(set(observ_properties))
    }

    # only_results =

    def update_datastreams(ds_dict, ds_new):
        ds_out = copy.deepcopy(ds_dict)
        if ds_new.get('Observations@iot.count') > 0:
            ds_name = f"{ds_new.get(Properties.name)} -- {ds_new.get(Entities.ObservedProperty, {}).get(Properties.name)}"
            update_dsi_dict = {
                Properties.iot_id: ds_out.get(ds_new[Properties.name], {}).get(Properties.iot_id, list()) + [ds_new.get(Properties.iot_id)],
                "unitOfMeasurement": ds_out.get(ds_new[Properties.name], {}).get("unitOfMeasurement", list()) + [ds_new.get("unitOfMeasurement").get(Properties.name)],
                # "description": ds_out.get(ds_new['name'], {}).get("description", list()) + [
                #     ds_new.get("description")],
                # "ObservedProperty": ds_out.get(ds_new['name'], {}).get("ObservedProperty", list()) + [ds_new.get("ObservedProperty")],
            }
            update_ds_dict = {ds_name: ds_dict.get(ds_new.get('name'), copy.deepcopy(update_dsi_dict))}
            update_ds_dict[ds_name].update(update_dsi_dict)
            ds_out.update(update_ds_dict)
        return ds_out
    log.debug(f"Start reducing query.")
    out[Entities.Datastreams] = reduce(update_datastreams, [{}] + request.get(Entities.Datastreams))
    log.debug(f"Return result inspection.")
    return out


def extend_summary_with_result_inspection(summary_dict: (str, list[dict[str,str | int]])):
    log.debug(f"Start extending summary.")
    summary_out = copy.deepcopy(summary_dict)
    nb_streams = len(summary_out.get(Entities.Datastreams))
    for i, dsi in enumerate(summary_dict.get(Entities.Datastreams)):
        log.debug(f"Start extending datastream {i+1}/{nb_streams}.")
        iot_id_list = summary_dict.get(Entities.Datastreams).get(dsi).get(Properties.iot_id)
        results = np.empty(0)
        for iot_id_i in iot_id_list:
            results_ = Query(Entity.Datastream).entity_id(iot_id_i).sub_entity(Entity.Observation).select("result").get_data_sets()
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
        summary_out.get(Entities.Datastreams).get(dsi)["results"] = extended_sumary
    return summary_out


def testing_patch():
    Patch.observation()
    pass


def min_max_check_values(values: list[float], min_: float, max_: float):
    out = np.logical_and(values >= min_, values <= max_)
    return out


def get_iot_id_datastreams_in_qc(dict_in: dict, summary_dict: dict):
    log.debug(f"Start loop datastreams items.")
    dict_out = copy.deepcopy(dict_in)
    for k, dsi in summary_dict.get(Entities.Datastreams).items():
        property_name = k.split(" -- ", 1)[1]
        if property_name in dict_out:
            dict_out[property_name] += dsi.get(Properties.iot_id)
    return dict_out


def get_id_result_lists(iot_id):
    id_list, result_list = Query(Entity.Datastream).entity_id(iot_id).sub_entity(Entity.Observation).select(
        Properties.iot_id, "result").get_data_sets()
    log.info(f"call done")
    return id_list, result_list


def qc_df(df_in, function):
    df_out = copy.deepcopy(df_in)
    df_out["bool"] = function(df_out["result"].array)
    df_out.loc[df_out["bool"], "qc_flag"] = 2
    df_out.loc[~df_out["bool"], "qc_flag"] = 3
    return df_out


def qc_observation(iot_id: int, function: Callable):
    log.info(f"start qc {iot_id}")
    id_list, result_list = get_id_result_lists(iot_id)
    df_ = pd.DataFrame.from_dict({Properties.iot_id: id_list,
                                  "result": result_list}) \
        .astype({Properties.iot_id: int, "result": float})
    return qc_df(df_, function)


def get_results_n_datastreams(n, skip, entity_id=1):
    top_observations = 1000
    base_query = Query(Entity.Thing).entity_id(entity_id)
    out_query = base_query.select(Entities.Datastreams)
    #  additional_query = Qactions.expand([
    Q = Qactions.expand([
        Entities.Datastreams([
            Settings.top(n),
            Settings.skip(skip),
            Qactions.select([
                Properties.iot_id, Properties.unitOfMeasurement, Entities.Observations
            ]),
            Qactions.expand([
                Entities.Observations([
                    Settings.top(top_observations),
                    Qactions.select([
                        Properties.iot_id,
                        'result'
                    ])
                ]),
                Entities.ObservedProperty([
                    Qactions.select([
                        Properties.iot_id,
                        Properties.name
                    ])
                ])
            ])
        ])
    ])
    request = json.loads(
        Query(Entity.Thing).get_with_retry(
            out_query.get_query() + '&' + Q
        ).content)
    # base_query = Query(Entity.Thing).entity_id(1)
    # out_query = base_query.select(Properties.name, Properties.iot_id, Entities.Datastreams)
    # test = out_query.select(Entities.Datastreams,Entities.ObservedProperty,Entities.Observations).get_data_sets(query=query)
    return request


def datastreams_request_to_df(request_datastreams):
    df = pd.DataFrame()
    for di in request_datastreams:
        observations_list = di.get(Entities.Observations)
        if observations_list:
            df_i = pd.DataFrame(observations_list).astype({Properties.iot_id: int, "result": float})
            df_i["datastream_id"] = int(di.get(Properties.iot_id))
            df_i["observation_type"] = di.get(Entities.ObservedProperty).get(Properties.name)
            k1, k2 = Properties.unitOfMeasurement.split('/', 1)
            df_i["units"] = di.get(k1).get(k2)
            df = pd.concat([df, df_i], ignore_index=True)
    return df


def qc_on_df_per_datastream(df, datastream_id, datastream_name):
    pass


log = logging.getLogger(__name__)


@hydra.main(config_path="../conf", config_name="config.yaml", version_base="1.2")
def main(cfg):
    log.info("Start")
    stapy.set_sta_url(cfg.data_api.base_url)
    summary = inspect_datastreams_thing(1)

    df_all = pd.DataFrame()
    query_nb = "https://sensors.naturalsciences.be/sta/v1.1/Things(1)?$expand=Datastreams($count=true;$select=@iot.id)&$select=Datastreams/@iot.count"
    nb_datastreams = json.loads(Query(Entity.Datastream).get_with_retry(query_nb).content).get("Datastreams@iot.count")
    log.debug(f"{nb_datastreams=}")
    nb_streams_per_call = 10
    for i in range(ceil(nb_datastreams/nb_streams_per_call)):
        df_i = datastreams_request_to_df(
            get_results_n_datastreams(n=nb_streams_per_call, skip=nb_streams_per_call * i)[Entities.Datastreams])
        log.debug(f"{df_i.shape[0]=}")
        df_all = pd.concat([df_all, df_i],
                           ignore_index=True)
    log.debug(f"{df_all.shape=}")
    log.debug("done with df_all")

    df_all["bool"] = None
    df_all["qc_flag"] = None
    for _, row in df_all[['datastream_id', 'units', "observation_type"]].drop_duplicates().iterrows():
        d_id_i, u_i, ot_i = row.values
        df_sub = df_all.loc[df_all["datastream_id"] == d_id_i]
        cfg_ds_i = cfg.get("QC").get(ot_i, {})
        if cfg_ds_i:
            min_, max_ = cfg.get("QC").get(ot_i).get("range")
            function_i = partial(min_max_check_values, min_=min_, max_=max_)
            df_sub = qc_df(df_sub, function_i)
            df_all.loc[df_sub.index] = df_sub
    log.info("End")


if __name__ == "__main__":
    log.debug("testing...")
    main()
