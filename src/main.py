import hydra
import stapy
from stapy import Query, Entity, Patch
from functools import reduce
import copy
import json
from enum import auto, nonmember
from strenum import StrEnum
import numpy as np
import pandas as pd
import logging
from functools import partial
from typing import Callable
from math import ceil
from datetime import datetime, timedelta
import pickle
import os.path
from pathlib import Path


iso_str_format = '%Y-%m-%dT%H:%M:%S.%fZ'
iso_str_format2 = '%Y-%m-%dT%H:%M:%SZ'


class BaseQueryStrEnum(StrEnum):
    def __str__(self):
        return f"${self.name}"


class Properties(StrEnum):
    description = auto()
    unitOfMeasurement = 'unitOfMeasurement/name'
    name = auto()
    iot_id = "@iot.id"
    coordinates = 'feature/coordinates'
    phenomenonTime = auto()


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
    FeatureOfInterest = auto()

    def __call__(self, args: list[Properties] | list['Qactions']):
        out = f"{self}({';'.join(list(filter(None, args)))})"
        return out

    def __repr__(self):
        return f"{self.name}"


class Qactions(BaseQueryStrEnum):
    expand = auto()
    select = auto()
    orderby = auto()

    def __call__(self, arg: Entities | Properties | list[Properties] | list[Entities]):
        out = ""
        if isinstance(arg, list):
            str_arg = ','.join(arg)
            out = f"{str(self)}={str_arg}"
        return out


class Filter(BaseQueryStrEnum):
    filter = auto()

    def __call__(self, condition: str) -> str:
        out = ''
        if condition:
            out = f"{str(self)}={condition}"
        return out


class Order(BaseQueryStrEnum):
    orderBy = auto()

    @nonmember
    class OrderOption(StrEnum):
        desc = auto()
        asc = auto()

    def __call__(self, property: BaseQueryStrEnum, option: str) -> str:
        option_ = self.OrderOption(option)
        out = f"{str(self)}={property} {option_}"
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


def filter_cfg_to_query(filter_cfg):
    filter_condition = ""
    if filter_cfg:
        range = filter_cfg.get(Properties.phenomenonTime).get("range")
        format = filter_cfg.get(Properties.phenomenonTime).get("format")

        t0, t1 = [datetime.strptime(str(ti), format) for ti in range]

        filter_condition = f"{Properties.phenomenonTime} gt {t0.strftime(iso_str_format)} and " \
                           f"{Properties.phenomenonTime} lt {t1.strftime(iso_str_format)}"
        return filter_condition


def get_results_n_datastreams(n, skip, entity_id, top_observations, filter_cfg):
    base_query = Query(Entity.Thing).entity_id(entity_id)
    out_query = base_query.select(Entities.Datastreams)
    filter_condition = filter_cfg_to_query(filter_cfg)

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
                    Filter.filter(filter_condition),
                    Settings.top(top_observations),
                    Qactions.select([
                        Properties.iot_id,
                        'result',
                        Properties.phenomenonTime,
                    ]),
                    # Qactions.expand([
                    #     Entities.FeatureOfInterest([
                    #         Qactions.select([
                    #             Properties.coordinates
                    #         ])
                    #     ])
                    # ])
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
    complete_query = out_query.get_query() + '&' + Q
    log.info("Start request")
    request = json.loads(
        Query(Entity.Thing).get_with_retry(
            complete_query
        ).content)
    log.info("End request")

    return request


def get_features_of_interest(filter_cfg, top_observations):
    filter_condition = filter_cfg_to_query(filter_cfg)
    base_query = Query(Entity.FeatureOfInterest).get_query()
    complete_query = base_query + "?" +Qactions.select([Properties.iot_id, "feature/coordinates", "Observations"])\
        + "&" + Qactions.expand([Entities.Observations([
        Qactions.select([Properties.iot_id]),
        Settings.top(top_observations)
    ])])
    complete_query += '&' + Settings.top(top_observations)
    log.info("Start request features")
    request_features = json.loads(
        Query(Entity.FeatureOfInterest).get_with_retry(
            complete_query
        ).content)
    log.info("End request features")

    df_features = features_request_to_df(request_features)
    features_observations_dict = {fi.get(Properties.iot_id):[oi.get(Properties.iot_id) for oi in fi.get(Entities.Observations)] for fi in request_features["value"]}
    # possible to write to pickle?
    # how to test if needed or not?
    return features_observations_dict


def convert_to_datetime(value):
    try:
        d_out = datetime.strptime(value, iso_str_format)
    except ValueError:
        d_out = datetime.strptime(value, iso_str_format2)
    return d_out


def features_request_to_df(request_features):
    data = []
    for fi in request_features["value"]:
        v = fi.get(Properties.iot_id)
        long, lat = fi.get("feature").get("coordinates")
        idx = [oi.get(Properties.iot_id) for oi in fi.get(Entities.Observations)]
        for idx_i in idx:
            data.append([idx_i, v, long, lat])
    df = pd.DataFrame(data, columns=["observation_id", "feature_id", "long", "lat"])
    return df


def datastreams_request_to_df(request_datastreams):
    df = pd.DataFrame()
    for di in request_datastreams:
        data_coordinates = di.get(Entities.FeatureOfInterest, {})
        if data_coordinates:
            del di[Entities.FeatureOfInterest]
        observations_list = di.get(Entities.Observations)
        if observations_list:
            df_i = pd.DataFrame(observations_list).astype({Properties.iot_id: int, "result": float})
            df_i["datastream_id"] = int(di.get(Properties.iot_id))
            df_i[Properties.phenomenonTime] = df_i[Properties.phenomenonTime].apply(convert_to_datetime)
            df_i["observation_type"] = di.get(Entities.ObservedProperty).get(Properties.name)
            df_i["observation_type"] = df_i["observation_type"].astype("category")
            k1, k2 = Properties.unitOfMeasurement.split('/', 1)
            df_i["units"] = di.get(k1).get(k2)
            df_i["units"] = df_i["units"].astype("category")
            # df_i[["long", "lat"]] = pd.DataFrame.from_records(df_i[str(Entities.FeatureOfInterest)].apply(
            #     lambda x: x.get('feature').get('coordinates')))
            # df_i.drop(columns=str(Entities.FeatureOfInterest))
            df = pd.concat([df, df_i], ignore_index=True)

    return df


def qc_on_df_per_datastream(df, datastream_id, datastream_name):
    pass


def get_datetime_latest_observation():
    query = Query(Entity.Observation).get_query() +\
            '?' + Order.orderBy(Properties.phenomenonTime, 'desc') + "&" +\
            Settings.top(1) + "&" +\
            Qactions.select([Properties.phenomenonTime])
    request = json.loads(
        Query(Entity.Observation).get_with_retry(
            query
        ).content)
    # https://sensors.naturalsciences.be/sta/v1.1/Observations?$orderBy=phenomenonTime%20desc&$top=1&$select=phenomenonTime
    latest_phenomenonTime = convert_to_datetime(request["value"][0].get(Properties.phenomenonTime))
    return latest_phenomenonTime


def features_to_global_df(features_dict: dict[int, list[int]], df: pd.DataFrame) -> pd.DataFrame:
    df_out = df.set_index(Properties.iot_id)
    i = 0
    for k, v in features_dict.items():
        log.info(f"{i}/{len(features_dict)}")
        existing_indices = df_out.index.intersection(v)
        # df_out.loc[existing_indices] = k
        i += 1
    return df_out

log = logging.getLogger(__name__)


@hydra.main(config_path="../conf", config_name="config.yaml", version_base="1.2")
def main(cfg):
    log.info("Start")
    stapy.set_sta_url(cfg.data_api.base_url)
    thing_id = cfg.data_api.things.id
    nb_streams_per_call = cfg.data_api.datastreams.top
    top_observations = cfg.data_api.observations.top
    filter_cfg = cfg.data_api.get("filter", {})

    features_file = Path(cfg.other.pickle.path)
    recreate_features_file = True
    if features_file.exists():
        latest_time = get_datetime_latest_observation()
        mod_time = datetime.fromtimestamp(os.path.getmtime(features_file))
        if mod_time > latest_time:
            recreate_features_file = False
    if recreate_features_file:
        feature_dict = get_features_of_interest(filter_cfg, top_observations)
        with open(features_file, 'wb') as f:
            pickle.dump(feature_dict, f)
    else:
        with open(features_file, 'rb') as f:
            feature_dict = pickle.load(f)

    base_query = Query(Entity.Thing).entity_id(thing_id).select("Datastreams/@iot.count")
    # summary = inspect_datastreams_thing(1)

    df_all = pd.DataFrame()
    add_query_nb = Qactions.expand([
        Entities.Datastreams(
            [Settings.count('true'),
             Qactions.select(
                 [Properties.iot_id]
             )]
        )
    ])
    nb_datastreams = json.loads(
        Query(Entity.Datastream).get_with_retry(base_query.get_query() + '&' + add_query_nb).content)\
        .get("Datastreams@iot.count")
    log.debug(f"{nb_datastreams=}")
    for i in range(ceil(nb_datastreams/nb_streams_per_call)):
        log.info(f"nb {i} of {ceil(nb_datastreams/nb_streams_per_call)}")
        df_i = datastreams_request_to_df(
            get_results_n_datastreams(n=nb_streams_per_call, skip=nb_streams_per_call * i, entity_id=thing_id,
                                      top_observations=top_observations, filter_cfg=filter_cfg)[Entities.Datastreams])
        log.debug(f"{df_i.shape[0]=}")
        df_all = pd.concat([df_all, df_i],
                           ignore_index=True)
    log.debug(f"{df_all.shape=}")
    log.debug("done with df_all")

    log.info("Start features to global df")
    df_out = features_to_global_df(feature_dict, df_all)
    log.info("End features to global df")
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
    df_all["observation_type"] = df_all["observation_type"].astype("category")
    df_all["units"] = df_all["units"].astype("category")
    print(f"{df_all.shape=}")
    log.info("End")


if __name__ == "__main__":
    log.debug("testing...")
    main()


# possibly faster?
# https://sensors.naturalsciences.be/sta/v1.1/Observations?$filter=phenomenonTime%20gt%202022-03-01T00:00:00Z%20and%20phenomenonTime%20lt%202022-04-01T00:00:00Z&$expand=FeatureOfInterest($select=feature/coordinates)&$select=FeatureOfInterest/feature/coordinates,result
# https://sensors.naturalsciences.be/sta/v1.1/Observations?$filter=phenomenonTime%20gt%202022-03-01T00:00:00Z%20and%20phenomenonTime%20lt%202022-04-01T00:00:00Z&$expand=FeatureOfInterest($select=feature/coordinates)&$select=FeatureOfInterest/feature/coordinates,result&$resultFormat=GeoJSON