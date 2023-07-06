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


# Type hinting often ignored
# name and COUNT are probably *known* variables names of python property
# might be solved with _name, _count, or NAME, COUNT. when all caps is used, the __str__ will need to be changed to lower
# doesn't work because other ARE with camelcase

iso_str_format = '%Y-%m-%dT%H:%M:%S.%fZ'
iso_str_format2 = '%Y-%m-%dT%H:%M:%SZ'


class BaseQueryStrEnum(StrEnum):
    def __str__(self):
        return f"${self.name}"


class Properties(StrEnum):
    DESCRIPTION = "description"
    UNITOFMEASUREMENT = 'unitOfMeasurement/name'
    NAME = "name"
    IOT_ID = "@iot.id"
    COORDINATES = 'feature/coordinates'
    PHENOMENONTIME = "phenomenonTime"


class Settings(BaseQueryStrEnum):
    TOP = "top"
    SKIP = "skip"
    COUNT = "count"

    def __call__(self, value):
        return f"{self}={str(value)}"


class Entities(StrEnum):
    DATASTREAMS = "Datastreams"
    OBSERVEDPROPERTY = "ObservedProperty"
    OBSERVATIONS = "Observations"
    FEATUREOFINTEREST = "FeatureOfInterest"

    def __call__(self, args: list[Properties] | list['Qactions'] | list[str]):
        out = f"{self}({';'.join(list(filter(None, args)))})"
        return out

    def __repr__(self):
        return f"{self.name}"


class Qactions(BaseQueryStrEnum):
    EXPAND = "expand"
    SELECT = "select"
    ORDERBY = "orderby"

    def __call__(self, arg: Entities | Properties | list[Properties] | list[Entities] | list[str]):
        out = ""
        if isinstance(arg, list):
            str_arg = ','.join(arg)
            out = f"{str(self)}={str_arg}"
        return out


class Filter(BaseQueryStrEnum):
    FILTER = "filter"

    def __call__(self, condition: str) -> str:
        out = ''
        if condition:
            out = f"{str(self)}={condition}"
        return out


class Order(BaseQueryStrEnum):
    ORDERBY = "orderBy"

    @nonmember
    class OrderOption(StrEnum):
        DESC = "desc"
        ASC = "asc"

    def __call__(self, property: BaseQueryStrEnum, option: str) -> str:
        option_ = self.OrderOption(option) # type: ignore
        out: str = f"{str(self)}={property} {option_}"
        return out


def inspect_datastreams_thing(entity_id: int) -> dict[str, list[dict[str,str | int]]]:
    log.debug(f"Start inspecting entity {entity_id}.")
    base_query = Query(Entity.Thing).entity_id(entity_id)
    out_query = base_query.select(Properties.NAME, Properties.IOT_ID, Entities.DATASTREAMS)
    additional_query = Qactions.EXPAND([
        Entities.DATASTREAMS(
            [
                Settings.COUNT('true'), # type: ignore
                Qactions.EXPAND([
                    Entities.OBSERVEDPROPERTY([Qactions.SELECT([Properties.NAME,
                                                                Properties.IOT_ID])]),
                    Entities.OBSERVATIONS([Settings.COUNT('true'),
                                           Qactions.SELECT([Properties.IOT_ID]),
                                           Settings.TOP(0)])
                ]),
                Qactions.SELECT([Properties.NAME, # type: ignore
                                 Properties.IOT_ID,
                                 Properties.DESCRIPTION,
                                 Properties.UNITOFMEASUREMENT,
                                 Entities.OBSERVEDPROPERTY]),
            ]
        )
    ])
    log.debug(f"Start getting query.")
    request = json.loads(
        out_query.get_with_retry(
            out_query.get_query() + '&' + additional_query
        ).content)
    log.debug(f"Start reformatting query.")
    observ_properties, observ_count = zip(*[(ds.get(Entities.OBSERVEDPROPERTY).get(Properties.NAME), ds.get("observations@iot.count")) for ds in request.get(Entities.DATASTREAMS)])

    # observ_count = [ds.get("OBSERVATIONS@iot.COUNT") for ds in request.get("DATASTREAMS")]
    out = {k: request[k] for k in request.keys() if Entities.DATASTREAMS not in k}
    out[Entities.OBSERVATIONS] = {
        Settings.COUNT: sum(observ_count),
        Entities.OBSERVATIONS: list(set(observ_properties))
    }

    # only_results =

    def update_datastreams(ds_dict, ds_new):
        ds_out = copy.deepcopy(ds_dict)
        if ds_new.get('observations@iot.count') > 0:
            ds_name = f"{ds_new.get(Properties.NAME)} -- {ds_new.get(Entities.OBSERVEDPROPERTY, {}).get(Properties.NAME)}"
            update_dsi_dict = {
                Properties.IOT_ID: ds_out.get(ds_new[Properties.NAME], {}).get(Properties.IOT_ID, list()) + [ds_new.get(Properties.IOT_ID)],
                "unitOfMeasurement": ds_out.get(ds_new[Properties.NAME], {}).get("unitOfMeasurement", list()) + [ds_new.get("unitOfMeasurement").get(Properties.NAME)],
                # "description": ds_out.get(ds_new['name'], {}).get("description", list()) + [
                #     ds_new.get("description")],
                # "OBSERVEDPROPERTY": ds_out.get(ds_new['name'], {}).get("OBSERVEDPROPERTY", list()) + [ds_new.get("OBSERVEDPROPERTY")],
            }
            update_ds_dict = {ds_name: ds_dict.get(ds_new.get('name'), copy.deepcopy(update_dsi_dict))}
            update_ds_dict[ds_name].update(update_dsi_dict)
            ds_out.update(update_ds_dict)
        return ds_out
    log.debug(f"Start reducing query.")
    out[Entities.DATASTREAMS] = reduce(update_datastreams, [{}] + request.get(Entities.DATASTREAMS))
    log.debug(f"Return result inspection.")
    return out


def extend_summary_with_result_inspection(summary_dict: dict[str,list]):
    log.debug(f"Start extending summary.")
    summary_out = copy.deepcopy(summary_dict)
    nb_streams = len(summary_out.get(Entities.DATASTREAMS, []))
    for i, dsi in enumerate(summary_dict.get(Entities.DATASTREAMS, [])):
        log.debug(f"Start extending datastream {i+1}/{nb_streams}.")
        iot_id_list = summary_dict.get(Entities.DATASTREAMS, []).get(dsi).get(Properties.iot_id) # type: ignore
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
        summary_out.get(Entities.DATASTREAMS).get(dsi)["results"] = extended_sumary # type: ignore
    return summary_out


def testing_patch():
    pass


def min_max_check_values(values: pd.DataFrame, min_: float, max_: float):
    out = np.logical_and(values >= min_, values <= max_)
    return out


def get_iot_id_datastreams_in_qc(dict_in: dict, summary_dict: dict):
    log.debug(f"Start loop datastreams items.")
    dict_out = copy.deepcopy(dict_in)
    for k, dsi in summary_dict.get(Entities.DATASTREAMS, {}).items():
        property_name = k.split(" -- ", 1)[1]
        if property_name in dict_out:
            dict_out[property_name] += dsi.get(Properties.IOT_ID)
    return dict_out


def get_id_result_lists(iot_id):
    id_list, result_list = Query(Entity.Datastream).entity_id(iot_id).sub_entity(Entity.Observation).select(
        Properties.IOT_ID, "result").get_data_sets()
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
    df_ = pd.DataFrame.from_dict({Properties.IOT_ID: id_list,
                                  "result": result_list}) \
        .astype({Properties.IOT_ID: int, "result": float})
    return qc_df(df_, function)


def filter_cfg_to_query(filter_cfg) -> str:
    filter_condition = ""
    if filter_cfg:
        range = filter_cfg.get(Properties.PHENOMENONTIME).get("range")
        format = filter_cfg.get(Properties.PHENOMENONTIME).get("format")

        t0, t1 = [datetime.strptime(str(ti), format) for ti in range]

        filter_condition = f"{Properties.PHENOMENONTIME} gt {t0.strftime(iso_str_format)} and " \
                           f"{Properties.PHENOMENONTIME} lt {t1.strftime(iso_str_format)}"
    return filter_condition
    

def get_results_n_datastreams(n, SKIP, entity_id, top_observations, filter_cfg):
    base_query = Query(Entity.Thing).entity_id(entity_id)
    out_query = base_query.select(Entities.DATASTREAMS)
    filter_condition = filter_cfg_to_query(filter_cfg)

    #  additional_query = Qactions.EXPAND([
    Q = Qactions.EXPAND([
        Entities.DATASTREAMS([
            Settings.TOP(n),
            Settings.SKIP(SKIP),
            Qactions.SELECT([
                Properties.IOT_ID, Properties.UNITOFMEASUREMENT, Entities.OBSERVATIONS
            ]),
            Qactions.EXPAND([
                Entities.OBSERVATIONS([
                    Filter.FILTER(filter_condition),
                    Settings.TOP(top_observations),
                    Qactions.SELECT([
                        Properties.IOT_ID,
                        'result',
                        Properties.PHENOMENONTIME,
                    ]),
                    # Qactions.EXPAND([
                    #     Entities.FEATUREOFINTEREST([
                    #         Qactions.SELECT([
                    #             Properties.COORDINATES
                    #         ])
                    #     ])
                    # ])
                ]),
                Entities.OBSERVEDPROPERTY([
                    Qactions.SELECT([
                        Properties.IOT_ID,
                        Properties.NAME # type: ignore
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
    complete_query = base_query + "?" +Qactions.SELECT([Properties.IOT_ID, "feature/coordinates", "OBSERVATIONS"])\
        + "&" + Qactions.EXPAND([Entities.OBSERVATIONS([
        Qactions.SELECT([Properties.IOT_ID]),
        Settings.TOP(top_observations)
    ])])
    complete_query += '&' + Settings.TOP(top_observations)
    log.info("Start request features")
    request_features = json.loads(
        Query(Entity.FeatureOfInterest).get_with_retry(
            complete_query
        ).content)
    log.info("End request features")

    df_features = features_request_to_df(request_features)
    features_observations_dict = {fi.get(Properties.IOT_ID):[oi.get(Properties.IOT_ID) for oi in fi.get(Entities.OBSERVATIONS)] for fi in request_features["value"]}
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
        v = fi.get(Properties.IOT_ID)
        long, lat = fi.get("feature").get("coordinates")
        idx = [oi.get(Properties.IOT_ID) for oi in fi.get(Entities.OBSERVATIONS)]
        for idx_i in idx:
            data.append([idx_i, v, long, lat])
    df = pd.DataFrame(data, columns=["observation_id", "feature_id", "long", "lat"])
    return df


def datastreams_request_to_df(request_datastreams):
    df = pd.DataFrame()
    for di in request_datastreams:
        data_coordinates = di.get(Entities.FEATUREOFINTEREST, {})
        if data_coordinates:
            del di[Entities.FEATUREOFINTEREST]
        observations_list = di.get(Entities.OBSERVATIONS)
        if observations_list:
            df_i = pd.DataFrame(observations_list).astype({Properties.IOT_ID: int, "result": float})
            df_i["datastream_id"] = int(di.get(Properties.IOT_ID))
            df_i[Properties.PHENOMENONTIME] = df_i[Properties.PHENOMENONTIME].apply(convert_to_datetime)
            df_i["observation_type"] = di.get(Entities.OBSERVEDPROPERTY).get(Properties.NAME)
            df_i["observation_type"] = df_i["observation_type"].astype("category")
            k1, k2 = Properties.UNITOFMEASUREMENT.split('/', 1)
            df_i["units"] = di.get(k1).get(k2)
            df_i["units"] = df_i["units"].astype("category")
            # df_i[["long", "lat"]] = pd.DataFrame.from_records(df_i[str(Entities.FEATUREOFINTEREST)].apply(
            #     lambda x: x.get('feature').get('coordinates')))
            # df_i.drop(columns=str(Entities.FEATUREOFINTEREST))
            df = pd.concat([df, df_i], ignore_index=True)

    return df


def qc_on_df_per_datastream(df, datastream_id, datastream_name):
    pass


def get_datetime_latest_observation():
    query = Query(Entity.Observation).get_query() +\
            '?' + Order.ORDERBY(Properties.PHENOMENONTIME, 'desc') + "&" +\
            Settings.TOP(1) + "&" +\
            Qactions.SELECT([Properties.PHENOMENONTIME]) # type:ignore
    request = json.loads(
        Query(Entity.Observation).get_with_retry(
            query
        ).content)
    # https://sensors.naturalsciences.be/sta/v1.1/OBSERVATIONS?$ORDERBY=phenomenonTime%20desc&$TOP=1&$SELECT=phenomenonTime
    latest_phenomenonTime = convert_to_datetime(request["value"][0].get(Properties.PHENOMENONTIME))
    return latest_phenomenonTime


def features_to_global_df(features_dict: dict[int, list[int]], df: pd.DataFrame) -> pd.DataFrame:
    df_out = df.set_index(Properties.IOT_ID)
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
    filter_cfg = cfg.data_api.get("FILTER", {})

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
    add_query_nb = Qactions.EXPAND([
        Entities.DATASTREAMS(
            [Settings.COUNT('true'),
             Qactions.SELECT(
                 [Properties.IOT_ID]
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
            get_results_n_datastreams(n=nb_streams_per_call, SKIP=nb_streams_per_call * i, entity_id=thing_id,
                                      top_observations=top_observations, filter_cfg=filter_cfg)[Entities.DATASTREAMS])
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
# https://sensors.naturalsciences.be/sta/v1.1/OBSERVATIONS?$FILTER=phenomenonTime%20gt%202022-03-01T00:00:00Z%20and%20phenomenonTime%20lt%202022-04-01T00:00:00Z&$EXPAND=FEATUREOFINTEREST($SELECT=feature/coordinates)&$SELECT=FEATUREOFINTEREST/feature/coordinates,result
# https://sensors.naturalsciences.be/sta/v1.1/OBSERVATIONS?$FILTER=phenomenonTime%20gt%202022-03-01T00:00:00Z%20and%20phenomenonTime%20lt%202022-04-01T00:00:00Z&$EXPAND=FEATUREOFINTEREST($SELECT=feature/coordinates)&$SELECT=FEATUREOFINTEREST/feature/coordinates,result&$resultFormat=GeoJSON