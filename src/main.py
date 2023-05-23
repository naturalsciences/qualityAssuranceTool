import hydra
import stapy
from stapy import Query, Entity
from functools import reduce
import copy
import json
from enum import auto
from strenum import StrEnum
import numpy as np


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



def get_names(query: Query) -> list[str]:
    names = query.select("name").get_data_sets()
    return names


def inspect_datastreams_thing(entity_id: int) -> (str, list[dict[str,str | int]]):
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
    request = json.loads(
        out_query.get_with_retry(
            out_query.get_query() + '&' + additional_query
        ).content)
    print(f"{out_query.get_query() + '&' + additional_query=}")
    observ_properties, observ_count = zip(*[(ds.get(Entities.ObservedProperty).get(Properties.name), ds.get("Observations@iot.count")) for ds in request.get(Entities.Datastreams)])

    # observ_count = [ds.get("Observations@iot.count") for ds in request.get("Datastreams")]
    out = {k: request[k] for k in request.keys() if Entities.Datastreams not in k}
    out[Entities.Observations] = {
        Settings.count: sum(observ_count),
        Entities.Observations: list(set(observ_properties))
    }

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
    out[Entities.Datastreams] = reduce(update_datastreams, [{}] + request.get(Entities.Datastreams))
    return out


@hydra.main(config_path="../conf", config_name="config.yaml", version_base="1.2")
def main(cfg):
    stapy.set_sta_url(cfg.data_api.base_url)
    summary = inspect_datastreams_thing(1)

    list_out = [idi for k, dsi in summary.get(Entities.Datastreams).items() if k.split(' -- ', 1)[1] in cfg.QC for idi in dsi.get("@iot.id")]

    dict_out = {k: [] for k in cfg.QC}
    for k, dsi in summary.get(Entities.Datastreams).items():
        property_name = k.split(" -- ", 1)[1]
        if property_name in dict_out:
            dict_out[property_name] += dsi.get(Properties.iot_id)

    for prop_name, list_ids in dict_out.items():
        min_, max_ = cfg.QC.get(prop_name).get("range")
        for iot_id in list_ids:
            result_ = Query(Entity.Datastream).entity_id(iot_id).sub_entity(Entity.Observation).select("result").get_data_sets()
            flags_i = [(vi >= min_) & (vi <= max_) for vi in result_]
            if not all(flags_i):
                print(f"issue with {prop_name} stream {iot_id}")
                print(f"{np.array(result_)[~np.array(flags_i)]}")

    # test_name, test_ids = dict_out.popitem()
    # test_id = test_ids[0]
    # test_query = Query(Entity.Datastream).entity_id(test_id).sub_entity(Entity.Observation).select("result")
    # print(f"{test_query.get_query()=}")
    # test_values = test_query.get_data_sets()
    # test_min, test_max = cfg.QC.get(test_name).get("range")
    # test_flag = [(vi >= test_min) & (vi <= test_max) for vi in test_values ]
    # print(f"{test_flag=}")
    # print(f"{test_values=}")
    # print(f"{len(test_values)=}")

    # streams_id_in_QC = [ds for ds in summary.get("Datastreams") if k.split(' -- ', 2)[1] in cfg.QC]

    with open('inspect.json', 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    main()
