import hydra
import stapy
from stapy import Query, Entity
from functools import reduce
import copy
import json
import numpy as np


def get_names(query: Query) -> list[str]:
    names = query.select("name").get_data_sets()
    return names


def inspect_datastreams_thing(entity_id: int) -> (str, list[dict[str,str | int]]):
    base_query = Query(Entity.Thing).entity_id(entity_id)
    out_query = base_query.select('name', '@iot.id', 'Datastreams') \
        .expand('Datastreams'
                '('
                '$count=true'
                ';$expand=ObservedProperty'
                '('
                '$select=name,@iot.id'
                ')'
                ',Observations'
                '('
                '$count=true'
                ';$select=@iot.id'
                ';$top=0'
                ')'
                ';$select=name,@iot.id,description,unitOfMeasurement/name,ObservedProperty'
                ')')
    request = json.loads(
        out_query.get_with_retry(
            out_query.get_query()
        ).content)
    observ_properties, observ_count = zip(*[(ds.get("ObservedProperty").get("name"), ds.get("Observations@iot.count")) for ds in request.get("Datastreams")])
    # observ_count = [ds.get("Observations@iot.count") for ds in request.get("Datastreams")]
    out = {k: request[k] for k in request.keys() if "Datastreams" not in k}
    out["Observations"] = {
        "count": sum(observ_count),
        "properties": list(set(observ_properties))
    }

    def update_datastreams(ds_dict, ds_new):
        ds_out = copy.deepcopy(ds_dict)
        if ds_new.get('Observations@iot.count') > 0:
            ds_name = f"{ds_new.get('name')} -- {ds_new.get('ObservedProperty', {}).get('name')}"
            update_dsi_dict = {
                "@iot.id": ds_out.get(ds_new['name'], {}).get("@iot.id", list()) + [ds_new.get("@iot.id")],
                "unitOfMeasurement": ds_out.get(ds_new['name'], {}).get("unitOfMeasurement", list()) + [ds_new.get("unitOfMeasurement").get("name")],
                # "description": ds_out.get(ds_new['name'], {}).get("description", list()) + [
                #     ds_new.get("description")],
                # "ObservedProperty": ds_out.get(ds_new['name'], {}).get("ObservedProperty", list()) + [ds_new.get("ObservedProperty")],
            }
            update_ds_dict = {ds_name: ds_dict.get(ds_new.get('name'), copy.deepcopy(update_dsi_dict))}
            update_ds_dict[ds_name].update(update_dsi_dict)
            ds_out.update(update_ds_dict)
        return ds_out
    out["Datastreams"] = reduce(update_datastreams, [{}] + request.get("Datastreams"))
    return out


@hydra.main(config_path="../conf", config_name="config.yaml", version_base="1.2")
def main(cfg):
    stapy.set_sta_url(cfg.data_api.base_url)
    summary = inspect_datastreams_thing(1)

    list_out = [idi for k, dsi in summary.get('Datastreams').items() if k.split(' -- ', 1)[1] in cfg.QC for idi in dsi.get("@iot.id")]

    dict_out = {k: [] for k in cfg.QC}
    for k, dsi in summary.get('Datastreams').items():
        property_name = k.split(" -- ", 1)[1]
        if property_name in dict_out:
            dict_out[property_name] += dsi.get('@iot.id')

    for prop_name, list_ids in dict_out.items():
        min_, max_ = cfg.QC.get(prop_name).get("range")
        for iot_id in list_ids:
            result_ = Query(Entity.Datastream).entity_id(iot_id).sub_entity(Entity.Observation).select("result").get_data_sets()
            flags_i = [(vi >= min_) & (vi <= max_) for vi in result_]
            if not all(flags_i):
                print(f"issue with {prop_name} stream {iot_id}")
                # print(f"{np.array(result_)[~np.array(flags_i)]}")

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
