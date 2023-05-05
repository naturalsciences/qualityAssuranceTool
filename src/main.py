import hydra
import stapy
from stapy import Query, Entity
from functools import reduce
import copy
import json


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
                ';$select=name,@iot.id,unitOfMeasurement/name,ObservedProperty'
                ')')
    request = json.loads(
        out_query.get_with_retry(
            out_query.get_query()
        ).content)
    observ_count = [ds.get("Observations@iot.count") for ds in request.get("Datastreams")]
    out = {k: request[k] for k in request.keys() if "Datastreams" not in k}
    out["Observations"] = {
        "count": sum(observ_count),
    }

    def update_datastreams(ds_dict, ds_new):
        ds_out = copy.deepcopy(ds_dict)
        if ds_new.get('Observations@iot.count', 12) > 0:
            update_dsi_dict = {
                "@iot.id": ds_out.get(ds_new['name'], {}).get("@iot.id", list()) + [ds_new.get("@iot.id")],
                "unitOfMeasurement": ds_out.get(ds_new['name'], {}).get("unitOfMeasurement", list()) + [ds_new.get("unitOfMeasurement").get("name")],
            }
            update_ds_dict = {ds_new.get('name'): ds_dict.get(ds_new.get('name'), copy.deepcopy(update_dsi_dict))}
            update_ds_dict[ds_new.get('name')].update(update_dsi_dict)
            ds_out.update(update_ds_dict)
        return ds_out
    out["Datastreams"] = reduce(update_datastreams, [{}] + request.get("Datastreams"))
    return out


@hydra.main(config_path="../conf", config_name="config.yaml", version_base="1.2")
def main(cfg):
    stapy.set_sta_url(cfg.data_api.base_url)
    a = inspect_datastreams_thing(1)
    print(f"{json.dumps(a, indent=4)}")


if __name__ == "__main__":
    main()
