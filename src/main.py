import hydra
import stapy
from stapy import Query, Entity
import json


def get_names(query: Query) -> list[str]:
    names = query.select("name").get_data_sets()
    return names


def inspect_datastreams_thing(entity_id: int) -> (str, list[dict[str,str | int]]):
    """
    name of the thing
        observationProperty
            datastreams
    """
    # examples
    # https://sensors.naturalsciences.be/sta/v1.1/
    # Things(1)?$select=name,@iot.id&$expand=Datastreams ($expand=ObservedProperty ($select=name,@iot.id);$select=name,@iot.id)
    # https://sensors.naturalsciences.be/sta/v1.1/Things(1)?$select=name,@iot.id&$expand=Datastreams%20($select=unitOfMeasurement/name)
    # https://sensors.naturalsciences.be/sta/v1.1/Things(1)?$expand=Datastreams%20($count=true)&$select=Datastreams/@iot.count

    out = dict()
    base_query = Query(Entity.Thing).entity_id(entity_id)
    # out = base_query.select('name', 'Datastreams').expand('Datastreams'
    #                                                       '($expand=ObservedProperty'
    #                                                       '($select=name,@iot.id);$select=name,@iot.id,unitOfMeasurement/name'
    #                                                       ')').get_data_sets()
    # print(json.dumps(out, indent=4))
    # working = "https://sensors.naturalsciences.be/sta/v1.1/Things(1)?$select=name,@iot.id,Datastreams&$expand=Datastreams%20($expand=ObservedProperty%20($select=name,@iot.id);$select=name,@iot.id)"
    # print(f"{working}")
    # a = base_query.select('name', '@iot.id').expand('Datastreams ($expand=ObservedProperty ($select=@iot.id);$select=name,@iot.id)')
    # out = base_query.select('name', 'Datastreams').expand('Datastreams ($expand=ObservedProperty ($select=@iot.id);$select=name,@iot.id)'.get_data_sets()
    # all shizzle
    # out = base_query.select('name', 'Datastreams').expand('Datastreams'
    #                                                       '($expand=Observations ($count=true)'
    #                                                       ';$expand=ObservedProperty'
    #                                                       '($select=name,@iot.id);$select=name,@iot.id,unitOfMeasurement/name'
    #                                                       ')').get_data_sets()

    # new all shizzle
    # out_query = base_query.select('name', 'Datastreams')\
    #     .expand('Datastreams'
    #             '('
    #             '$count=true;'
    #             '$top=1;'
    #             '$expand=ObservedProperty'
    #                 '('
    #                 '$select=name,@iot.id'
    #                 ');'
    #             '$select=name,@iot.id,unitOfMeasurement/name'
    #              ')')
    out_query = base_query.select('name', 'Datastreams') \
        .expand('Datastreams'
                '('
                '$count=true'
                ';$expand=ObservedProperty'
                '('
                '$select=name,@iot.id'
                ')'
                ';$select=name,@iot.id,unitOfMeasurement/name'
                ';$expand=Observations'
                '('
                '$count=true'
                ';$select=@iot.id'
                ';$top=0'
                ')'
                ')')
    # data = out_query.get_data_sets()

    request = json.loads(
        out_query.get_with_retry(
            out_query.get_query()
        ).content)
    observ_count = [ds.get("Observations@iot.count") for ds in request.get("Datastreams")]
    out["Observations"] = {"count": sum(observ_count)}
    # out["Units"] = {"unique": list(set([ds.get("unitOfMeasurement").get("name") for ds in request.get("Datastreams")]))}
    # out["Streams"] = {"names": dict.fromkeys(list(set([ds.get("name") for ds in request.get("Datastreams")])), list())}
    out.update({"Streams": dict.fromkeys(list(set([ds.get("name") for ds in request.get("Datastreams")])), dict.fromkeys(["Units"]))})
    print(f"{json.dumps(out, indent=4)}")
    # print(f"{request=}")
    # print(f"{request.get('Datastreams@iot.count')=}")
    # print(json.dumps(out, indent=4))
    # print(f"{base_query.get_query()}")
    return out


@hydra.main(config_path="../conf", config_name="config.yaml", version_base="1.2")
def main(cfg):
    stapy.set_sta_url(cfg.data_api.base_url)
    a = inspect_datastreams_thing(1)

    # # id, desc = Query(Entity.Datastream).select("@iot.id", "description").filter(
    # #     "substringof('humidity', tolower(description))").get_data_sets()
    # # test = Query(Entity.Datastream)
    # # test = test.sub_entity(Entity.Observation).entity_id(id).select("@iot.id")
    # # print(test.get_query())
    # # # for t in things:
    # # #     print(f"{t=}")

    # t_expand = Query(Entity.Thing).entity_id(1)
    # print(f"{t_expand.get_query()=}")
    # names_datastreams = get_names(t_expand.sub_entity(Entity.Datastream))
    # print(f"{t_expand.sub_entity(Entity.Datastream).get_query()=}")
    # q_observedProperties = t_expand.sub_entity(Entity.Datastream).select("ObservedProperty@iot.navigationLink")
    # print(f"{q_observedProperties.get_query()=}")



    # a0 = Query(Entity.Thing).entity_id(1).expand(Entity.Datastream.value)
    # a1 = a0.sub_entity(Entity.Datastream)
    # a2 = a1.expand(Entity.ObservedProperty.value)
    # print(f"{a0.get_query()}")
    # print(f"{a1.get_query()}")
    # print(f"{a2.get_query()}")
    # # a = Query(Entity.Thing).entity_id(1).expand(Entity.Datastream.value).expand(Entity.ObservedProperty.value).select("ObservedProptery")

    # # print(f"{a.get_query()=}")
    # # print(f"{q_observedProperties.get_query()=}")
    # # print(f"{t_expand.get_query()=}")
    # print(f"{len(names_datastreams)=}")
    # print(f"{len(set(names_datastreams))=}")
    # # print(f"{names_observedProperties=}")
    # # print(f"{len(names_observedProperties)=}")
    # # print(f"{len(set(names_observedProperties))=}")
    # ## ds = t_expand.get_data_sets(query=t_expand.select("@iot.id").get_query())
    # ## print(f"{ds=}")
    # ## a = t_expand.get_with_retry(t_expand.get_query())
    # ## data = json.loads(a.content)
    # ## payload = data.get("value")

    # ## print(f"{type(data)=}")
    # ## print(f"{type(payload)=}")
    # ## print(f"{len(payload)=}")
    # ## for pi in payload:
    # ##     print(f"{pi.keys()=}")
    # ##     print(f"{pi.get('@iot.id')=} : {pi.get('name', '')=}")
    # ##     print(f"{len(pi.get('Datastreams'))=}")
    # ## # print(f"{a=}")

    # ## d_expand_obs = Query(Entity.Datastream).entity_id(5).expand(Entity.Observation)
    # ## payload = ery()).content).get("value)")

    # ## # print(f"{len(payload)}")

    # ## # t_expand.expand()
    # ## # print(f"{t_expand=}")


if __name__ == "__main__":
    main()
