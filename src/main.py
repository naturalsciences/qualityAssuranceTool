import hydra
import stapy
from stapy import Query, Entity
import json


@hydra.main(config_path="../conf", config_name="config.yaml", version_base="1.2")
def main(cfg):
    stapy.set_sta_url(cfg.data_api.base_url)
    # id, desc = Query(Entity.Datastream).select("@iot.id", "description").filter(
    #     "substringof('humidity', tolower(description))").get_data_sets()
    # test = Query(Entity.Datastream)
    # test = test.sub_entity(Entity.Observation).entity_id(id).select("@iot.id")
    # print(test.get_query())
    # # for t in things:
    # #     print(f"{t=}")

    t_expand = Query(Entity.Thing).expand("Datastreams")
    ds = t_expand.get_data_sets(query=t_expand.select("@iot.id").get_query())
    print(f"{ds=}")
    a = t_expand.get_with_retry(t_expand.get_query())
    data = json.loads(a.content)
    payload = data.get("value")

    print(f"{type(data)=}")
    print(f"{type(payload)=}")
    print(f"{len(payload)=}")
    for pi in payload:
        print(f"{pi.keys()=}")
        print(f"{pi.get('@iot.id')=} : {pi.get('name', '')=}")
        print(f"{len(pi.get('Datastreams'))=}")
    # print(f"{a=}")

    d_expand_obs = Query(Entity.Datastream).entity_id(5).expand(Entity.Observation)
    payload = json.loads(d_expand_obs.get_with_retry(d_expand_obs.get_query()).content).get("value)")

    # print(f"{len(payload)}")

    # t_expand.expand()
    # print(f"{t_expand=}")


if __name__ == "__main__":
    main()
