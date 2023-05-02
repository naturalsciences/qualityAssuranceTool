import hydra
import stapy
from stapy import Query, Entity
import json


@hydra.main(config_path="../conf", config_name="config.yaml", version_base="1.2")
def main(cfg):
    stapy.set_sta_url(cfg.data_api.base_url)
    id, desc = Query(Entity.Datastream).select("@iot.id", "description").filter(
        "substringof('humidity', tolower(description))").get_data_sets()
    test = Query(Entity.Datastream)
    test = test.sub_entity(Entity.Observation).entity_id(id).select("@iot.id")
    print(test.get_query())
    # for t in things:
    #     print(f"{t=}")


if __name__ == "__main__":
    main()
