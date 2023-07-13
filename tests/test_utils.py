import pytest
import utils as u
from hydra import (
    initialize,
    compose,
)
import stapy
from omegaconf import DictConfig


@pytest.fixture(scope="session")
def cfg() -> DictConfig:
    initialize(config_path="./conf", version_base="1.2")
    conf = compose("conf_base.yaml")
    stapy.set_sta_url(conf.data_api.base_url)

    stapy.set_sta_url(conf.data_api.base_url)

    return conf


class TestUtils:
    def test_hydra_is_loaded(self, cfg):
        assert cfg

    def test_stapy_integration(self):
        q = u.Query(u.Entity.Thing).entity_id(0)
        assert q.get_query() == "http://testing.com/v1.1/Things(0)"

    def test_build_query_datastreams(self, cfg):
        q = u.build_query_datastreams(entity_id=cfg.data_api.things.id)
        assert (
            q == "http://testing.com/v1.1/Things(1)"
            "?$select=name,@iot.id,Datastreams"
            "&$expand=Datastreams($count=true;"
            "$expand=ObservedProperty($select=name,@iot.id),"
            "Observations($count=true;$select=@iot.id;$top=0);"
            "$select=name,@iot.id,description,unitOfMeasurement/name,ObservedProperty)"
        )
