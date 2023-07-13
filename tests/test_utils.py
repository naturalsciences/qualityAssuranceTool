import pytest
import utils.utils as u
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

    return conf


class MockResponse():
    def __init__(self):
        self.status_code = 200
        self.url = "testing.be"

    def json(self):
        return {"one": "two"}

    def get_data_sets(self):
        return (0, list(range(10)))

@pytest.fixture
def mock_response(monkeypatch):

    def mock_get(*args, **kwargs):
        return MockResponse()

    def mock_get_sets(*args, **kwars):
        return MockResponse().get_data_sets()

    monkeypatch.setattr(u.Query, "get_with_retry", mock_get)
    monkeypatch.setattr(u.Query, "get_data_sets", mock_get_sets)

class TestUtils:
    def test_hydra_is_loaded(self):
        print(cfg)
        assert cfg

    def test_stapy_integration(self, cfg):
        q = u.Query(u.Entity.Thing).entity_id(0)
        assert q.get_query() == "http://testing.com/v1.1/Things(0)"

    def test_build_query_datastreams(self):
        q = u.build_query_datastreams(entity_id=cfg.data_api.things.id)
        assert (
            q == "http://testing.com/v1.1/Things(1)"
            "?$select=name,@iot.id,Datastreams"
            "&$expand=Datastreams($count=true;"
            "$expand=ObservedProperty($select=name,@iot.id),"
            "Observations($count=true;$select=@iot.id;$top=0);"
            "$select=name,@iot.id,description,unitOfMeasurement/name,ObservedProperty)"
        )

    def test_get_request(self, mock_response):
        status_code, response = u.get_request("random")
        assert (status_code, response) == (200, {"one": "two"})

    @pytest.mark.skip(reason="What response to provide?")
    def test_inspect_datastreams_thing(self, mock_response):
        out = u.inspect_datastreams_thing(0)
