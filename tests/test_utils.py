import pytest
import json
import services.requests
import utils.utils as u
from hydra import (
    initialize,
    compose,
)
import stapy
from omegaconf import DictConfig

from models.enums import Entities


@pytest.fixture(scope="session")
def cfg() -> DictConfig:
    with initialize(config_path="./conf", version_base="1.2"):
        conf = compose("conf_base.yaml")
    stapy.set_sta_url(conf.data_api.base_url)

    return conf


class MockResponse:
    def __init__(self):
        self.status_code = 200
        self.url = "testing.be"

    def json(self):
        return {"one": "two"}

    def get_data_sets(self):
        return (0, list(range(10)))


class MockResponseFull:
    def __init__(self):
        self.status_code = 200

    def json(self):
        with open("./tests/resources/test_response_wF.json", "r") as f:
            out = json.load(f)

        # if self.b:
        #     for dsi in out.get(Entities.DATASTREAMS):
        #         for bsi in dsi.get(Entities.OBSERVATIONS, {}):
        #             del bsi[Entities.FEATUREOFINTEREST]
        return out


@pytest.fixture
def mock_response(monkeypatch):
    def mock_get(*args, **kwargs):
        return MockResponse()

    def mock_get_sets(*args, **kwars):
        return MockResponse().get_data_sets()

    monkeypatch.setattr(u.Query, "get_with_retry", mock_get)
    monkeypatch.setattr(u.Query, "get_data_sets", mock_get_sets)


@pytest.fixture
def mock_response_full(monkeypatch):
    def mock_get(*args, **kwargs):
        return MockResponseFull()

    monkeypatch.setattr(u.Query, "get_with_retry", mock_get)


class TestUtils:
    def test_hydra_is_loaded(self):
        print(cfg)
        assert cfg

    def test_stapy_integration(self, cfg):
        q = u.Query(u.Entity.Thing).entity_id(0)
        assert q.get_query() == "http://testing.com/v1.1/Things(0)"
