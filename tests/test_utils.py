import json

import pytest
import stapy
from hydra import compose, initialize
from omegaconf import DictConfig

import utils.utils as u


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


class MockResponseFullObs:
    def __init__(self):
        self.status_code = 200

    def json(self):
        with open("./tests/resources/test_response_obs_wF.json", "r") as f:
            out = json.load(f)

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


@pytest.fixture
def mock_response_full_obs(monkeypatch):
    def mock_get(*args, **kwargs):
        return MockResponseFullObs()

    monkeypatch.setattr(u.Query, "get_with_retry", mock_get)


class TestUtils:
    def test_hydra_is_loaded(self):
        print(cfg)
        assert cfg

    def test_stapy_integration(self, cfg):
        q = u.Query(u.Entity.Thing).entity_id(0)
        assert q.get_query() == "http://testing.com/v1.1/Things(0)"

    def test_update_response(self):
        d = {
            "one": "this",
            "two": "two",
            "three": "threeee",
            "four": "four",
            "list": list(range(5)),
        }
        update = {"one": "that", "two": "two", "list": list(range(5, 11))}
        d = u.update_response(d, update)

        ref = {
            "one": "that",
            "two": "two",
            "three": "threeee",
            "four": "four",
            "list": list(range(11)),
        }
        assert d == ref

    def test_get_velocity(self):
        assert 0

    def test_get_distance(self):
        assert 0

    def test_get_acceleration(self):
        assert 0
