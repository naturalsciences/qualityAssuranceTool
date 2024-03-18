import json

import pytest
# from numpy.testing import
import services.pandasta.requests
from test_config import cfg # should not use this

from services.pandasta.requests import (Entity, Query, build_query_datastreams,
                                        get_absolute_path_to_base,
                                        get_nb_datastreams_of_thing,
                                        get_observations_count_thing_query,
                                        get_request, get_results_n_datastreams,
                                        get_results_n_datastreams_query,
                                        response_datastreams_to_df,
                                        update_response)
from services.pandasta.sta import (Entities, Filter, Properties, Qactions,
                                   Settings)
from services.qualityassurancetool.config import QCconf, filter_cfg_to_query

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

    monkeypatch.setattr(services.pandasta.requests.Query, "get_with_retry", mock_get)
    # monkeypatch.setattr(u.Query, "get_data_sets", mock_get_sets)


@pytest.fixture
def mock_response_full(monkeypatch):
    def mock_get(*args, **kwargs):
        return MockResponseFull()

    monkeypatch.setattr(services.pandasta.requests.Query, "get_with_retry", mock_get)


@pytest.fixture
def mock_response_full_obs(monkeypatch):
    def mock_get(*args, **kwargs):
        return MockResponseFullObs()

    monkeypatch.setattr(services.pandasta.requests.Query, "get_with_retry", mock_get)


class TestServicesRequests:
    def test_get_observations_count_thing_query(self, cfg: QCconf):
        q = get_observations_count_thing_query(
            entity_id=cfg.data_api.things.id,
            filter_condition=f"{Properties.PHENOMENONTIME} gt 2023-01-02",
            skip_n=2,
        )
        assert (
            q.build() == "http://testing.com/v1.1/Things(1)"
            "?$select=Datastreams"
            "&$expand=Datastreams($skip=2;"
            "$expand=Observations($filter=phenomenonTime gt 2023-01-02;$count=true);"
            "$select=Observations/@iot.count)"
        )

    def test_build_query_datastreams(self, cfg: QCconf):
        q = build_query_datastreams(entity_id=cfg.data_api.things.id)
        assert (
            q == "http://testing.com/v1.1/Things(1)"
            "?$select=name,@iot.id,Datastreams"
            "&$expand=Datastreams($count=true;"
            "$expand=ObservedProperty($select=name,@iot.id),"
            "Observations($count=true;$top=0;$select=@iot.id);"
            "$select=name,@iot.id,description,unitOfMeasurement/name,ObservedProperty)"
        )

    def test_get_request(self, mock_response):
        status_code, response = get_request(
            Query(base_url="test.be", root_entity=Entities.DATASTREAMS)
        )
        assert (status_code, response) == (200, {"one": "two"})

    # @pytest.mark.skip(reason="What response to provide?")
    # def test_inspect_datastreams_thing(self, mock_response):
    #     out = u.inspect_datastreams_thing(0)

    def test_get_request_full(self, mock_response_full):
        status_code, response = get_request(
            Query(base_url="test.be", root_entity=Entities.DATASTREAMS)
        )
        with open("./tests/resources/test_response_wF.json") as f:
            ref = json.load(f)
        assert (status_code, response) == (200, ref)

    def test_get_request_full_2(self, mock_response_full):
        status_code, response = get_request(
            Query(base_url="test.be", root_entity=Entities.DATASTREAMS)
        )
        assert (
            Entities.FEATUREOFINTEREST
            in response[Entities.DATASTREAMS][1][Entities.OBSERVATIONS][0].keys()
        )

    @pytest.mark.skip()
    def test_get_request_full_3(self, mock_response_full):
        status_code, response = get_request(
            Query(base_url="test.be", root_entity=Entities.DATASTREAMS)
        )
        assert 0

    def test_get_results_n_datastreams_query(self, cfg):
        cfg_api = cfg.get("data_api", {})
        entity_id = cfg_api.get("things", {}).get("id")
        n = cfg_api.get("datastreams", {}).get("top")
        top = cfg.get("data_api", {}).get("observations", {}).get("top")
        skip = 0
        filter_condition = filter_cfg_to_query(cfg.data_api.get("filter", {}))
        out = get_results_n_datastreams_query(
            entity_id=entity_id,
            filter_condition=filter_condition,
            expand_feature_of_interest=False,
        )

        assert (
            out.build()
            == "http://testing.com/v1.1/Things(1)?$select=Datastreams&$expand=Datastreams("
            "$expand=Observations("
            "$filter=phenomenonTime gt 1002-01-01T00:00:00.000000Z and phenomenonTime lt 3003-01-01T00:00:00.000000Z;"
            "$select=@iot.id,result,phenomenonTime,resultQuality),"
            "ObservedProperty($select=@iot.id,name);"
            "$select=@iot.id,unitOfMeasurement/name,Observations"
            ")"
        )

    def test_get_results_n_datastreams_query_none(self, cfg):
        cfg_api = cfg.get("data_api", {})
        entity_id = cfg_api.get("things", {}).get("id")
        filter_condition = filter_cfg_to_query(cfg.data_api.get("filter", {}))
        out = get_results_n_datastreams_query(
            entity_id=entity_id,
            filter_condition=filter_condition,
            expand_feature_of_interest=False,
        )

        assert (
            out.build()
            == "http://testing.com/v1.1/Things(1)?$select=Datastreams&$expand=Datastreams("
            "$expand=Observations("
            "$filter=phenomenonTime gt 1002-01-01T00:00:00.000000Z and phenomenonTime lt 3003-01-01T00:00:00.000000Z;"
            "$select=@iot.id,result,phenomenonTime,resultQuality),"
            "ObservedProperty($select=@iot.id,name);"
            "$select=@iot.id,unitOfMeasurement/name,Observations)"
        )

    def test_get_results_n_datastreams(self, mock_response_full):
        nb_datastreams = len(
            get_results_n_datastreams(
                Query(base_url="test.be", root_entity=Entities.DATASTREAMS)
            )[1][Entities.DATASTREAMS]
        )
        assert nb_datastreams == 10

    def test_get_nb_datastreams_of_thing(self, mock_response_full):
        nb_datastreams = get_nb_datastreams_of_thing(1)
        assert nb_datastreams == 125

    @pytest.mark.skip(reason="no features in fixture at the moment")
    def test_features_of_interest(self):
        assert False

    # @pytest.mark.skip(reason="fails after including qc flag")
    def test_response_datastreams_to_df_nextLink_datastreams_warning(self, caplog):
        with open("./tests/resources/response_with_nextlink.json", "r") as f:
            res = json.load(f)
        df = response_datastreams_to_df(res)

        assert "Not all observations are extracted!" in caplog.text

    def test_absolute_path_to_base_exists(self):
        out = get_absolute_path_to_base()
        assert out.exists()

    def test_update_response(self):
        d = {
            "one": "this",
            "two": "two",
            "three": "threeee",
            "four": "four",
            "list": list(range(5)),
        }
        update = {"one": "that", "two": "two", "list": list(range(5, 11))}
        d = update_response(d, update)

        ref = {
            "one": "that",
            "two": "two",
            "three": "threeee",
            "four": "four",
            "list": list(range(11)),
        }
        assert d == ref

    def test_class_creation_base_query(self):
        thing_1 = Entity(Entities.THINGS)

        thing_1.id = 5

        q_out1 = Query(base_url="http://testing.be", root_entity=thing_1)
        q1 = q_out1.build()
        assert q1 == "http://testing.be/Things(5)"

    def test_class_creation_select(self):
        thing_1 = Entity(Entities.THINGS)
        thing_1.id = 5

        thing_1.selection = [Entities.DATASTREAMS, Properties.DESCRIPTION]
        thing_1.expand = [Entities.OBSERVATIONS]

        assert (
            Qactions.SELECT(Query.selection_to_list(thing_1))
            == "$select=Datastreams,description"
        )

    def test_class_creation_settings(self):
        ds0 = Entity(Entities.DATASTREAMS)
        ds0.settings = [Settings.SKIP(2)]
        assert Query.settings_to_list(ds0) == ["$skip=2"]

    def test_class_creation_filter(self):
        obs0 = Entity(Entities.OBSERVATIONS)
        obs0.filter = "result gt 0.6"
        assert Filter.FILTER(Query.filter_to_str(obs0)) == "$filter=result gt 0.6"
        obs0.filter = "phenomenonTime gt 2023-01-02"
        assert (
            Filter.FILTER(Query.filter_to_str(obs0))
            == "$filter=result gt 0.6 and phenomenonTime gt 2023-01-02"
        )

    def test_class_creation_expand(self):

        thing_1 = Entity(Entities.THINGS)
        thing_1.id = 5
        thing_1.expand = [Entities.OBSERVATIONS]
        assert Qactions.EXPAND(Query.expand_to_list(thing_1)) == "$expand=Observations"

    def test_class_creation_expand_nested(self):
        obs0 = Entity(Entities.OBSERVATIONS)
        obs0.filter = "result gt 0.6"
        obs0.filter = "phenomenonTime gt 2023-01-02"

        thing_1 = Entity(Entities.THINGS)
        thing_1.id = 5

        thing_1.expand = [obs0]
        assert (
            Qactions.EXPAND(Query.expand_to_list(thing_1))
            == "$expand=Observations($filter=result gt 0.6 and phenomenonTime gt 2023-01-02)"
        )
