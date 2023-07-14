import pytest
from test_utils import cfg, mock_response_full
from api.observations_api import (
    filter_cfg_to_query,
    get_results_n_datastreams_query,
    get_results_n_datastreams,
)
from models.enums import Entities

#
# class TestApi:
#     @pytest.mark.usefixtures("mock_response")
#     def test_get_id_result_lists(self, mock_response):
#         a = get_id_result_lists(0)
#         assert 1 == 1


class TestObservationsApi:
    def test_filter_cfg_to_query(self, cfg):
        out = filter_cfg_to_query(cfg.data_api.get("filter", {}))
        assert (
            out == "phenomenonTime gt 1002-01-01T00:00:00.000000Z and "
            "phenomenonTime lt 3003-01-01T00:00:00.000000Z"
        )

    def test_get_results_n_datastreams_query(self, cfg):
        cfg_api = cfg.get("data_api", {})
        entity_id = cfg_api.get("things", {}).get("id")
        n = cfg_api.get("datastreams", {}).get("top")
        top = cfg.get("data_api", {}).get("observations", {}).get("top")
        skip = 0
        filter_condition = filter_cfg_to_query(cfg.data_api.get("filter", {}))
        out = get_results_n_datastreams_query(
            entity_id=entity_id,
            n=n,
            top_observations=top,
            skip=skip,
            filter_condition=filter_condition,
            expand_feature_of_interest=False
        )

        assert (
            out
            == "http://testing.com/v1.1/Things(1)?$select=Datastreams&$expand=Datastreams("
            "$top=10;$skip=0;$select=@iot.id,unitOfMeasurement/name,Observations;"
            "$expand=Observations("
            "$filter=phenomenonTime gt 1002-01-01T00:00:00.000000Z and phenomenonTime lt 3003-01-01T00:00:00.000000Z;"
            "$top=10000;$select=@iot.id,result,phenomenonTime),"
            "ObservedProperty($select=@iot.id,name))"
        )

    def test_get_results_n_datastreams(self, mock_response_full):
        nb_datastreams = len(get_results_n_datastreams("random")[1][Entities.DATASTREAMS])
        assert nb_datastreams == 10

        
    @pytest.mark.skip(reason="no features in fixture at the moment")
    def test_features_of_interest(self):
        assert False

    @pytest.mark.skip(reason="no features in fixture at the moment & should be moved!")
    def test_features_request_to_df(self):
        assert False
