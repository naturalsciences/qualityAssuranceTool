import pytest
from test_utils import MockResponse, mock_response, cfg
from api.observations_api import filter_cfg_to_query

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
