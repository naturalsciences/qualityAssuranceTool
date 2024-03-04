import json

import pandas as pd
import pandas.testing as pdt
import pytest
from pandas.api import types

# from numpy.testing import
from test_utils import cfg, mock_response, mock_response_full, mock_response_full_obs

from models.enums import Df, Entities, Properties, QualityFlags
from services.config import QCconf, filter_cfg_to_query
from services.df import response_obs_to_df, response_single_datastream_to_df
from services.qc import CAT_TYPE
from services.requests import (
    build_query_datastreams,
    build_query_observations,
    get_nb_datastreams_of_thing,
    get_observations_count_thing_query,
    get_request,
    get_results_n_datastreams,
    get_results_n_datastreams_query,
    response_datastreams_to_df,
)

# from services.df import response_datastreams_to_df
#
# class TestApi:
#     @pytest.mark.usefixtures("mock_response")
#     def test_get_id_result_lists(self, mock_response):
#         a = get_id_result_lists(0)
#         assert 1 == 1


class TestServicesConfig:
    def test_filter_cfg_to_query(self, cfg: QCconf):
        out = filter_cfg_to_query(cfg.data_api.filter)
        assert (
            out == "phenomenonTime gt 1002-01-01T00:00:00.000000Z and "
            "phenomenonTime lt 3003-01-01T00:00:00.000000Z"
        )


class TestServicesRequests:
    def test_get_observations_count_thing_query(self, cfg: QCconf):
        q = get_observations_count_thing_query(
            entity_id=cfg.data_api.things.id,
            filter_condition=f"{Df.TIME} gt 2023-01-02",
            skip_n=2,
        )
        assert (
            q == "http://testing.com/v1.1/Things(1)"
            "?$select=Datastreams"
            "&$expand=Datastreams($skip=2;"
            "$expand=Observations($filter=phenomenonTime gt 2023-01-02;$count=True);"
            "$select=Observations/@iot.count)"
        )

    def test_build_query_datastreams(self, cfg: QCconf):
        q = build_query_datastreams(entity_id=cfg.data_api.things.id)
        assert (
            q == "http://testing.com/v1.1/Things(1)"
            "?$select=name,@iot.id,Datastreams"
            "&$expand=Datastreams($count=true;"
            "$expand=ObservedProperty($select=name,@iot.id),"
            "Observations($count=true;$select=@iot.id;$top=0);"
            "$select=name,@iot.id,description,unitOfMeasurement/name,ObservedProperty)"
        )

    def test_get_request(self, mock_response):
        status_code, response = get_request("random")
        assert (status_code, response) == (200, {"one": "two"})

    # @pytest.mark.skip(reason="What response to provide?")
    # def test_inspect_datastreams_thing(self, mock_response):
    #     out = u.inspect_datastreams_thing(0)

    def test_get_request_full(self, mock_response_full):
        status_code, response = get_request("random")
        with open("./tests/resources/test_response_wF.json") as f:
            ref = json.load(f)
        assert (status_code, response) == (200, ref)

    def test_get_request_full_2(self, mock_response_full):
        status_code, response = get_request("random")
        assert (
            Entities.FEATUREOFINTEREST
            in response[Entities.DATASTREAMS][1][Entities.OBSERVATIONS][0].keys()
        )

    @pytest.mark.skip()
    def test_get_request_full_3(self, mock_response_full):
        status_code, response = get_request("random")
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
            out
            == "http://testing.com/v1.1/Things(1)?$select=Datastreams&$expand=Datastreams("
            "$select=@iot.id,unitOfMeasurement/name,Observations;"
            "$expand=Observations("
            "$filter=phenomenonTime gt 1002-01-01T00:00:00.000000Z and phenomenonTime lt 3003-01-01T00:00:00.000000Z;"
            "$select=@iot.id,result,phenomenonTime,resultQuality),"
            "ObservedProperty($select=@iot.id,name))"
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
            out
            == "http://testing.com/v1.1/Things(1)?$select=Datastreams&$expand=Datastreams("
            "$select=@iot.id,unitOfMeasurement/name,Observations;"
            "$expand=Observations("
            "$filter=phenomenonTime gt 1002-01-01T00:00:00.000000Z and phenomenonTime lt 3003-01-01T00:00:00.000000Z;"
            "$select=@iot.id,result,phenomenonTime,resultQuality),"
            "ObservedProperty($select=@iot.id,name))"
        )

    def test_get_results_n_datastreams(self, mock_response_full):
        nb_datastreams = len(
            get_results_n_datastreams("random")[1][Entities.DATASTREAMS]
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


class TestDf:
    @pytest.mark.skip(reason="no features in fixture at the moment & should be moved!")
    def test_features_request_to_df(self):
        assert False

    def test_response_single_datastream_to_df_qc_flag(self):
        with open(
            "./tests/resources/single_datastream_response_missing_resultquality.json"
        ) as f:
            res = json.load(f)
        df = response_single_datastream_to_df(res)
        assert not df.isnull().any().any()
        pdt.assert_series_equal(
            df[Df.QC_FLAG],
            pd.Series([2, 0, 2, 2, 0]).apply(QualityFlags).astype(CAT_TYPE),  # type: ignore
            check_names=False,
        )

    # @pytest.mark.skip(reason="fails after including qc flag in df, missing from json")
    def test_features_datastreams_request_to_df(self, mock_response_full):
        response_in = get_results_n_datastreams("random")[1]
        datastreams_data = response_in[Entities.DATASTREAMS]
        df = response_single_datastream_to_df(datastreams_data[1])
        assert not Entities.FEATUREOFINTEREST in df.keys()

    # incomplete!
    def test_shape_observations_request_to_df(self, mock_response_full_obs):
        response_in = get_request("random")[1]
        df = response_obs_to_df(response_in)
        assert df.shape == (10, 6)

    # incomplete comparison
    # @pytest.mark.skip(reason="fails  AND not used")
    def test_shape_datastreams_request_to_df(self, mock_response_full):
        response_in = get_results_n_datastreams("random")[1]
        df = response_datastreams_to_df(response_in)
        assert df.shape == (27, 11)

    @pytest.mark.skip(reason="fails after including qc flag")
    def test_num_dtypes_datastreams_request_to_df(self, mock_response_full):
        response_in = get_results_n_datastreams("random")[1]
        datastreams_data = response_in[Entities.DATASTREAMS]
        df = response_single_datastream_to_df(datastreams_data[1])

        assert all(
            types.is_numeric_dtype(df[ci])
            for ci in [Properties.IOT_ID, Df.RESULT, Df.DATASTREAM_ID, Df.LONG, Df.LAT]
        )
