import json

import pandas as pd
import pandas.testing as pdt
import pytest
from pandas.api import types
from test_utils import (cfg, mock_response, mock_response_full,
                        mock_response_full_obs)

from services.pandasta.sta import Entities, Properties
from services.pandasta.df import Df, response_obs_to_df, response_single_datastream_to_df
from services.qualityassurancetool.qualityflags import QualityFlags
from services.qualityassurancetool.qualityflags import CAT_TYPE
from services.pandasta.requests import (Query, get_request, get_results_n_datastreams,
                               response_datastreams_to_df)


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
        response_in = get_results_n_datastreams(Query(base_url="test.be", root_entity=Entities.DATASTREAMS))[1]
        datastreams_data = response_in[Entities.DATASTREAMS]
        df = response_single_datastream_to_df(datastreams_data[1])
        assert not Entities.FEATUREOFINTEREST in df.keys()

    # incomplete!
    def test_shape_observations_request_to_df(self, mock_response_full_obs):
        response_in = get_request(Query(base_url="test.be", root_entity=Entities.DATASTREAMS))[1]
        df = response_obs_to_df(response_in)
        assert df.shape == (10, 6)

    # incomplete comparison
    # @pytest.mark.skip(reason="fails  AND not used")
    def test_shape_datastreams_request_to_df(self, mock_response_full):
        response_in = get_results_n_datastreams(Query(base_url="test.be", root_entity=Entities.DATASTREAMS))[1]
        df = response_datastreams_to_df(response_in)
        assert df.shape == (27, 11)

    @pytest.mark.skip(reason="fails after including qc flag")
    def test_num_dtypes_datastreams_request_to_df(self, mock_response_full):
        response_in = get_results_n_datastreams(Query(base_url="test.be", root_entity=Entities.DATASTREAMS))[1]
        datastreams_data = response_in[Entities.DATASTREAMS]
        df = response_single_datastream_to_df(datastreams_data[1])

        assert all(
            types.is_numeric_dtype(df[ci])
            for ci in [Properties.IOT_ID, Df.RESULT, Df.DATASTREAM_ID, Df.LONG, Df.LAT]
        )
