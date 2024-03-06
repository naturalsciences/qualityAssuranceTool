import json

import pandas as pd
import pytest
from aums_data_request import get_unique_value_series

from models.enums import Df, Entities, Properties


@pytest.fixture(scope="session")
def response_fix() -> dict:
    with open("./tests/resources/aums_data_request_response.json") as f:
        response_out = json.load(f)
    return response_out

@pytest.fixture(scope="session")
def df_fix() -> pd.DataFrame:
    df_out = pd.read_csv("./tests/resources/raw_data.csv", header=list(range(6)), index_col=list(range(4)))
    return df_out

class TestOtherFixture:
    def test_response_fix(self, response_fix):
     assert response_fix
     assert response_fix["Datastreams"]

    def test_df_fix(self, df_fix):
        assert df_fix.shape == (10853, 14)
        assert df_fix.columns.names == [None, Df.DATASTREAM_ID, Properties.DESCRIPTION, Entities.SENSOR, Df.OBSERVATION_TYPE, Df.UNITS]
        assert df_fix.index.names == [f"{Df.TIME}_round", "dt", Df.LAT, Df.LONG]
    
class TestOther:
    def test_unique_values_series_float(self):
        idx_list = list(range(10))
        series_in = pd.Series(index = list(range(10)))
        series_in.iloc[[3,9]] = 8.
        out = get_unique_value_series(series_in)
        assert out == 8.

    def test_unique_values_series_nan(self):
        idx_list = list(range(10))
        series_in = pd.Series(index = list(range(10)))
        out = get_unique_value_series(series_in)
        assert pd.isna(out)

    def test_unique_values_series_idx_error(self):
        idx_list = list(range(10))
        series_in = pd.Series(index = list(range(10)))
        series_in.iloc[[3,9]] = 8.
        series_in.iloc[[0]] = 0.
        with pytest.raises(AssertionError):
            out = get_unique_value_series(series_in)
