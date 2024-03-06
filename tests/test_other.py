import json

import pandas as pd
import pytest

from aums_data_request import (get_agg_data_from_pivoted, get_flag_columns, get_unique_value_series, time_conversions,
                               wrapper_pivot_df)
from models.enums import Df, Entities, Properties
from services.requests import response_datastreams_to_df


@pytest.fixture(scope="session")
def response_fix() -> dict:
    with open("./tests/resources/aums_data_request_response.json") as f:
        response_out = json.load(f)
    return response_out


@pytest.fixture(scope="session")
def df_fix(response_fix) -> pd.DataFrame:
    df_out = response_datastreams_to_df(response_fix)
    return df_out

@pytest.fixture(scope="session")
def df_pivoted_fix(df_fix) -> pd.DataFrame:
    df = time_conversions(df_fix)
    df_pivoted_out = wrapper_pivot_df(df)
    return df_pivoted_out


class TestOtherFixture:
    def test_response_fix(self, response_fix):
        assert response_fix
        assert response_fix["Datastreams"]

    def test_df_pivot(self, df_pivoted_fix):
        assert df_pivoted_fix.shape == (10853, 14)
        assert df_pivoted_fix.columns.names == [
            None,
            Df.DATASTREAM_ID,
            Properties.DESCRIPTION,
            Entities.SENSOR,
            Df.OBSERVATION_TYPE,
            Df.UNITS,
        ]
        assert df_pivoted_fix.index.names == [f"{Df.TIME}_round", "dt", Df.LAT, Df.LONG, Df.IOT_ID]


class TestOther:
    def test_unique_values_series_float(self):
        idx_list = list(range(10))
        series_in = pd.Series(index=list(range(10)))
        series_in.iloc[[3, 9]] = 8.0
        out = get_unique_value_series(series_in)
        assert out == 8.0

    def test_unique_values_series_nan(self):
        idx_list = list(range(10))
        series_in = pd.Series(index=list(range(10)))
        out = get_unique_value_series(series_in)
        assert pd.isna(out)

    def test_unique_values_series_idx_error(self):
        idx_list = list(range(10))
        series_in = pd.Series(index=list(range(10)))
        series_in.iloc[[3, 9]] = 8.0
        series_in.iloc[[0]] = 0.0
        with pytest.raises(AssertionError):
            out = get_unique_value_series(series_in)

    def test_time_conversions(self, df_fix, response_fix):
        df = response_datastreams_to_df(response_fix)
        assert ~(f"{Df.TIME}_round" in df.columns)
        df = time_conversions(df)

        assert f"{Df.TIME}_round" in df.columns
        assert df[f"{Df.TIME}"].dt.microsecond.max() > 0.0
        assert df[f"{Df.TIME}_round"].dt.microsecond.max() == 0.0

    def test_wrapper_pivot_df(self, df_fix):
        df = time_conversions(df_fix)
        pivoted = wrapper_pivot_df(df)
        assert pivoted.shape == (10853, 14)
        assert set(list(filter(None, pivoted.columns.names))) == set(
            [
                Df.DATASTREAM_ID,
                Properties.DESCRIPTION,
                str(Entities.SENSOR),
                Df.OBSERVATION_TYPE,
                Df.UNITS,
            ]
        )
        assert set(pivoted.index.names) == set(
            [f"{Df.TIME}_round", "dt", Df.LAT, Df.LONG, Df.IOT_ID]
        )
        assert pivoted.index.get_level_values(f"{Df.TIME}_round").is_monotonic_increasing
        assert ~pivoted.index.get_level_values(f"{Df.TIME}_round").has_duplicates
    
    def test_get_flag_columns(self, df_pivoted_fix):
        cq = get_flag_columns(df_pivoted_fix)
        assert len(cq) == df_pivoted_fix.shape[1]/2
        assert len([cqi for cqi in cq.get_level_values(0) if Df.QC_FLAG == cqi])

    def test_add_data_from_pivoted(self, df_pivoted_fix):
        cq = get_flag_columns(df_pivoted_fix)
        df_agg = get_agg_data_from_pivoted(df_pivoted_fix, flag_columns=cq)
