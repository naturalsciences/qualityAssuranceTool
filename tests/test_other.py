import json

import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest
from pandassta.df import Df, QualityFlags
from pandassta.sta import Entities, Properties
from pandassta.sta_requests import response_datastreams_to_df

from aums_data_request import (
    datastream_id_in_list_filter_conditions,
    find_nearest_idx,
    get_agg_data_from_pivoted,
    get_agg_from_response,
    get_flag_columns,
    get_results_specified_datastreams_query,
    get_unique_value_series,
    time_conversions,
    wrapper_pivot_df,
)


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
        assert df_pivoted_fix.index.names == [
            f"{Df.TIME}_round",
            "dt",
            Df.LAT,
            Df.LONG,
            Df.IOT_ID,
        ]


class TestOther:
    def test_datastream_id_in_list_filter_conditions(self):
        filter_ds_cfg = datastream_id_in_list_filter_conditions([7751, 7769])
        assert filter_ds_cfg == "@iot.id eq 7751 or @iot.id eq 7769"

    def test_get_results_specified_datastreams_query(self):
        Q = get_results_specified_datastreams_query(
            1,
            filter_condition_observations="$filter=result gt 0.6 and phenomenonTime gt 2023-01-02",
            filter_conditions_datastreams=datastream_id_in_list_filter_conditions(
                [7751, 7769]
            ),
        )
        Q.base_url = ""
        assert (
            Q.build()
            == "/Things(1)?$select=Datastreams&$expand=Datastreams($filter=@iot.id eq 7751 or @iot.id eq 7769;$expand=Observations($filter=$filter=result gt 0.6 and phenomenonTime gt 2023-01-02;$count=false;$expand=FeatureOfInterest($select=feature/coordinates,@iot.id);$select=@iot.id,result,phenomenonTime,resultQuality),ObservedProperty($select=@iot.id,name),Sensor($select=name,@iot.id,description);$select=@iot.id,name,description,unitOfMeasurement/name,Observations)"
        )

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
        assert pivoted.index.get_level_values(
            f"{Df.TIME}_round"
        ).is_monotonic_increasing
        assert ~pivoted.index.get_level_values(f"{Df.TIME}_round").has_duplicates

    def test_get_flag_columns(self, df_pivoted_fix):
        cq = get_flag_columns(df_pivoted_fix)
        assert len(cq) == df_pivoted_fix.shape[1] / 2
        assert len([cqi for cqi in cq.get_level_values(0) if Df.QC_FLAG == cqi])

    # REFACTORING needed ... type conversion is hell
    def test_add_data_from_pivoted(self, df_pivoted_fix):
        cq = get_flag_columns(df_pivoted_fix)
        df_agg = get_agg_data_from_pivoted(df_pivoted_fix, flag_columns=cq)
        arr_columns = [
            df_agg.columns.get_level_values(i) for i in range(df_agg.columns.nlevels)
        ]
        arr_columns[0] = [str(i) for i in arr_columns[0]]  # type: ignore
        df_agg.columns = pd.MultiIndex.from_arrays(
            arr_columns, names=df_agg.columns.names
        )
        df_agg_ref = pd.read_csv(
            "./tests/resources/df_agg.csv", header=list(range(6)), index_col=0
        )
        columns_df = pd.DataFrame(df_agg_ref.columns.to_list())
        for i in range(5):
            columns_df.loc[columns_df[i + 1].str.startswith("Unnamed"), i + 1] = ""
        df_agg_ref.columns = pd.MultiIndex.from_tuples(
            columns_df.to_records(index=False).tolist(), names=df_agg_ref.columns.names
        )
        df_agg_ref = df_agg_ref.astype(dtype=df_agg.dtypes.to_dict())
        df_agg_ref.index = df_agg_ref.index.astype("datetime64[ns]")

        df_agg[get_flag_columns(df_agg, level=1)] = (
            df_agg[get_flag_columns(df_agg, level=1)].astype(str).astype(float)
        )

        pdt.assert_frame_equal(df_agg, df_agg_ref, rtol=0.01, check_dtype=False)

    def test_agg_from_response(self, response_fix):
        df_agg = get_agg_from_response(response_fix)

        arr_columns = [
            df_agg.columns.get_level_values(i) for i in range(df_agg.columns.nlevels)
        ]
        arr_columns[0] = [str(i) for i in arr_columns[0]]  # type: ignore
        df_agg.columns = pd.MultiIndex.from_arrays(
            arr_columns, names=df_agg.columns.names
        )
        df_agg_ref = pd.read_csv(
            "./tests/resources/df_agg.csv", header=list(range(6)), index_col=0
        )
        columns_df = pd.DataFrame(df_agg_ref.columns.to_list())
        for i in range(5):
            columns_df.loc[columns_df[i + 1].str.startswith("Unnamed"), i + 1] = ""
        df_agg_ref.columns = pd.MultiIndex.from_tuples(
            columns_df.to_records(index=False).tolist(), names=df_agg_ref.columns.names
        )
        df_agg_ref = df_agg_ref.astype(dtype=df_agg.dtypes.to_dict())
        df_agg_ref.index = df_agg_ref.index.astype("datetime64[ns]")

        df_agg[get_flag_columns(df_agg, level=1)] = (
            df_agg[get_flag_columns(df_agg, level=1)].astype(str).astype(float)
        )

        pdt.assert_frame_equal(df_agg, df_agg_ref, rtol=0.01, check_dtype=False)

    @pytest.mark.parametrize(
        "array_in, value_in, idx_ref",
        [
            (np.array([1, 2, 3, 4, 5]), 2.3, 1),
            ([1, 2, 3, 4, 5], 2.3, 1),
            ([1, 2, 3, 4, 5], 2.6, 2),
            ([1, 2, 3, 4, 5], 5, 4),
            ([1, 5, 3, 4, 5], 5, 1),
        ],
    )
    def test_find_nearest(self, array_in, value_in, idx_ref):
        out = find_nearest_idx(array_in, value_in)
        assert out == idx_ref
