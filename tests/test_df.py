import json

import geopandas as gpd
import geopy.distance as gp_distance
import pandas as pd
import pandas.testing as pdt
import pytest
from geopy import Point as gp_point
from pandas.api import types
from test_config import cfg
from test_queries import (mock_response, mock_response_full,
                          mock_response_full_obs)

from services.pandasta.df import (Df, QualityFlags, get_acceleration_series,
                                  get_distance_geopy_series,
                                  get_dt_velocity_and_acceleration_series,
                                  get_velocity_series, response_obs_to_df,
                                  response_single_datastream_to_df,
                                  series_to_patch_dict)
from services.pandasta.requests import (Query, get_request,
                                        get_results_n_datastreams,
                                        response_datastreams_to_df)
from services.pandasta.sta import Entities, Properties
from services.pandasta.df import CAT_TYPE


@pytest.fixture
def df_velocity_acceleration() -> gpd.GeoDataFrame:
    df_t = pd.read_csv("./tests/resources/data_velocity_acc.csv", header=0)
    df_t[Df.TIME] = pd.to_timedelta(df_t["Time (s)"], "s") + pd.Timestamp("now")

    p0 = gp_point(longitude=3.1840709669760137, latitude=51.37115902107277)
    for index, row_i in df_t.iterrows():
        di = gp_distance.distance(meters=row_i["Distance (m)"])
        pi = di.destination(point=p0, bearing=row_i["Heading (degrees)"])

        df_t.loc[index, [Df.LONG, Df.LAT]] = pi.longitude, pi.latitude  # type: ignore
        p0 = pi

    df_t = df_t.drop(columns=["Time (s)", "Distance (m)", "Heading (degrees)"])
    df_t = gpd.GeoDataFrame(df_t, geometry=gpd.points_from_xy(df_t[Df.LONG], df_t[Df.LAT], crs="EPSG:4326"))  # type: ignore
    return df_t


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
        response_in = get_results_n_datastreams(
            Query(base_url="test.be", root_entity=Entities.DATASTREAMS)
        )[1]
        datastreams_data = response_in[Entities.DATASTREAMS]
        df = response_single_datastream_to_df(datastreams_data[1])
        assert not Entities.FEATUREOFINTEREST in df.keys()

    # incomplete!
    def test_shape_observations_request_to_df(self, mock_response_full_obs):
        response_in = get_request(
            Query(base_url="test.be", root_entity=Entities.DATASTREAMS)
        )[1]
        df = response_obs_to_df(response_in)
        assert df.shape == (10, 6)

    # incomplete comparison
    # @pytest.mark.skip(reason="fails  AND not used")
    def test_shape_datastreams_request_to_df(self, mock_response_full):
        response_in = get_results_n_datastreams(
            Query(base_url="test.be", root_entity=Entities.DATASTREAMS)
        )[1]
        df = response_datastreams_to_df(response_in)
        assert df.shape == (27, 11)

    @pytest.mark.skip(reason="fails after including qc flag")
    def test_num_dtypes_datastreams_request_to_df(self, mock_response_full):
        response_in = get_results_n_datastreams(
            Query(base_url="test.be", root_entity=Entities.DATASTREAMS)
        )[1]
        datastreams_data = response_in[Entities.DATASTREAMS]
        df = response_single_datastream_to_df(datastreams_data[1])

        assert all(
            types.is_numeric_dtype(df[ci])
            for ci in [Properties.IOT_ID, Df.RESULT, Df.DATASTREAM_ID, Df.LONG, Df.LAT]
        )

    def test_series_to_patch(self):
        entry = pd.Series({Df.IOT_ID: 123, Df.QC_FLAG: QualityFlags.BAD}, name=4)
        patch_out = series_to_patch_dict(
            entry,
            group_per_x=3,
            url_entity=Entities.OBSERVATIONS,
        )
        ref = {
            "id": str(5),
            "atomicityGroup": "Group2",
            "method": "patch",
            "url": "Observations(123)",
            "body": {"resultQuality": 4},
        }
        assert patch_out == ref

    def test_get_dt_velocity_and_acceleration(self, df_velocity_acceleration):
        df_file = pd.read_csv("./tests/resources/data_velocity_acc.csv", header=0)
        dt, velocity, acc = get_dt_velocity_and_acceleration_series(
            df_velocity_acceleration
        )

        pdt.assert_series_equal(
            df_file.loc[~velocity.isnull(), "Velocity (m/s)"],
            velocity.loc[~velocity.isnull()],
            check_names=False,
            rtol=1e-3,
        )
        pdt.assert_series_equal(
            df_file.loc[~acc.isnull(), "Acceleration (m/s²)"],
            acc.loc[~acc.isnull()],
            check_names=False,
            rtol=1e-3,
        )

    def test_get_velocity(self, df_velocity_acceleration):
        df_file = pd.read_csv("./tests/resources/data_velocity_acc.csv", header=0)
        dt_, velocity = get_velocity_series(df_velocity_acceleration, return_dt=True)
        velocity = velocity.fillna(0.0)

        pdt.assert_series_equal(
            df_file["Velocity (m/s)"], velocity, check_names=False, check_index=False
        )
        pdt.assert_series_equal(
            df_file["dt"], dt_, check_names=False, check_index=False
        )

    def test_get_velocity_return_dt_false(self, df_velocity_acceleration):
        df_file = pd.read_csv("./tests/resources/data_velocity_acc.csv", header=0)
        velocity = get_velocity_series(df_velocity_acceleration)
        velocity = velocity.fillna(0.0)  # type: ignore

        pdt.assert_series_equal(
            df_file["Velocity (m/s)"], velocity, check_names=False, check_index=False
        )

    def test_get_acceleration(self, df_velocity_acceleration):
        df_file = pd.read_csv("./tests/resources/data_velocity_acc.csv", header=0)
        dt_, acceleration = get_acceleration_series(
            df_velocity_acceleration, return_dt=True
        )
        acc_ = get_acceleration_series(df_velocity_acceleration, return_dt=False)
        acc__ = get_acceleration_series(df_velocity_acceleration)
        pdt.assert_series_equal(acc_, acceleration)  # type: ignore
        pdt.assert_series_equal(acc__, acceleration)  # type: ignore
        acceleration = acceleration.fillna(0.0)
        pdt.assert_series_equal(
            df_file.loc[acceleration.index, "Acceleration (m/s²)"],
            acceleration,
            check_names=False,
            check_index=True,
        )

    def test_get_distance_geopy_Ghent_Brussels(self):
        lat_g, lon_g = 51.053562, 3.720867
        lat_b, lon_b = 50.846279, 4.354727
        points = gpd.points_from_xy([lon_g, lon_b], [lat_g, lat_b], crs="EPSG:4326")
        dfg = gpd.GeoDataFrame(geometry=points)  # type: ignore
        distance_series = get_distance_geopy_series(dfg)
        assert pytest.approx(50.03e3, rel=3e-3) == distance_series.iloc[0]

    def test_fixture_velocity_acceleration(self, df_velocity_acceleration):
        df_file = pd.read_csv("./tests/resources/data_velocity_acc.csv", header=0)
        pdt.assert_series_equal(
            get_distance_geopy_series(df_velocity_acceleration).iloc[:-1],
            df_file["Distance (m)"].iloc[1:],
            check_index=False,
            check_names=False,
        )
