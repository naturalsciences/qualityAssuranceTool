import json

import geopandas as gpd
import geopy.distance as gp_distance
import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest
# import stapy
from aums_data_request import find_nearest_idx
from main import get_date_from_string
from services.pandasta.df import Df, get_acceleration_series, get_distance_geopy_series, get_dt_velocity_and_acceleration_series, get_velocity_series, series_to_patch_dict
from services.pandasta.requests import config, convert_to_datetime, get_absolute_path_to_base
from geopy import Point as gp_point
from hydra import compose, initialize
from omegaconf import DictConfig
import services.pandasta.requests
from services.pandasta.requests import set_sta_url

from services.qualityassurancetool.qc import combine_dicts
from services.qualityassurancetool.qualityflags import QualityFlags
import services.pandasta.requests as u
from services.pandasta.requests import ISO_STR_FORMAT2
from services.pandasta.sta import Entities
from services.pandasta.requests import (ISO_STR_FORMAT)


@pytest.fixture(scope="session")
def cfg() -> DictConfig:
    with initialize(config_path="./conf", version_base="1.2"):
        conf = compose("conf_base.yaml")
    set_sta_url(conf.data_api.base_url)

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


class TestUtils:

    def test_hydra_is_loaded(self):
        print(cfg)
        assert cfg

    @pytest.mark.parametrize(
        "date_str,date_ref",
        [
            ("2023-01-02T13:14:15.00Z", "20230102131415"),
            ("2023-01-02T13:14:15.030Z", "20230102131415"),
            ("2023-01-02T13:14:15Z", "20230102131415"),
            ("2023-01-02T10:14:15Z", "20230102101415"),
        ],
    )
    def test_convert_to_datetime(self, date_str, date_ref):
        datetime_out = convert_to_datetime(date_str)
        assert datetime_out.strftime("%Y%m%d%H%M%S") == date_ref

    @pytest.mark.parametrize(
        "date_str",
        [
            "202301021314152",
            "2023-01-02T10PM:14:15.030Z",
            "2023-01-02T10:14:.030",
            "2023-01-02 10:14:50.030",
            "",
        ],
    )
    def test_convert_to_datetime_exception(self, date_str):
        with pytest.raises(Exception) as e:
            datetime_out = convert_to_datetime(date_str)
        assert (
            str(e.value)
            == f"time data '{date_str}' does not match format '%Y-%m-%dT%H:%M:%SZ'"
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

    def test_absolute_path_to_base_exists(self):
        out = get_absolute_path_to_base()
        assert out.exists()

    def test_combine_dicts(self):
        out = combine_dicts(
            {"first": 1, "str": "test", "float": 2.3},
            {"second": 2, "str": "ing", "float": 4.5},
        )
        assert out == {"first": 1, "str": "testing", "second": 2, "float": 6.8}

    # def test_stapy_integration(self, cfg):
        # q = u.Query(u.Entity.Thing).entity_id(0)
        # assert q.get_query() == "http://testing.com/v1.1/Things(0)"

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


def test_get_dt_velocity_and_acceleration(df_velocity_acceleration):
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


def test_get_velocity(df_velocity_acceleration):
    df_file = pd.read_csv("./tests/resources/data_velocity_acc.csv", header=0)
    dt_, velocity = get_velocity_series(df_velocity_acceleration, return_dt=True)
    velocity = velocity.fillna(0.0)

    pdt.assert_series_equal(
        df_file["Velocity (m/s)"], velocity, check_names=False, check_index=False
    )
    pdt.assert_series_equal(df_file["dt"], dt_, check_names=False, check_index=False)


def test_get_velocity_return_dt_false(df_velocity_acceleration):
    df_file = pd.read_csv("./tests/resources/data_velocity_acc.csv", header=0)
    velocity = get_velocity_series(df_velocity_acceleration)
    velocity = velocity.fillna(0.0)  # type: ignore

    pdt.assert_series_equal(
        df_file["Velocity (m/s)"], velocity, check_names=False, check_index=False
    )


def test_get_acceleration(df_velocity_acceleration):
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


def test_get_distance_geopy_Ghent_Brussels():
    lat_g, lon_g = 51.053562, 3.720867
    lat_b, lon_b = 50.846279, 4.354727
    points = gpd.points_from_xy([lon_g, lon_b], [lat_g, lat_b], crs="EPSG:4326")
    dfg = gpd.GeoDataFrame(geometry=points)  # type: ignore
    distance_series = get_distance_geopy_series(dfg)
    assert pytest.approx(50.03e3, rel=3e-3) == distance_series.iloc[0]


def test_fixture_velocity_acceleration(df_velocity_acceleration):
    df_file = pd.read_csv("./tests/resources/data_velocity_acc.csv", header=0)
    pdt.assert_series_equal(
        get_distance_geopy_series(df_velocity_acceleration).iloc[:-1],
        df_file["Distance (m)"].iloc[1:],
        check_index=False,
        check_names=False,
    )


def test_get_date_from_string():
    # date_o = get_date_from_string("2023-04-01 12:15", "%Y-%m-%d %H:%M", "%Y%m%d")
    date_o = get_date_from_string("2023-04-01 12:15")
    assert date_o == "20230401"
