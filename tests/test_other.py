import json

import geopandas as gpd
import geopy.distance as gp_distance
import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest
import stapy
from geopy import Point as gp_point
from hydra import compose, initialize
from omegaconf import DictConfig
from services.requests import get_query_response

import utils.utils as u
from models.constants import ISO_STR_FORMAT, ISO_STR_FORMAT2
from models.enums import Df, Entities, QualityFlags
from utils.utils import (combine_dicts, convert_to_datetime, find_nearest_idx,
                         get_absolute_path_to_base, get_acceleration_series,
                         get_date_from_string, get_distance_geopy_series,
                         get_dt_velocity_and_acceleration_series,
                         get_velocity_series, series_to_patch_dict)


@pytest.fixture(scope="session")
def response_fix() -> dict:
    with open("./tests/resources/aums_data_request_response.json") as f:
        response_out = json.load(f)
    return response_out

@pytest.fixture(scope="session")
def df_fix() -> pd.DataFrame:
    df_out = pd.read_csv("./tests/resources/raw_data.csv", header=list(range(6)), index_col=list(range(4)))
    return df_out

class TestOther:
    def test_response_fix(self, response_fix):
     assert response_fix
     assert response_fix["Datastreams"]

    def test_df_fix(self, df_fix):
        assert df_fix.shape == (10853, 14)

