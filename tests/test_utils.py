import json

import geopandas as gpd
import geopy.distance as gp_distance
import pandas as pd
import pandas.testing as pdt
import pytest
# import stapy
from services.pandasta.df import Df, get_acceleration_series, get_distance_geopy_series, get_dt_velocity_and_acceleration_series, get_velocity_series, series_to_patch_dict
from geopy import Point as gp_point

from services.qualityassurancetool.qualityflags import QualityFlags
from services.qualityassurancetool.config import get_date_from_string
import services.pandasta.requests as u
from services.pandasta.sta import Entities


# class TestUtils:


   
    # def test_stapy_integration(self, cfg):
        # q = u.Query(u.Entity.Thing).entity_id(0)
        # assert q.get_query() == "http://testing.com/v1.1/Things(0)"



