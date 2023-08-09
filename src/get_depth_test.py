import pandas as pd

from pydap.client import open_url
import pydap.lib

from services.regions_query import get_depth_from_etop
from utils.utils import find_nearest_idx
pydap.lib.CACHE = "/tmp/cache-pydap/" # type: ignore

import netCDF4 as nc

def main():

    url_etop = "https://www.ngdc.noaa.gov/thredds/dodsC/global/ETOPO2022/60s/60s_geoid_netcdf/ETOPO_2022_v1_60s_N90W180_geoid.nc"
    dataset_online = open_url(url_etop)

    dataset = nc.Dataset("./resources/ETOPO_2022_v1_60s_N90W180_bed.nc")
    # Define a list of latitude and longitude coordinates
    coordinates = [
        (40.0, -120.0),
        (52.0, 3.0),
        (51.25036194708964, 2.9627686111810965)
        
        # Add more coordinates as needed
    ]
    # Retrieve the latitude and longitude variables
    latitude_var = dataset["lat"][:]
    longitude_var = dataset["lon"][:]

    df = pd.DataFrame(coordinates, columns=["lat", "lon"])
    df["lat_idx"] = df.apply(lambda x: find_nearest_idx(latitude_var, x["lat"]), axis=1)
    df["lon_idx"] = df.apply(
        lambda x: find_nearest_idx(longitude_var, x["lon"]), axis=1
    )

    df["z"] = df.apply(lambda x: dataset['z'][x["lat_idx"], x["lon_idx"]], axis=1)

    # df["z"] = df.apply(
    #     lambda x: dataset.z[int(x["lat_idx"]), int(x["lon_idx"])].z.data.take(0), axis=1
    # )
    # df_compare = get_depth_from_etop_2(
    #     lat=df["lat"],
    #     lon=df["lon"],
    #     grid_data=dataset_online.z,
    #     lat_var=latitude_var,
    #     lon_var=longitude_var,
    # )
    print("testing")


if __name__ == "__main__":
    main()
