import pandas as pd

from pydap.client import open_url
import pydap.lib

from services.regions_query import get_depth_from_etop
pydap.lib.CACHE = "/tmp/cache-pydap/" # type: ignore


def main():
    pydap.lib.CACHE = "/tmp/cache-pydap/" # type: ignore
    url_etop = "https://www.ngdc.noaa.gov/thredds/dodsC/global/ETOPO2022/60s/60s_geoid_netcdf/ETOPO_2022_v1_60s_N90W180_geoid.nc"
    dataset = open_url(url_etop)

    # Specify the variable(s) you want to retrieve
    variable_name = "z"

    # Define a list of latitude and longitude coordinates
    coordinates = [
        (40.0, -120.0),
        (41.0, -121.0),
        (42.0, -122.0),
        # Add more coordinates as needed
    ]
    # Retrieve the latitude and longitude variables
    latitude_var = dataset["lat"]
    longitude_var = dataset["lon"]

    df = pd.DataFrame(coordinates, columns=["lat", "lon"])
    # df["lat_idx"] = df.apply(lambda x: find_nearest_idx(latitude_var, x["lat"]), axis=1)
    # df["lon_idx"] = df.apply(
    #     lambda x: find_nearest_idx(longitude_var, x["lon"]), axis=1
    # )

    # df["z"] = df.apply(
    #     lambda x: dataset.z[int(x["lat_idx"]), int(x["lon_idx"])].z.data.take(0), axis=1
    # )
    df_compare = get_depth_from_etop(
        lat=df["lat"],
        lon=df["lon"],
        grid_data=dataset.z,
        lat_var=latitude_var,
        lon_var=longitude_var,
    )
    print("testing")


if __name__ == "__main__":
    main()
