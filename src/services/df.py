import logging
import pandas as pd
from models.enums import Entities, Properties


from copy import deepcopy

from utils.utils import convert_to_datetime


log = logging.getLogger(__name__)

def df_type_conversions(df):
    df_out = deepcopy(df)
    for ci in ["observation_type", "units", "qc_flag"]:
        mu0 = df_out[[ci]].memory_usage().get(ci)
        df_out[ci] = df_out[ci].astype("category")
        mu1 = df_out[[ci]].memory_usage().get(ci)
        if mu1 > mu0:
            log.warning("df type conversion might not reduce the memory usage!")

    for ci in ["bool"]:
        df_out[ci] = df_out[ci].astype("bool")

    return df_out


def features_request_to_df(request_features):
    data = []
    for fi in request_features["value"]:
        v = fi.get(Properties.IOT_ID)
        long, lat = fi.get("feature").get("coordinates")
        idx = [oi.get(Properties.IOT_ID) for oi in fi.get(Entities.OBSERVATIONS)]
        for idx_i in idx:
            data.append([idx_i, v, long, lat])
    df = pd.DataFrame(data, columns=["observation_id", "feature_id", "long", "lat"])
    return df


def response_single_datastream_to_df(response_datastream: dict) -> pd.DataFrame:
    df = pd.DataFrame()
    for di in response_datastream:
        observations_list = di.get(Entities.OBSERVATIONS)
        if observations_list:
            df_i = pd.DataFrame(observations_list).astype(
                {Properties.IOT_ID: int, "result": float}
            )
            df_i["datastream_id"] = int(di.get(Properties.IOT_ID))
            df_i[Properties.PHENOMENONTIME] = df_i[Properties.PHENOMENONTIME].apply(
                convert_to_datetime
            )
            df_i["observation_type"] = di.get(Entities.OBSERVEDPROPERTY).get(
                Properties.NAME
            )
            df_i["observation_type"] = df_i["observation_type"].astype("category")
            k1, k2 = Properties.UNITOFMEASUREMENT.split("/", 1)
            df_i["units"] = di.get(k1).get(k2)
            df_i["units"] = df_i["units"].astype("category")

            df_i[["long", "lat"]] = pd.DataFrame.from_records(
                df_i[str(Entities.FEATUREOFINTEREST)].apply(
                    lambda x: x.get("feature").get("coordinates")
                )
            )
            del df_i[str(Entities.FEATUREOFINTEREST)]
            # df_i.drop(columns=str(Entities.FEATUREOFINTEREST))
            df = pd.concat([df, df_i], ignore_index=True)

    return df


def response_datastreams_to_df(response: dict) -> pd.DataFrame:
    df_out = pd.DataFrame()
    for ds_i in response[Entities.DATASTREAMS]:
        if f"{Entities.OBSERVATIONS}@iot.nextLink" in ds_i:
            log.warning("Not all observations are extracted!")  # TODO: follow link!
        df_i = response_single_datastream_to_df(response[Entities.DATASTREAMS])
        log.debug(f"{df_i.shape[0]=}")
        df_out = pd.concat([df_out, df_i], ignore_index=True)
    return df_out