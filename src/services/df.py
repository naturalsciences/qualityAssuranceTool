import logging
import pandas as pd
from models.enums import Entities, Properties


from copy import deepcopy

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