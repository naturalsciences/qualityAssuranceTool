import logging
import time

import geopandas as gpd
import hydra
import pandas as pd
import stapy
from pydap.client import open_url

from models.enums import Df, QualityFlags
from services.config import QCconf, filter_cfg_to_query
from services.df import intersect_df_region
from services.qc import (
    calc_gradient_results,
    get_bool_land_region,
    get_bool_null_region,
    get_bool_out_of_range,
    get_bool_spacial_outlier_compared_to_median,
    get_qc_flag_from_bool,
    qc_dependent_quantity_base,
    qc_dependent_quantity_secondary,
    qc_region,
)
from services.regions_query import get_depth_from_etop
from services.requests import get_all_data, patch_qc_flags

log = logging.getLogger(__name__)

def get_bool_depth_below_threshold(df: pd.DataFrame, threshold: float) -> pd.Series:
    mask_is_none = df[Df.REGION].isnull()  # type: ignore
    df_coords_none_unique = df.loc[mask_is_none, [Df.LONG, Df.LAT]] # type: ignore
    bool_depth = get_depth_from_etop(
        lat=df_coords_none_unique[Df.LAT],  # type: ignore
        lon=df_coords_none_unique[Df.LONG]  # type: ignore
    ) < threshold
    bool_out = pd.Series(bool_depth, index=df.loc[mask_is_none].index) # type: ignore
    return bool_out


@hydra.main(config_path="../conf", config_name="config.yaml", version_base="1.2")
def main(cfg: QCconf):
    t0 = time.time()
    log.info("Start")

    # setup
    t_df0 = time.time()
    stapy.set_sta_url(cfg.data_api.base_url)

    thing_id = cfg.data_api.things.id

    filter_cfg = filter_cfg_to_query(cfg.data_api.filter)

    # get data in dataframe
    df_all = get_all_data(thing_id=thing_id, filter_cfg=filter_cfg)
    nb_observations = df_all.shape[0]
    df_all = gpd.GeoDataFrame(df_all, geometry=gpd.points_from_xy(df_all[Df.LONG], df_all[Df.LAT]), crs=cfg.location.crs)  # type: ignore
    # get qc check df (try to find clearer name)
    qc_df = pd.DataFrame.from_dict(cfg.QC, orient="index")
    qc_df.index.name = Df.OBSERVATION_TYPE

    ## setup needed columns. Should these be removed?
    t_ranges0 = time.time()
    for qc_type in qc_df.keys():
        qc_df[[f"qc_{'_'.join([qc_type, i])}" for i in ["min", "max"]]] = qc_df.pop(
            qc_type
        ).apply(pd.Series)

    df_all = calc_gradient_results(df_all, Df.DATASTREAM_ID)
    

    t_df1 = time.time()
    t_qc0 = time.time()
    ## find region
    t_region0 = time.time()
    df_all = intersect_df_region(
        db_credentials=cfg.location.connection,
        df=df_all,
        max_queries=5,
        max_query_points=20,
    )

    bool_nan = get_bool_null_region(df_all)
    df_all.update(
        get_qc_flag_from_bool(
            df_all,
            bool_=bool_nan,
            flag_on_true=QualityFlags.PROBABLY_BAD,
            update_verified=False,
        )[[Df.QC_FLAG]]
    )

    bool_mainland = get_bool_land_region(df_all)
    df_all.update(
        get_qc_flag_from_bool(
            df_all,
            bool_=bool_mainland,
            flag_on_true=QualityFlags.BAD,
            update_verified=False,
        )[[Df.QC_FLAG]]
    )

    bool_depth_below_0 = get_bool_depth_below_threshold(df_all, threshold=0.)
    df_all.update(get_qc_flag_from_bool(
        df_all,
        bool_= bool_depth_below_0,
        flag_on_true=QualityFlags.PROBABLY_GOOD,
        update_verified=False)[[Df.QC_FLAG]])

    ## outliers location
    bool_outlier = get_bool_spacial_outlier_compared_to_median(
        df_all, max_dx_dt=cfg.location.max_dx_dt, time_window=cfg.location.time_window
    )
    df_all.update(
        get_qc_flag_from_bool(
            df_all,
            bool_=bool_outlier,
            flag_on_true=QualityFlags.BAD,
            update_verified=False,
        )[[Df.QC_FLAG]]
    )

    t_region1 = time.time()
    df_all = df_all.merge(qc_df, on=Df.OBSERVATION_TYPE, how="left")
    df_all.set_index(Df.IOT_ID)
    if nb_observations != df_all.shape[0]:
        raise RuntimeError("Not all observations are included in the dataframe.")

    bool_range = get_bool_out_of_range(df=df_all, qc_on=Df.RESULT, qc_type="range")
    df_all.update(
        get_qc_flag_from_bool(
            df_all,
            bool_=bool_range,
            flag_on_true=QualityFlags.BAD,
            update_verified=True,
        )
    )

    # df_merge = set_qc_flag_range_check(
    # df_merge, qc_type="range", qc_on=Df.RESULT, flag_on_fail=QualityFlags.BAD
    # )
    bool_gradient = get_bool_out_of_range(
        df=df_all, qc_on=Df.GRADIENT, qc_type="gradient"
    )
    df_all.update(
        get_qc_flag_from_bool(
            df_all,
            bool_=bool_gradient,
            flag_on_true=QualityFlags.BAD,
            update_verified=True,
        )
    )

    # df_merge = set_qc_flag_range_check(
    # df_merge, qc_type="range", qc_on=Df.GRADIENT, flag_on_fail=QualityFlags.BAD
    # )
    t_ranges1 = time.time()

    t_flag_ranges0 = time.time()
    # df_all[Df.VALID] = df_all[Df.VALID] & df_all[Df.VERIFIED].astype(bool)
    # df_all.loc[df_all[Df.VALID], Df.QC_FLAG] = QualityFlags.GOOD  # type:ignore

    t_flag_ranges1 = time.time()

    t_dependent0 = time.time()
    df_all = qc_dependent_quantity_base(df_all, independent=69, dependent=124)
    df_all = qc_dependent_quantity_secondary(
        df_all, independent=69, dependent=124, range_=(5.0, 10)
    )
    t_dependent1 = time.time()

    log.info(f"{df_all[Df.QC_FLAG].value_counts(dropna=False).to_json()=}")
    log.info(
        f"{df_all[[Df.OBSERVATION_TYPE, Df.QC_FLAG]].value_counts(dropna=False).to_json()=}"
    )

    cfg_dependent = cfg.QC_dependent
    t_qc1 = time.time()
    t_patch0 = time.time()
    t3 = time.time()
    url = "http://localhost:8080/FROST-Server/v1.1/$batch"
    counter = patch_qc_flags(df_all.reset_index(), url=url)
    t_patch1 = time.time()
    tend = time.time()
    log.info(f"df requests/construction duration: {t_df1 - t_df0}")
    # log.info(f"Region check duration: {t_region1 - t_region0}")
    log.info(f"Ranges check duration: {t_ranges1 - t_ranges0}")
    log.info(f"Flagging ranges duration: {t_flag_ranges1 - t_flag_ranges0}")
    log.info(f"Total QC check duration: {t_qc1 - t_qc0}")
    log.info(f"Patch duration: {t_patch1 - t_patch0}")
    log.info(f"Total duration: {tend-t0}")
    log.info("End")


if __name__ == "__main__":
    log.debug("testing...")
    main()
