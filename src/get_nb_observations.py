import logging
import os
import sys
import threading
import time
from functools import partial
from pathlib import Path

import geopandas as gpd
import hydra
import pandas as pd
import stapy
from dotenv import load_dotenv
from git import Repo
from omegaconf import OmegaConf

from models.constants import FEATURES_BODY_TEMPLATE
from models.enums import Df, Entities, QualityFlags
from services.config import QCconf, filter_cfg_to_query
from services.df import intersect_df_region
from services.qc import (QCFlagConfig, calc_gradient_results,
                         get_bool_depth_above_treshold,
                         get_bool_exceed_max_acceleration,
                         get_bool_exceed_max_velocity, get_bool_land_region,
                         get_bool_null_region, get_bool_out_of_range,
                         get_bool_spacial_outlier_compared_to_median,
                         qc_dependent_quantity_base,
                         qc_dependent_quantity_secondary,
                         update_flag_history_series)
from services.requests import get_all_data, get_elev_netcdf, get_total_observations_count, patch_qc_flags
from utils.utils import (get_date_from_string,
                         get_dt_velocity_and_acceleration_series)

log = logging.getLogger(__name__)

load_dotenv()

OmegaConf.register_new_resolver("datetime_to_date", get_date_from_string, replace=True)


@hydra.main(config_path="../conf", config_name="config.yaml", version_base="1.2")
def main(cfg: QCconf):
    log_extra = logging.getLogger(name="extra")
    log_extra.setLevel(logging.INFO)
    rootlog = logging.getLogger()
    extra_log_file = Path(
        getattr(rootlog.handlers[1], "baseFilename", "./")
    ).parent.joinpath("history.log")
    file_handler_extra = logging.FileHandler(extra_log_file)
    file_handler_extra.setFormatter(rootlog.handlers[0].formatter)
    log_extra.addHandler(file_handler_extra)

    def custom_exception_handler(exc_type, exc_value, exc_traceback):
        # Log the exception
        log.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

        # Call the default exception hook (prints the traceback and exits)
        sys.__excepthook__(exc_type, exc_value, exc_traceback)

    sys.excepthook = custom_exception_handler

    t0 = time.time()
    docker_image_tag = os.environ.get("IMAGE_TAG", None)
    if docker_image_tag:
        log.info(f"Docker image tag: {docker_image_tag}.")
    git_repo = Repo(search_parent_directories=True)
    git_commit_hash = git_repo.head.object.hexsha
    if git_commit_hash:
        log.info(f"The git hash: {git_commit_hash} on {git_repo.head.reference}.")

    log.info("Start")

    history_series = pd.Series()

    # setup
    log.info("Setup")
    t_df0 = time.time()
    stapy.config.filename = Path("outputs/.stapy.ini")
    stapy.set_sta_url(cfg.data_api.base_url)
    url_batch = cfg.data_api.base_url + "/$batch"

    auth_tuple = (
        getattr(cfg.data_api, "auth", {}).get("username", None),
        getattr(cfg.data_api, "auth", {}).get("passphrase", None),
    )
    auth_in = [None, auth_tuple][all(auth_tuple)]

    thing_id = cfg.data_api.things.id

    filter_cfg = filter_cfg_to_query(cfg.data_api.filter)

    total_observations_count = get_total_observations_count(thing_id=thing_id, filter_cfg=filter_cfg)
    

if __name__ == "__main__":
    log.debug("testing...")
    main()
