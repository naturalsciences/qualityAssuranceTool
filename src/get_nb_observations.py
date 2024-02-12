import logging
import os
import re
import sys
import time
from copy import deepcopy
from datetime import datetime
from functools import partial
from pathlib import Path

import hydra
import pandas as pd
import stapy
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from hydra.core.hydra_config import HydraConfig
from omegaconf import OmegaConf

from models.enums import Properties
from services.config import QCconf, filter_cfg_to_query
from services.requests import get_total_observations_count
from utils.utils import get_date_from_string

log = logging.getLogger(__name__)
# log.setLevel(logging.CRITICAL)
loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
for li in loggers:
    li.setLevel(logging.CRITICAL)


load_dotenv()


def parse_window(window_str: str) -> relativedelta:
    split_digits_ascii = re.split(r"([\d\.]+)", window_str.strip(), maxsplit=1)
    split_digits_ascii.remove("")
    nb, unit = split_digits_ascii
    nb = nb.strip()
    unit = unit.strip()
    if not unit.endswith("s"):
        unit += "s"
    relativedelta_args = {unit: float(nb)}
    out = relativedelta(**relativedelta_args)  # type: ignore
    return out.normalized()

    
def parse_count_summary(file: str | Path) -> pd.DataFrame:
    df = pd.read_csv(file, sep=" - |, |: ", header=0, skiprows=3, names=["timestamp", "start", "end", "count"], engine="python")
    df["start"] = pd.to_datetime(df["start"].str.strip("()"))
    df["end"] = pd.to_datetime(df["end"].str.strip("()"))
    df = df.drop(columns=["timestamp"])

    return df


default_logger_format = "[%(asctime)s][%(name)s][%(levelname)s] - %(message)s"

OmegaConf.register_new_resolver("datetime_to_date", get_date_from_string, replace=True)


@hydra.main(
    config_path="../conf", config_name="config_counter.yaml", version_base="1.2"
)
def main(cfg: QCconf):
    # log = logging.getLogger(__name__)
    log.setLevel(logging.CRITICAL)
    loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    for li in loggers:
        li.setLevel(logging.CRITICAL)

    log_counter = logging.getLogger(name="counter")
    log_counter.setLevel(logging.INFO)
    rootlog = logging.getLogger()
    extra_log_file = Path(HydraConfig.get().run.dir).joinpath("summary.log")
    file_handler_extra = logging.FileHandler(extra_log_file)
    file_handler_extra.setFormatter(logging.Formatter(default_logger_format))
    log_counter.addHandler(file_handler_extra)
    log_counter.setLevel(logging.INFO)

    def custom_exception_handler(exc_type, exc_value, exc_traceback):
        # Log the exception
        log.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

        # Call the default exception hook (prints the traceback and exits)
        sys.__excepthook__(exc_type, exc_value, exc_traceback)

    sys.excepthook = custom_exception_handler

    datetime_to_str = partial(
        lambda fmt, datetime_in: datetime.strftime(datetime_in, fmt), cfg.time.format
    )

    t0 = time.time()
    docker_image_tag = os.environ.get("IMAGE_TAG", None)
    if docker_image_tag:
        log.info(f"Docker image tag: {docker_image_tag}.")
    # git_repo = Repo(search_parent_directories=True)
    # if git_repo:
    # git_commit_hash = git_repo.head.object.hexsha
    # log.info(f"The git hash: {git_commit_hash} on {git_repo.head.reference}.")

    log.info("Start COUNTING")

    log_counter.info("-" * 75)
    log_counter.info(" " * 19 + f"{cfg.time.start} --> {cfg.time.end}" + " " * 19)
    log_counter.info("-" * 75)

    history_series = pd.Series()

    # setup
    log.info("Setup")
    t_df0 = time.time()
    stapy.config.filename = Path("outputs/.stapy.ini")
    stapy.set_sta_url(cfg.data_api.base_url)

    auth_tuple = (
        getattr(cfg.data_api, "auth", {}).get("username", None),
        getattr(cfg.data_api, "auth", {}).get("passphrase", None),
    )
    auth_in = [None, auth_tuple][all(auth_tuple)]

    thing_id = cfg.data_api.things.id

    filter_cfg = filter_cfg_to_query(cfg.data_api.filter)

    df_count = pd.DataFrame(columns=["t0", "t1", "dt", "nb"])
    t0 = datetime.strptime(str(cfg.time.start), cfg.time.format)
    t1 = datetime.strptime(str(cfg.time.end), cfg.time.format)
    count_window = getattr(cfg.time, "window", None)

    if count_window:
        dt = parse_window(count_window)
    else:
        raise IOError(f"No dt is defined in the config.")

    ti = deepcopy(t0)
    filter_i = deepcopy(cfg.data_api.filter)

    while ti < t1:
        filter_i[Properties.PHENOMENONTIME]["range"] = [datetime_to_str(ti), datetime_to_str(ti + dt)]  # type: ignore
        nbi: int = get_total_observations_count(
            thing_id=thing_id, filter_cfg=filter_cfg_to_query(filter_i)
        )
        log_counter.info(f"({datetime_to_str(ti), datetime_to_str(ti+dt)}): {nbi}")
        ti += dt
    df_logs = parse_count_summary(extra_log_file)
    log_counter.info(f"{'*'*75}")
    log_str_count = f"TOTAL COUNT: {df_logs['count'].sum()}"
    log_counter.info(f"{log_str_count:*^75}")
    log_str_rows = f"{df_logs['count'].astype(bool).sum(axis=0)} of the {df_logs.shape[0]} counts above zero"
    log_counter.info(f"{log_str_rows:*^75}")
    log_counter.info(f"{'*'*75}")


if __name__ == "__main__":
    log.debug("testing...")
    main()
