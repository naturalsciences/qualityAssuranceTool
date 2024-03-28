import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Tuple

from omegaconf import MISSING
from pandassta.logging_constants import ISO_STR_FORMAT
from pandassta.sta import (DbCredentials, FilterEntry, PhenomenonTimeFilter,
                          Properties)

from searegion_detection.pandaseavox import DbCredentials

log = logging.getLogger(__name__)


@dataclass
class ThingConfig:
    id: int


@dataclass
class SensorThingsAuth:
    username: str
    passphrase: str


@dataclass
class DataApi:
    base_url: str
    things: ThingConfig
    filter: FilterEntry
    auth: SensorThingsAuth


@dataclass
class Range:
    range: Tuple[float, float]


@dataclass
class QcDependentEntry:
    independent: int
    dependent: int
    QC: Range
    dt_tolerance: str


@dataclass
class QcEntry:
    range: Range
    gradient: Range
    zscore: Range


@dataclass
class LocationConfig:
    connection: DbCredentials
    crs: str
    time_window: str
    max_dx_dt: float
    max_ddx_dtdt: float


@dataclass
class ResetConfig:
    overwrite_flags: bool = field(default=False)
    observation_flags: bool = field(default=False)
    feature_flags: bool = field(default=False)
    exit: bool = field(default=False)


@dataclass
class DateConfig:
    format: str

    
@dataclass
class TimeConfig:
    start: str
    end: str
    date: DateConfig
    format: str = field(default="%Y-%m-%d %H:%M")
    window: Optional[str] = field(default=None)


@dataclass
class HydraRunConfig:
    dir: str
    
    
@dataclass
class HydraConfig:
    run: HydraRunConfig
    verbose: Optional[str] = field(default=None)


@dataclass
class QCconf:
    time: TimeConfig
    hydra: HydraConfig
    data_api: DataApi
    reset: ResetConfig
    location: LocationConfig
    QC_dependent: list[QcDependentEntry]
    QC: dict[str, QcEntry]
    QC_global: dict[str, QcEntry] = field(default_factory=dict)


def filter_cfg_to_query(filter_cfg: FilterEntry) -> str:
    filter_condition = ""
    if filter_cfg:
        range = filter_cfg.phenomenonTime.range
        format = filter_cfg.phenomenonTime.format

        t0, t1 = [datetime.strptime(str(ti), format) for ti in range]

        filter_condition = (
            f"{Properties.PHENOMENONTIME} gt {t0.strftime(ISO_STR_FORMAT)} and "
            f"{Properties.PHENOMENONTIME} lt {t1.strftime(ISO_STR_FORMAT)}"
        )
    log.debug(f"Configure filter: {filter_condition=}")
    return filter_condition


def get_date_from_string(
    str_in: str, str_format_in: str = "%Y-%m-%d %H:%M", str_format_out: str = "%Y%m%d"
) -> str:
    date_out = datetime.strptime(str(str_in), str_format_in)
    return date_out.strftime(str_format_out)


