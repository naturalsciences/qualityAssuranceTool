import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Tuple

from models.constants import ISO_STR_FORMAT
from models.enums import Properties


log = logging.getLogger(__name__)

@dataclass
class PhenomenonTimeFilter:
    format: str
    range: Tuple[str, str]


@dataclass
class ThingConfig:
    id: int


@dataclass
class FilterEntry:
    phenomenonTime: PhenomenonTimeFilter


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


@dataclass
class DbCredentials:
    database: str
    user: str
    host: str
    port: int
    passphrase: str


@dataclass
class LocationConfig:
    connection: DbCredentials
    crs: str
    time_window: str
    max_dx_dt: float


@dataclass
class QCconf:
    data_api: DataApi
    location: LocationConfig
    QC_dependent: list[QcDependentEntry]
    QC: dict[str, QcEntry]


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
