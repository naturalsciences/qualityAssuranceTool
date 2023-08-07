from dataclasses import dataclass
from datetime import datetime
from typing import Tuple

from models.constants import ISO_STR_FORMAT
from models.enums import Properties


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
class DataApi:
    base_url: str
    things: ThingConfig
    filter: FilterEntry


@dataclass
class Range:
    range: Tuple[float, float]


@dataclass
class QcDependentEntry:
    independent: int
    dependent: int
    QC: Range


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
class RegionsConfig:
    connection: DbCredentials
    crs: str


@dataclass
class QCconf:
    data_api: DataApi
    regions: RegionsConfig
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
    return filter_condition
