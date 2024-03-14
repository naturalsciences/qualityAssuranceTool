from __future__ import annotations

import logging

from strenum import StrEnum

log = logging.getLogger(__name__)


class BaseQueryStrEnum(StrEnum):
    def __str__(self):
        if self.value:
            return f"${self.value}"
        else:
            return ""


class Properties(StrEnum):
    DESCRIPTION = "description"
    UNITOFMEASUREMENT = "unitOfMeasurement/name"
    NAME = "name"
    IOT_ID = "@iot.id"
    COORDINATES = "feature/coordinates"
    PHENOMENONTIME = "phenomenonTime"
    RESULT = "result"
    QC_FLAG = "resultQuality"
    OBSERVATIONS_COUNT = (
        "Observations/@iot.count"  # can this be dynamic? base_entity/count?
    )

    def __str__(self):
        return f"{self.value}"

    def __repr__(self):
        return f"{self.value}"


class Settings(BaseQueryStrEnum):
    TOP = "top"
    SKIP = "skip"
    COUNT = "count"

    def __call__(self, value=None):
        if value is None:
            return ""
        else:
            return f"{self}={str(value)}"


class Entities(StrEnum):
    DATASTREAMS = "Datastreams"
    OBSERVEDPROPERTY = "ObservedProperty"
    OBSERVATIONS = "Observations"
    FEATUREOFINTEREST = "FeatureOfInterest"
    FEATURESOFINTEREST = "FeaturesOfInterest"
    SENSOR = "Sensor"
    THINGS = "Things"

    def __call__(self, args: list[Properties] | list["Qactions"] | list[str]):
        out = f"{self}({';'.join(list(filter(None, args)))})"
        return out

    def __repr__(self):
        return f"{self.value}"


class Qactions(BaseQueryStrEnum):
    EXPAND = "expand"
    SELECT = "select"
    ORDERBY = "orderby"
    NONE = ""

    def __call__(
        self,
        arg: (
            Entities | Properties | list[Properties] | list[Entities] | list[str] | None
        ) = None,
    ):
        out = ""
        if arg:
            str_arg = ",".join(arg)
            out = f"{str(self)}={str_arg}"
        # change to None? for now this would result in error
        return out


class Filter(BaseQueryStrEnum):
    FILTER = "filter"

    def __call__(self, condition: str) -> str:
        out = ""
        if condition:
            out = f"{str(self)}={condition}"
        return out


class OrderOption(StrEnum):
    DESC = "desc"
    ASC = "asc"


class Order(BaseQueryStrEnum):
    ORDERBY = "orderBy"

    def __call__(self, property: Properties, option: OrderOption) -> str:
        out: str = f"{str(self)}={property} {option}"
        return out
