from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Dict, List

from ordered_enum.ordered_enum import OrderedEnum
from strenum import StrEnum


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
    THING = "Thing"

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
        if isinstance(arg, list):
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


class QualityFlags(OrderedEnum):
    """
    http://vocab.nerc.ac.uk/collection/L20/current/

    Args:
        OrderedEnum (_type_): _description_

    Returns:
        _type_: _description_
    """

    NO_QUALITY_CONTROL = 0
    GOOD = 1
    PROBABLY_GOOD = 2
    PROBABLY_BAD = 3
    CHANGED = 5
    BELOW_detection = 6
    IN_EXCESS = 7
    INTERPOLATED = 8
    MISSING = 9
    PHENOMENON_UNCERTAIN = "A"
    NOMINAL = "B"
    BELOW_LIMIT_OF_QUANTIFICATION = "Q"
    BAD = 4

    def __str__(self):
        return f"{self.value}"


class Df(StrEnum):
    IOT_ID = Properties.IOT_ID
    DATASTREAM_ID = "datastream_id"
    UNITS = "units"
    OBSERVATION_TYPE = "observation_type"
    QC_FLAG = Properties.QC_FLAG
    GRADIENT = "gradient"
    TIME = "phenomenonTime"
    RESULT = Properties.RESULT
    REGION = "Region"
    SUB_REGION = "Sub-region"
    LONG = "long"
    LAT = "lat"
    OBSERVED_PROPERTY_ID = "observed_property_id"
    FEATURE_ID = "feature_id"


class Query:
    def __init__(self, base_url: str, root_entity: Entities):
        self.base_url = base_url
        self.root_entity = root_entity

    @staticmethod
    def selection_to_list(entity):
        out = []
        for si in entity.selection:
            out.append(si)
        return out

    @staticmethod
    def filter_to_str(entity):
        out = ""
        if entity:
            out = " and ".join(entity.filters)
        return out

    @staticmethod
    def settings_to_list(entity):
        out = []
        for si in entity.settings:
            out.append(si)
        return out

    @staticmethod
    def expand_to_list(entity):
        out = []
        if entity.expand:
            for ei in entity.expand:
                if isinstance(ei, Entity):
                    out.append(
                        ei.type(
                            [Filter.FILTER(Query.filter_to_str(ei))]
                            + Query.settings_to_list(ei)
                            + Query.selection_to_list(ei)
                            + Query.expand_to_list(ei)
                        )
                    )
                else:
                    out.append(ei)
        return out
    
    
    def build(self):
        out = self.base_url + self.root_entity


@dataclass
class Entity:
    type: Entities
    id: int | None = None
    selection: List[Entities | Properties | None] = field(default_factory=list)
    settings: List[str | None] = field(default_factory=list)
    expand: List[Entity | Entities | Properties | None] = field(default_factory=list)
    filters: List[str | None] = field(default_factory=list)

    @property
    def filter(self):
        return self.filters

    @filter.setter
    def filter(self, filter_i):
        self.filters += [filter_i]
