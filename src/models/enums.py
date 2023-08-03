from enum import Enum

from strenum import StrEnum
from ordered_enum.ordered_enum import OrderedEnum


class BaseQueryStrEnum(StrEnum):
    def __str__(self):
        return f"${self.value}"


class Properties(StrEnum):
    DESCRIPTION = "description"
    UNITOFMEASUREMENT = "unitOfMeasurement/name"
    NAME = "name"
    IOT_ID = "@iot.id"
    COORDINATES = "feature/coordinates"
    PHENOMENONTIME = "phenomenonTime"
    RESULT = "result"
    QC_FLAG = "resultQuality"


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

    def __call__(self, args: list[Properties] | list["Qactions"] | list[str]):
        out = f"{self}({';'.join(list(filter(None, args)))})"
        return out

    def __repr__(self):
        return f"{self.value}"

    def __str__(self):
        return f"{self.value}"


class Qactions(BaseQueryStrEnum):
    EXPAND = "expand"
    SELECT = "select"
    ORDERBY = "orderby"

    def __call__(
        self, arg: Entities | Properties | list[Properties] | list[Entities] | list[str]
    ):
        out = ""
        if isinstance(arg, list):
            str_arg = ",".join(arg)
            out = f"{str(self)}={str_arg}"
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


class Df(StrEnum, OrderedEnum):
    IOT_ID = Properties.IOT_ID
    DATASTREAM_ID = "datastream_id"
    UNITS = "units"
    OBSERVATION_TYPE = "observation_type"
    QC_FLAG= Properties.QC_FLAG
    GRADIENT = "gradient"
    TIME = "phenomenonTime"
    RESULT = Properties.RESULT
    REGION = "Region"
    SUB_REGION = "Sub-region"
    LONG= "long"
    LAT = "lat"
    VERIFIED = "verified"
    VALID = "valid"
    OBSERVED_PROPERTY_ID = "observed_property_id"