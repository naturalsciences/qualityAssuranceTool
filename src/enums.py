from enum import nonmember
from strenum import StrEnum


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


class Settings(BaseQueryStrEnum):
    TOP = "top"
    SKIP = "skip"
    COUNT = "count"

    def __call__(self, value):
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

    def __call__(self, property: BaseQueryStrEnum, option: str) -> str:
        option_ = OrderOption(option) 
        out: str = f"{str(self)}={property} {option_}"
        return out
