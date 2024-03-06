from __future__ import annotations

import logging
import time
from functools import wraps

from ordered_enum.ordered_enum import OrderedEnum
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


def retry(exception_to_check, tries=4, delay=3, backoff=2):
    """Retry calling the decorated function using an exponential backoff.

    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    :param exception_to_check: the exception to check. may be a tuple of
        exceptions to check
    :type exception_to_check: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int
    """

    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except exception_to_check as e:
                    msg = "%s, Retrying in %d seconds..." % (str(e), mdelay)
                    logging.info(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry




