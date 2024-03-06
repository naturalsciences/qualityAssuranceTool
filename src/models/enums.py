from __future__ import annotations

import configparser
import logging
import time
from dataclasses import dataclass, field
from functools import wraps
from typing import List

import requests
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


class Query:
    def __init__(self, base_url: str, root_entity: Entities | Entity):
        self.base_url = base_url
        if isinstance(root_entity, Entities):
            self.root_entity = Entity(type=root_entity)
        else:
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
                out_i = None
                if isinstance(ei, Entity):
                    out_i = ei.type(
                        [Filter.FILTER(Query.filter_to_str(ei))]
                        + Query.settings_to_list(ei)
                        + [Qactions.EXPAND(Query.expand_to_list(ei))]
                        + [Qactions.SELECT(Query.selection_to_list(ei))]
                    )
                else:
                    out_i = ei
                out.append(out_i)

        return list(out)

    def get_with_retry(self):
        """
        This method retries to fetch data from the specified path according to the retry parameters
        :param path: the path which should be opened
        """
        return get_with_retry(self.build())

    def build(self):
        out_list = [
            Filter.FILTER(Query.filter_to_str(self.root_entity)),
            Query.settings_to_list(self.root_entity),
            Qactions.SELECT(Query.selection_to_list(self.root_entity)),
            Qactions.EXPAND(Query.expand_to_list(self.root_entity)),
        ]
        out_list = list(filter(None, out_list))
        out = f"{self.base_url.strip('/')}/{self.root_entity()}"
        if out_list:
            out += "?"
            out += "&".join(out_list)

        return out


@dataclass
class Entity:
    type: Entities
    id: int | None = None
    selection: List[Entities | Properties | None] = field(default_factory=list)
    settings: List[str | None] = field(default_factory=list)
    expand: List[Entity | Entities | None] = field(default_factory=list)
    filters: List[str | None] = field(default_factory=list)

    def __call__(self) -> str:
        out = f"{self.type}"
        if self.id:
            out += f"({self.id})"
        return out

    @property
    def filter(self) -> List[str | None]:
        return self.filters

    @filter.setter
    def filter(self, filter_i) -> None:
        self.filters += [filter_i]

    # @property
    # def expand(self) -> List[Entity | Entities | None]:
    #     return self.expand

    # @expand.setter
    # def expand(self, list_values):
    #     self.expand = list(list_values)


FILENAME = ".staconf.ini"


class Config:
    """
    This class allows to store and load settings that are relevant for stapy
    Therefore one does not need to pass this arguments each time stapy is used
    """

    def __init__(self, filename=None):
        self.filename = filename
        if filename is None:
            self.filename = FILENAME
        self.config = configparser.ConfigParser()
        self.read()

    def read(self):
        self.config.read(self.filename)  # type: ignore

    def save(self):
        with open(self.filename, "w") as configfile:  # type: ignore
            self.config.write(configfile)

    def get(self, arg):
        try:
            return self.config["DEFAULT"][arg]
        except KeyError:
            return None

    def set(self, **kwargs):
        for k, v in kwargs.items():
            self.config["DEFAULT"][k] = str(v)

    def remove(self, arg):
        try:
            return self.config.remove_option("DEFAULT", arg)
        except NoSectionError:  # type: ignore
            return False

    def load_sta_url(self):
        sta_url = self.get("STA_URL")
        if sta_url is None:
            log.critical(
                "The key (STA_URL) does not exist in the config file set the url first"
            )
            return ""
        return sta_url

    def load_authentication(self):
        sta_usr = self.get("STA_USR")
        sta_pwd = self.get("STA_PWD")
        if sta_usr is None or sta_pwd is None:
            log.debug("Sending the request without credentials")
            return None
        else:
            log.debug("Sending the request without credentials")
            return requests.auth.HTTPBasicAuth(sta_usr, sta_pwd)  # type: ignore


config = Config()


def set_sta_url(sta_url):
    if not isinstance(sta_url, str):
        logging.critical("The provided url (" + str(sta_url) + ") is not valid")
        return
    if not sta_url.endswith("/"):
        sta_url = sta_url + "/"
    config.set(STA_URL=sta_url)
    config.save()


@retry(requests.HTTPError, tries=5, delay=1, backoff=2)
def get_with_retry(query: str):
    """
    This method retries to fetch data from the specified path according to the retry parameters
    :param path: the path which should be opened
    """
    auth = config.load_authentication()
    return requests.get(query, auth=auth)