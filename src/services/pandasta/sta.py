from __future__ import annotations

from datetime import datetime
import logging

from strenum import StrEnum

from services.pandasta.logging_constants import ISO_STR_FORMAT, ISO_STR_FORMAT2
# from services.pandasta.requests import log

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


# not used, keep as reference
# def extend_summary_with_result_inspection(summary_dict: dict[str, list]):
#     log.debug(f"Start extending summary.")
#     summary_out = copy.deepcopy(summary_dict)
#     nb_streams = len(summary_out.get(Entities.DATASTREAMS, []))
#     for i, dsi in enumerate(summary_dict.get(Entities.DATASTREAMS, [])):
#         log.debug(f"Start extending datastream {i+1}/{nb_streams}.")
#         iot_id_list = summary_dict.get(Entities.DATASTREAMS, []).get(dsi).get(Properties.iot_id)  # type: ignore
#         results = np.empty(0)
#         for iot_id_i in iot_id_list:
#             results_ = (
#                 Query(Entity.Datastream)
#                 .entity_id(iot_id_i)
#                 .sub_entity(Entity.Observation)
#                 .select(Properties.RESULT)
#                 .get_data_sets()
#             )
#             results = np.concatenate([results, results_])
#         min = np.min(results)
#         max = np.max(results)
#         mean = np.mean(results)
#         median = np.median(results)
#         nb = np.shape(results)[0]
# 
#         extended_sumary = {
#             "min": min,
#             "max": max,
#             "mean": mean,
#             "median": median,
#             "nb": nb,
#         }
#         summary_out.get(Entities.DATASTREAMS).get(dsi)[Properties.RESULT] = extended_sumary  # type: ignore
#     return summary_out


# def get_date_from_string(
#     str_in: str, str_format_in: str = "%Y-%m-%d %H:%M", str_format_out: str = "%Y%m%d"
# ) -> str:
#     date_out = datetime.strptime(str(str_in), str_format_in)
#     return date_out.strftime(str_format_out)


def convert_to_datetime(value: str) -> datetime:
    try:
        d_out = datetime.strptime(value, ISO_STR_FORMAT)
        return d_out
    except ValueError as e:
        try:
            d_out = datetime.strptime(value, ISO_STR_FORMAT2)
            return d_out
        except Exception as e:
            log.exception(e)
            raise e
