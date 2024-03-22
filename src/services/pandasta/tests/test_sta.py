import pytest

from services.pandasta.sta_requests import update_response
from services.pandasta.sta import (Entities, Filter, Order, OrderOption,
                                   Properties, Qactions, Settings,
                                   convert_to_datetime)


class TestSta:
    def test_feature_str(self):
        featureOfInterest = f"{Entities.FEATUREOFINTEREST}"
        assert featureOfInterest == "FeatureOfInterest"

    def test_feature_repr(self):
        featureOfInterest = f"{Entities.FEATUREOFINTEREST.__repr__()}"
        assert featureOfInterest == "FeatureOfInterest"

    def test_feature_call(self):
        out = Entities.FEATUREOFINTEREST([Properties.IOT_ID, Properties.NAME])
        assert out == "FeatureOfInterest(@iot.id;name)"

    def test_expand_call(self):
        out = Qactions.EXPAND([Properties.IOT_ID, Properties.NAME])
        assert out == "$expand=@iot.id,name"

    def test_filter_call(self):
        out = Filter.FILTER(f"{Properties.IOT_ID}>10")
        assert out == "$filter=@iot.id>10"

    def test_order_call(self):
        out = Order.ORDERBY(Properties.PHENOMENONTIME, OrderOption.DESC)
        assert out == "$orderBy=phenomenonTime desc"

    def test_properties_repr(self):
        out = f"{Properties.DESCRIPTION.__repr__()}"
        assert out == "description"

    def test_settings_call_argument(self):
        out = Settings.TOP(10)
        assert out == "$top=10"

    def test_settings_call_argument_none(self):
        out = Settings.TOP(None)
        assert out == ""

    def test_settings_call_empty_argument_none(self):
        out = Settings.TOP()
        assert out == ""

    @pytest.mark.parametrize(
        "date_str,date_ref",
        [
            ("2023-01-02T13:14:15.00Z", "20230102131415"),
            ("2023-01-02T13:14:15.030Z", "20230102131415"),
            ("2023-01-02T13:14:15Z", "20230102131415"),
            ("2023-01-02T10:14:15Z", "20230102101415"),
        ],
    )
    def test_convert_to_datetime(self, date_str, date_ref):
        datetime_out = convert_to_datetime(date_str)
        assert datetime_out.strftime("%Y%m%d%H%M%S") == date_ref

    @pytest.mark.parametrize(
        "date_str",
        [
            "202301021314152",
            "2023-01-02T10PM:14:15.030Z",
            "2023-01-02T10:14:.030",
            "2023-01-02 10:14:50.030",
            "",
        ],
    )
    def test_convert_to_datetime_exception(self, date_str):
        with pytest.raises(Exception) as e:
            datetime_out = convert_to_datetime(date_str)
        assert (
            str(e.value)
            == f"time data '{date_str}' does not match format '%Y-%m-%dT%H:%M:%SZ'"
        )
    def test_update_response(self):
        d = {
            "one": "this",
            "two": "two",
            "three": "threeee",
            "four": "four",
            "list": list(range(5)),
        }
        update = {"one": "that", "two": "two", "list": list(range(5, 11))}
        d = update_response(d, update)

        ref = {
            "one": "that",
            "two": "two",
            "three": "threeee",
            "four": "four",
            "list": list(range(11)),
        }
        assert d == ref

