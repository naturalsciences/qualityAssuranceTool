import pytest

from models.enums import (Entities, Filter, Order, OrderOption, Properties,
                          Qactions, Settings)


class TestEnums:
    def test_feature_repr(self):
        featureOfInterest = f"{Entities.FEATUREOFINTEREST}"
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

    def test_settings_call_argument(self):
        out = Settings.TOP(10)
        assert out == "$top=10"

    def test_settings_call_argument_none(self):
        out = Settings.TOP(None)
        assert out == ""

    def test_settings_call_empty_argument_none(self):
        out = Settings.TOP()
        assert out == ""
