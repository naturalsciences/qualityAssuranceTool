import pytest
import main as m

 
class TestEnums:
    def test_feature_repr(self):
        featureOfInterest = f"{m.Entities.FEATUREOFINTEREST}"
        assert featureOfInterest == "FeatureOfInterest"

    def test_feature_call(self):
        out = m.Entities.FEATUREOFINTEREST([m.Properties.IOT_ID, m.Properties.NAME])
        assert out == "FeatureOfInterest(@iot.id;name)"

    def test_expand_call(self):
        out = m.Qactions.EXPAND([m.Properties.IOT_ID, m.Properties.NAME])
        assert out == "$expand=@iot.id,name"

    def test_filter_call(self):
        out = m.Filter.FILTER(f"{m.Properties.IOT_ID}>10")
        assert out == "$filter=@iot.id>10"

    def test_order_call(self):
        out = m.Order.ORDERBY(m.Properties.PHENOMENONTIME, m.OrderOption.DESC)
        assert out == "$orderBy=phenomenonTime desc"
