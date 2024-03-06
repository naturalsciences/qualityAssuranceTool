import pytest

from models.enums import (
    Entities,
    Filter,
    Order,
    OrderOption,
    Properties,
    Qactions,
    Settings,
)
from services.requests import Entity, Query


class TestEnums:
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

    def test_settings_call_argument(self):
        out = Settings.TOP(10)
        assert out == "$top=10"

    def test_settings_call_argument_none(self):
        out = Settings.TOP(None)
        assert out == ""

    def test_settings_call_empty_argument_none(self):
        out = Settings.TOP()
        assert out == ""


class TestQuery:
    def test_class_creation_base_query(self):
        thing_1 = Entity(Entities.THINGS)

        thing_1.id = 5

        q_out1 = Query(base_url="http://testing.be", root_entity=thing_1)
        q1 = q_out1.build()
        assert q1 == "http://testing.be/Things(5)"

    def test_class_creation_select(self):
        thing_1 = Entity(Entities.THINGS)
        thing_1.id = 5

        thing_1.selection = [Entities.DATASTREAMS, Properties.DESCRIPTION]
        thing_1.expand = [Entities.OBSERVATIONS]

        assert (
            Qactions.SELECT(Query.selection_to_list(thing_1))
            == "$select=Datastreams,description"
        )

    def test_class_creation_settings(self):
        ds0 = Entity(Entities.DATASTREAMS)
        ds0.settings = [Settings.SKIP(2)]
        assert Query.settings_to_list(ds0) == ["$skip=2"]

    def test_class_creation_filter(self):
        obs0 = Entity(Entities.OBSERVATIONS)
        obs0.filter = "result gt 0.6"
        assert Filter.FILTER(Query.filter_to_str(obs0)) == "$filter=result gt 0.6"
        obs0.filter = "phenomenonTime gt 2023-01-02"
        assert (
            Filter.FILTER(Query.filter_to_str(obs0))
            == "$filter=result gt 0.6 and phenomenonTime gt 2023-01-02"
        )

    def test_class_creation_expand(self):

        thing_1 = Entity(Entities.THINGS)
        thing_1.id = 5
        thing_1.expand = [Entities.OBSERVATIONS]
        assert Qactions.EXPAND(Query.expand_to_list(thing_1)) == "$expand=Observations"

    def test_class_creation_expand_nested(self):
        obs0 = Entity(Entities.OBSERVATIONS)
        obs0.filter = "result gt 0.6"
        obs0.filter = "phenomenonTime gt 2023-01-02"

        thing_1 = Entity(Entities.THINGS)
        thing_1.id = 5

        thing_1.expand = [obs0]
        assert (
            Qactions.EXPAND(Query.expand_to_list(thing_1))
            == "$expand=Observations($filter=result gt 0.6 and phenomenonTime gt 2023-01-02)"
        )
