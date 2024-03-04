import pytest

from models.enums import (Entities, Filter, Order, OrderOption, Properties,
                          Qactions, Query, Settings, Entity)


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
    def test_class_creation(self):
        thing_1 = Entity(Entities.THING)
        q_out0 = Query(base_url="http://testing.be", root_entity=Entities.THING)

        thing_1.id = 5
        thing_1.selection = [Entities.DATASTREAMS, Properties.DESCRIPTION]
        thing_1.expand = [Entities.OBSERVATIONS]

        assert Qactions.SELECT(Query.selection_to_list(thing_1)) == "$select=Datastreams,description"

        ds0 = Entity(Entities.DATASTREAMS)
        ds0.settings = [Settings.SKIP(2)]
        assert Query.settings_to_list(ds0) == ["$skip=2"]

        obs0 = Entity(Entities.OBSERVATIONS)
        obs0.filter = "result gt 0.6"
        assert Filter.FILTER(Query.filter_to_str(obs0)) == "$filter=result gt 0.6"
        obs0.filter = "phenomenonTime gt 2023-01-02"
        assert Filter.FILTER(Query.filter_to_str(obs0)) == "$filter=result gt 0.6 and phenomenonTime gt 2023-01-02"

        assert Qactions.EXPAND(Query.expand_to_list(thing_1)) == "$expand=Observations"
        thing_1.expand = [obs0]
        assert Qactions.EXPAND(Query.expand_to_list(thing_1)) == "$expand=Observations($filter=result gt 0.6 and phenomenonTime gt 2023-01-02)"
        # # assert q_out0.build() == "http://testing.be/Thing"
        # # thing_1.id = 5
        # # q_out1 = Query(base_url="http://testing.be", root_entity=thing_1)
        # # assert q_out1.build() == "http://testing.be/Thing(5)"
        # # thing_1.selection = [Entities.DATASTREAMS, Properties.DESCRIPTION]
        # # assert (q_out1.build() == "http://testing.be/Thing(5)"
        # #         "?$select=Datastreams,description")

        # # ds0 = Entities.DATASTREAMS
        # # ds0.settings = [Settings.SKIP(2)]
        # # assert Query.build_entity(ds0) == "Datastreams($skip=2)"

        # # obs0 = Entities.OBSERVATIONS
        # # ds0.expand = [obs0]
        # # # assert Query.build_entity(ds0) == "Datastreams($skip=2;$expand=Observations)"
        # # obs0.filter = "phenomenonTime gt 2023-01-02"
        # # assert Query.build_entity(obs0) == "Observations($filter=phenomenonTime gt 2023-01-02)"
        # # obs0.filter = "result gt 0.6"
        # # assert Query.build_entity(obs0) == "Observations($filter=phenomenonTime gt 2023-01-02 and result gt 0.6)"
        # # thing_1.expand = [Entities.OBSERVATIONS]

        # # assert ";".join(Query.expansion(thing_1.expand)) == "Observations($filter=phenomenonTime gt 2023-01-02 and result gt 0.6)"
        # # # assert Query.build_entity(thing_1) == "&$expand=Datastreams($skip=2;"
        # # # "$expand=Observations($filter=phenomenonTime gt 2023-01-02;$count=True);"


