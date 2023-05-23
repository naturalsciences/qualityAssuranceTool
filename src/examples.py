# https://sensors.naturalsciences.be/sta/v1.1/
# Things(1)?$select=name,@iot.id&$expand=Datastreams ($expand=ObservedProperty ($select=name,@iot.id);$select=name,@iot.id)
# https://sensors.naturalsciences.be/sta/v1.1/Things(1)?$select=name,@iot.id&$expand=Datastreams%20($select=unitOfMeasurement/name)
# https://sensors.naturalsciences.be/sta/v1.1/Things(1)?$expand=Datastreams%20($count=true)&$select=Datastreams/@iot.count

# out = dict()
# base_query = Query(Entity.Thing).entity_id(entity_id)
# out = base_query.select('name', 'Datastreams').expand('Datastreams'
#                                                       '($expand=ObservedProperty'
#                                                       '($select=name,@iot.id);$select=name,@iot.id,unitOfMeasurement/name'
#                                                       ')').get_data_sets()
# print(json.dumps(out, indent=4))
# working = "https://sensors.naturalsciences.be/sta/v1.1/Things(1)?$select=name,@iot.id,Datastreams&$expand=Datastreams%20($expand=ObservedProperty%20($select=name,@iot.id);$select=name,@iot.id)"
# print(f"{working}")
# a = base_query.select('name', '@iot.id').expand('Datastreams ($expand=ObservedProperty ($select=@iot.id);$select=name,@iot.id)')
# out = base_query.select('name', 'Datastreams').expand('Datastreams ($expand=ObservedProperty ($select=@iot.id);$select=name,@iot.id)'.get_data_sets()
# all shizzle
# out = base_query.select('name', 'Datastreams').expand('Datastreams'
#                                                       '($expand=Observations ($count=true)'
#                                                       ';$expand=ObservedProperty'
#                                                       '($select=name,@iot.id);$select=name,@iot.id,unitOfMeasurement/name'
#                                                       ')').get_data_sets()

# new all shizzle
# out_query = base_query.select('name', 'Datastreams')\
#     .expand('Datastreams'
#             '('
#             '$count=true;'
#             '$top=1;'
#             '$expand=ObservedProperty'
#                 '('
#                 '$select=name,@iot.id'
#                 ');'
#             '$select=name,@iot.id,unitOfMeasurement/name'
#              ')')
# out_query = base_query.select('name', 'Datastreams') \
#     .expand('Datastreams'
#             '('
#             '$count=true'
#             ';$expand=ObservedProperty'
#             '('
#             '$select=name,@iot.id'
#             ')'
#             ';$select=name,@iot.id,unitOfMeasurement/name'
#             ';$expand=Observations'
#             '('
#             '$count=true'
#             ';$select=@iot.id'
#             ';$top=0'
#             ')'
#             ')')


# sort results by size
# https://sensors.naturalsciences.be/sta/v1.1/Datastreams(7714)/Observations?$select=result&$orderby=result desc