home="/home/RBINS.BE/bmdc/belgica_data_transfer"
export PGPASSFILE="/home/RBINS.BE/bmdc/.pgpass"

PGHOST=postgres01_bmdc.rbins.be
PGHOSTPROD=postgres02_bmdc.rbins.be

PGDBMDM=mdm5
PGMDMUSER=stauser

PGDBSTA=sensorthings_11BU
PGSTAUSER=stauser

PGDBSTAPROD=sensorthings_prod
PGSTAPRODUSER=stauser

## 4. Multidatastream creation
#
echo $(date +%Y-%m-%d' '%H:%M:%S) "Creation of the multidatastream"

	psql -X -h $PGHOST -p 5432 -U $PGSTAUSER -d $PGDBSTA \
	-c "select bmdc.observations_populate_mds(1,1,600,'$startdate','$enddate');" \
	-c "select bmdc.observations_populate_mds(1,12,600,'$startdate','$enddate');"

	psql_exit_status=$?

	if [ $psql_exit_status != 0 ]; then
		echo -e "The creation of the multidatastream has failed for the data between '$startdate' and '$enddate'."
	fi


# 5. Sub-sample data and move to the production server
echo $(date +%Y-%m-%d' '%H:%M:%S) "Sub-sampling data and moving to production server"


#Starting data download from source database
echo  $(date +%Y-%m-%d' '%H:%M:%S) 'Downloading data from sensorthings database between' $startdate 'and' $enddate
psql -h $PGHOST -p 5432 -U $PGSTAUSER -d $PGDBSTA \
        -c "\copy (select * from \"SENSORS\") to '$home/data/mdm5_raw_data/$temp_dir/sensors.csv' csv header;"  \
        -c "\copy (select * from \"DATASTREAMS\") to '$home/data/mdm5_raw_data/$temp_dir/datastreams.csv' csv header;" \
	-c "\copy (with ID as (select time_bucket('10 minutes', \"PHENOMENON_TIME_START\") as time10min, o.\"DATASTREAM_ID\", first(o.\"ID\", o.\"PHENOMENON_TIME_START\") filter(where o.\"RESULT_NUMBER\" is not null) as id from \"OBSERVATIONS\" o where o.\"PHENOMENON_TIME_START\" between '$startdate' and '$enddate' group by time10min, o.\"DATASTREAM_ID\") select * from \"FEATURES\" f where f.\"ID\" in (select o.\"FEATURE_ID\" from \"OBSERVATIONS\" o where o.\"ID\" in (select id from ID))) to '$home/data/mdm5_raw_data/$temp_dir/features.csv' csv header;" \
	-c "\copy (with ID as (select o.\"FEATURE_ID\" as id from \"OBSERVATIONS\" o where o.\"MULTI_DATASTREAM_ID\" is not null and o.\"PHENOMENON_TIME_START\" between '$startdate' and '$enddate') select * from \"FEATURES\" f where f.\"ID\" in (select id from ID)) to '$home/data/mdm5_raw_data/$temp_dir/mds_features.csv' csv header;" \
	-c "\copy (select hl.* from \"HIST_LOCATIONS\" hl where hl.\"TIME\" between '$startdate' and '$enddate') to '$home/data/mdm5_raw_data/$temp_dir/hist_locations.csv' csv header;" \
	-c "\copy (select l.* from \"LOCATIONS\" l left join \"LOCATIONS_HIST_LOCATIONS\" lhl on lhl.\"LOCATION_ID\" = l.\"ID\" left join \"HIST_LOCATIONS\" hl on hl.\"ID\" = lhl.\"HIST_LOCATION_ID\" where hl.\"TIME\" between '$startdate' and '$enddate') to '$home/data/mdm5_raw_data/$temp_dir/locations.csv' csv header;" \
	-c "\copy (select lhl.* from \"LOCATIONS_HIST_LOCATIONS\" lhl left join \"HIST_LOCATIONS\" hl on hl.\"ID\" = lhl.\"HIST_LOCATION_ID\" where hl.\"TIME\" between '$startdate' and '$enddate') to '$home/data/mdm5_raw_data/$temp_dir/loc_hist_loc.csv' csv header;"\
	-c "\copy (with ID as (select time_bucket('10 minutes', \"PHENOMENON_TIME_START\") as time10min, o.\"DATASTREAM_ID\", first(o.\"ID\", o.\"PHENOMENON_TIME_START\") filter(where o.\"RESULT_NUMBER\" is not null) as id from \"OBSERVATIONS\" o where o.\"PHENOMENON_TIME_START\" between '$startdate' and '$enddate' group by time10min, o.\"DATASTREAM_ID\") select * from \"OBSERVATIONS\" o where o.\"ID\" in (select id from ID)) to '$home/data/mdm5_raw_data/$temp_dir/data.csv' csv header;"\
	-c "\copy (select * from \"OBSERVATIONS\" o where o.\"PHENOMENON_TIME_START\" between '$startdate' and '$enddate' and \"MULTI_DATASTREAM_ID\" is not null) to '$home/data/mdm5_raw_data/$temp_dir/mds_data.csv' csv header;" \
        -c "\copy (select * from \"THINGS_LOCATIONS\") to '$home/data/mdm5_raw_data/$temp_dir/things_locations.csv' csv header;"

psql_exit_status=$?

	if [ $psql_exit_status != 0 ]; then
		echo "The subsampling of the data has failed with exit code '$psql_exit_status'"
	fi

# Start importing data in the target database
echo $(date +%Y-%m-%d' '%H:%M:%S) 'Importing data to target database.'

psql -h $PGHOSTPROD -p 5432 -U $PGSTAUSER -d $PGDBSTAPROD \
 	-c "CREATE TEMPORARY TABLE temp_sensors as (SELECT * FROM \"SENSORS\" LIMIT 0);" \
    	-c "\copy temp_sensors (\"NAME\", \"DESCRIPTION\", \"PROPERTIES\", \"ENCODING_TYPE\", \"METADATA\", \"ID\") FROM '$home/data/mdm5_raw_data/$temp_dir/sensors.csv' csv header;" \
	-c "INSERT INTO  \"SENSORS\" (\"NAME\", \"DESCRIPTION\", \"PROPERTIES\", \"ENCODING_TYPE\", \"METADATA\", \"ID\") SELECT \"NAME\", \"DESCRIPTION\", \"PROPERTIES\", \"ENCODING_TYPE\", \"METADATA\", \"ID\" from temp_sensors ON CONFLICT DO NOTHING;" \
	-c "CREATE TEMPORARY TABLE temp_datastream as (SELECT * FROM \"DATASTREAMS\" LIMIT 0);" \
	-c "\copy temp_datastream (\"NAME\", \"DESCRIPTION\", \"PROPERTIES\", \"OBSERVATION_TYPE\", \"PHENOMENON_TIME_START\", \"PHENOMENON_TIME_END\", \"RESULT_TIME_START\", \"RESULT_TIME_END\", \"OBSERVED_AREA\", \"SENSOR_ID\", \"OBS_PROPERTY_ID\", \"THING_ID\", \"UNIT_NAME\", \"UNIT_SYMBOL\", \"UNIT_DEFINITION\" , \"LAST_FOI_ID\", \"ID\") FROM '$home/data/mdm5_raw_data/$temp_dir/datastreams.csv' csv header;"\
	-c "INSERT INTO \"DATASTREAMS\" (\"ID\", \"DESCRIPTION\", \"OBSERVATION_TYPE\", \"PHENOMENON_TIME_START\", \"PHENOMENON_TIME_END\", \"RESULT_START_TIME\", \"RESULT_TIME_END\", \"SENSOR_ID\", \"OBS_PROPERTY_ID\", \"THING_ID\", \"UNIT_NAME\", \"UNIT_SYMBOL\", \"UNIT_DEFINITION\", \"NAME\", \"OBSERVED_AREA\", \"PROPERTIES\", \"LAST_FOI_ID\") select \"ID\", \"DESCRIPTION\", \"OBSERVATION_TYPE\", \"PHENOMENON_TIME_START\", \"PHENOMENON_TIME_END\", \"RESULT_TIME_START\", \"RESULT_TIME_END\", \"SENSOR_ID\", \"OBS_PROPERTY_ID\", \"THING_ID\", \"UNIT_NAME\", \"UNIT_SYMBOL\", \"UNIT_DEFINITION\", \"NAME\", \"OBSERVED_AREA\", \"PROPERTIES\", \"LAST_FOI_ID\" from temp_datastream ON CONFLICT DO NOTHING;" \
	-c "CREATE TEMPORARY TABLE temp_feature as (SELECT * FROM \"FEATURES\" LIMIT 0);" \
	-c "\copy temp_feature (\"NAME\", \"DESCRIPTION\", \"PROPERTIES\", \"ENCODING_TYPE\", \"FEATURE\", \"GEOM\", \"ID\") FROM '$home/data/mdm5_raw_data/$temp_dir/features.csv' csv header;" \
        -c "\copy temp_feature (\"NAME\", \"DESCRIPTION\", \"PROPERTIES\", \"ENCODING_TYPE\", \"FEATURE\", \"GEOM\", \"ID\") FROM '$home/data/mdm5_raw_data/$temp_dir/mds_features.csv' csv header;" \
	-c "INSERT INTO \"FEATURES\" (\"NAME\", \"DESCRIPTION\", \"PROPERTIES\", \"ENCODING_TYPE\", \"FEATURE\", \"GEOM\", \"ID\") SELECT \"NAME\", \"DESCRIPTION\", \"PROPERTIES\", \"ENCODING_TYPE\", \"FEATURE\", \"GEOM\", \"ID\" FROM temp_feature ON CONFLICT DO NOTHING;" \
	-c "CREATE TEMPORARY TABLE temp_hist_loc as (SELECT * FROM \"HIST_LOCATIONS\" LIMIT 0);" \
	-c "\copy temp_hist_loc (\"TIME\", \"THING_ID\", \"ID\") FROM '$home/data/mdm5_raw_data/$temp_dir/hist_locations.csv' csv header;" \
	-c "INSERT INTO \"HIST_LOCATIONS\"(\"TIME\", \"THING_ID\", \"ID\") SELECT \"TIME\", \"THING_ID\", \"ID\" FROM temp_hist_loc ON CONFLICT DO NOTHING;" \
	-c "CREATE TEMPORARY TABLE temp_loc as (SELECT * FROM \"LOCATIONS\" LIMIT 0);" \
	-c "\copy temp_loc (\"NAME\", \"DESCRIPTION\", \"PROPERTIES\", \"ENCODING_TYPE\", \"LOCATION\", \"GEOM\", \"GEN_FOI_ID\", \"ID\") FROM '$home/data/mdm5_raw_data/$temp_dir/locations.csv' csv header;" \
	-c "INSERT INTO \"LOCATIONS\" (\"NAME\", \"DESCRIPTION\", \"PROPERTIES\", \"ENCODING_TYPE\", \"LOCATION\", \"GEOM\", \"GEN_FOI_ID\", \"ID\") SELECT \"NAME\", \"DESCRIPTION\", \"PROPERTIES\", \"ENCODING_TYPE\", \"LOCATION\", \"GEOM\", \"GEN_FOI_ID\", \"ID\" FROM temp_loc ON CONFLICT DO NOTHING;" \
	-c "CREATE TEMPORARY TABLE temp_loc_hist_loc as (SELECT * FROM \"LOCATIONS_HIST_LOCATIONS\" LIMIT 0);" \
	-c "\copy temp_loc_hist_loc (\"LOCATION_ID\", \"HIST_LOCATION_ID\") FROM '$home/data/mdm5_raw_data/$temp_dir/loc_hist_loc.csv' csv header;" \
	-c "INSERT INTO \"LOCATIONS_HIST_LOCATIONS\"(\"LOCATION_ID\", \"HIST_LOCATION_ID\") SELECT \"LOCATION_ID\", \"HIST_LOCATION_ID\" FROM temp_loc_hist_loc ON CONFLICT DO NOTHING;" \
	-c "CREATE TEMPORARY TABLE temp_obs as (SELECT * FROM \"OBSERVATIONS\" LIMIT 0);" \
	-c "\copy temp_obs (\"PHENOMENON_TIME_START\",\"PHENOMENON_TIME_END\", \"RESULT_TIME\", \"RESULT_TYPE\", \"RESULT_NUMBER\", \"RESULT_BOOLEAN\", \"RESULT_JSON\", \"RESULT_STRING\", \"RESULT_QUALITY\", \"VALID_TIME_START\", \"VALID_TIME_END\", \"PARAMETERS\", \"DATASTREAM_ID\", \"FEATURE_ID\", \"ID\", \"MULTI_DATASTREAM_ID\") FROM '$home/data/mdm5_raw_data/$temp_dir/data.csv' csv header;" \
        -c "\copy temp_obs (\"PHENOMENON_TIME_START\",\"PHENOMENON_TIME_END\", \"RESULT_TIME\", \"RESULT_TYPE\", \"RESULT_NUMBER\", \"RESULT_BOOLEAN\", \"RESULT_JSON\", \"RESULT_STRING\", \"RESULT_QUALITY\", \"VALID_TIME_START\", \"VALID_TIME_END\", \"PARAMETERS\", \"DATASTREAM_ID\", \"FEATURE_ID\", \"ID\", \"MULTI_DATASTREAM_ID\") FROM '$home/data/mdm5_raw_data/$temp_dir/mds_data.csv' csv header;" \
	-c "INSERT INTO \"OBSERVATIONS\" (\"PHENOMENON_TIME_START\",\"PHENOMENON_TIME_END\", \"RESULT_TIME\", \"RESULT_TYPE\", \"RESULT_NUMBER\", \"RESULT_BOOLEAN\", \"RESULT_JSON\", \"RESULT_STRING\", \"RESULT_QUALITY\", \"VALID_TIME_START\", \"VALID_TIME_END\", \"PARAMETERS\", \"DATASTREAM_ID\", \"FEATURE_ID\", \"ID\", \"MULTI_DATASTREAM_ID\") SELECT \"PHENOMENON_TIME_START\",\"PHENOMENON_TIME_END\", \"RESULT_TIME\", \"RESULT_TYPE\", \"RESULT_NUMBER\", \"RESULT_BOOLEAN\", \"RESULT_JSON\", \"RESULT_STRING\", \"RESULT_QUALITY\", \"VALID_TIME_START\", \"VALID_TIME_END\", \"PARAMETERS\", \"DATASTREAM_ID\", \"FEATURE_ID\", \"ID\", \"MULTI_DATASTREAM_ID\" FROM temp_obs ON CONFLICT DO NOTHING;" \
        -c "CREATE TEMPORARY TABLE temp_things_locations as (SELECT * FROM \"THINGS_LOCATIONS\" LIMIT 0);" \
        -c "\copy temp_things_locations (\"THING_ID\", \"LOCATION_ID\") FROM '$home/data/mdm5_raw_data/$temp_dir/things_locations.csv' csv header;" \
        -c "UPDATE \"THINGS_LOCATIONS\" TL SET \"LOCATION_ID\" = TTL.\"LOCATION_ID\" FROM temp_things_locations TTL WHERE TTL.\"THING_ID\" = TL.\"THING_ID\";"


psql_exit_status=$?

	if [ $psql_exit_status != 0 ]; then
		echo "The import of the data in the production database has failed with exit code '$psql_exit_status'"

		for file in $(ls $home/data/subsampling)
        		do
                		mv $home/data/subsampling/$file $home/data/failed_subsampling/${now}_$file
        		done


	fi

