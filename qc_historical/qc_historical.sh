#!/usr/bin/env bash

# example usage
# ./qc_historical.sh "2023-05-05 00:00" "2023-05-05 02:00" "10minutes" "1">> qc_historical.log 2>&1

START="$1"
END="$2"
DT="$3"
DT_OVERLAP=$(("$4"))

source ./env_hist

DT_INT=$(echo $DT | tr -dc '0-9')
DT_INT=$(($DT_INT))
DT_UNIT=$(printf '%s\n' "${DT//[[:digit:]]/}")

START_SECONDS=$(date -u --date="$START" +%s)
END_SECONDS=$(date -u --date="$END" +%s)

START_I=$START

SENSORS_USER=$(keyctl print $(keyctl search $(keyctl get_persistent @u) user SENSORS_USER))
SENSORS_PASS=$(keyctl print $(keyctl search $(keyctl get_persistent @u) user SENSORS_PASS))

while [[ $(date -u --date="$START_I UTC" "+%s") < $END_SECONDS ]]
do
    START_SECONDS=$(date -u --date="$START UTC" +%s)

    CONFIG_FOLDER=$CONFIG_FOLDER_HIST
    OUTPUT_FOLDER=$OUTPUT_FOLDER_HIST

    START_II=$(date -u --date="$START_I UTC +""$((-DT_OVERLAP))""$DT_UNIT" "+%Y-%m-%d %H:%M")

    END_I=$(date -u --date="$START_I UTC +$((DT_INT))"$DT_UNI" "+%Y-%m-%d %H:%M")

    docker run \
        -it --network=host --user "$(id -u):$(id -g)"\
        --workdir /app \
        -v "$CONFIG_FOLDER":/app/conf \
        -v "$OUTPUT_FOLDER":/app/outputs \
        -e DEV_SENSORS_USER="$SENSORS_USER" \
        -e DEV_SENSORS_PASS="$SENSORS_PASS" \
        nbmdc/quality_assurance_tool:v0.3 \
        "time.start=$START_II" "time.end=$END_I"
    START_I=$END_I
 
done