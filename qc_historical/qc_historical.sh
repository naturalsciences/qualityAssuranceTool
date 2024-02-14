#!/usr/bin/env bash

# example usage
# ./qc_historical.sh "2023-05-05 00:00" "2023-05-05 02:00" "10minutes" >> qc_historical.log 2>&1

START="$1"
END="$2"
DT="$3"

source ./env_hist

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

    END_I=$(date -u --date="$START_I UTC +10minutes" "+%Y-%m-%d %H:%M")

    docker run \
        -it --network=host --user "$(id -u):$(id -g)"\
        --workdir /app \
        -v "$CONFIG_FOLDER":/app/conf \
        -v "$OUTPUT_FOLDER":/app/outputs \
        -e DEV_SENSORS_USER="$SENSORS_USER" \
        -e DEV_SENSORS_PASS="$SENSORS_PASS" \
        nbmdc/quality_assurance_tool:v0.2 \
        "time.start=$START_I" "time.end=$END_I"
    START_I=$END_I
 
done