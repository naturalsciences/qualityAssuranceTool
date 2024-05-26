#!/usr/bin/env bash

IMAGE_TAG="v0.7.0"
CONFIG_NAME="config.yaml"

# 

# Function to display usage
usage() {
    echo "Usage: $0 -s START -e END -d total_time_window -o time_window_overlap [-i IMAGE_TAG] [-c CONFIG_NAME]"
    exit 1
}

# Parse arguments
while getopts ":s:e:d:o:i:c:" opt; do
    case $opt in
        s) START="$OPTARG"
        ;;
        e) END="$OPTARG"
        ;;
        d) TOTAL_TIME_WINDOW="$OPTARG"
        ;;
        o) WINDOW_OVERLAP="$OPTARG"
        ;;
        i) IMAGE_TAG="$OPTARG"
        ;;
        c) CONFIG_NAME="$OPTARG"
        ;;
        \?) echo "Invalid option -$OPTARG" >&2
            usage
        ;;
        :) echo "Option -$OPTARG requires an argument." >&2
           usage
        ;;
    esac
done

# Check mandatory parameters
if [ -z "$START" ] || [ -z "$END" ] || [ -z "$TOTAL_TIME_WINDOW" ] || [ -z "$WINDOW_OVERLAP" ]; then
    usage
fi

source ./env_hist
source ./.env

TIME_WINDOW_INT=$(echo $TOTAL_TIME_WINDOW | tr -dc '0-9')
TIME_WINDOW_INT=$(($TIME_WINDOW_INT))
TIME_WINDOW_UNIT=$(printf '%s\n' "$TOTAL_TIME_WINDOW//[[:digit:]]/}")

START_SECONDS=$(date -u --date="$START" +%s)
END_SECONDS=$(date -u --date="$END" +%s)

START_I=$START

while [[ $(date -u --date="$START_I UTC" "+%s") < $END_SECONDS ]]
do
    START_SECONDS=$(date -u --date="$START UTC" +%s)

    CONFIG_FOLDER=$CONFIG_FOLDER_HIST
    OUTPUT_FOLDER=$OUTPUT_FOLDER_HIST

    START_II=$(date -u --date="$START_I UTC +""$((-WINDOW_OVERLAP))""$DT_UNIT" "+%Y-%m-%d %H:%M")

    END_I=$(date -u --date="$START_I UTC" "+%Y-%m-%d %H:%M")

    docker run \
        -it --rm --network=host --user "$(id -u):$(id -g)"\
        --workdir /app \
        -v "$CONFIG_FOLDER":/app/conf \
        -v "$OUTPUT_FOLDER":/app/outputs \
        -e DEV_SENSORS_USER="$SENSORS_USER" \
        -e DEV_SENSORS_PASS="$SENSORS_PASS" \
        rbinsbmdc/quality_assurance_tool:$IMAGE_TAG \
        "--config-name" $CONFIG_NAME \
        "time.start=$START_II" "time.end=$END_I"

    START_I=$END_I
 
done
