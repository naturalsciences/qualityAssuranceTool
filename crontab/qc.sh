#!/usr/bin/env bash

PATH=$PATH:/usr/bin

ROOT_DIR=$1

mkdir -p $ROOT_DIR/outputs
source $ROOT_DIR/crontab/env_qc_settings
source $ROOT_DIR/crontab/.env_sta
source $ROOT_DIR/crontab/timestamp_parsing.sh

cd $ROOT_DIR

DT_INT=$(echo $DT | tr -dc '0-9')
DT_INT=$(($DT_INT))
DT_UNIT=$(printf '%s\n' "${DT//[[:digit:]]/}")

FMT="+%Y-%m-%d %H:%M:%S"

END_I=$(date -u --date="$END_I UTC" "$FMT")
START_I=$(date -u --date="$END_I UTC -$((DT_INT+OVERLAP))$DT_UNIT" "$FMT")

TIMESTAMP_NOW=$(date --date now "$FMT")
echo -n "["$TIMESTAMP_NOW"] - "
CONTAINER_ID= $(docker run \
        -d --rm --network=host --user "$(id -u):$(id -g)"\
        --workdir /app \
        -v "$CONFIG_FOLDER":/app/conf \
        -v "$OUTPUT_FOLDER":/app/outputs \
        -e DEV_SENSORS_USER="$SENSORS_USER" \
        -e DEV_SENSORS_PASS="$SENSORS_PASS" \
        rbinsbmdc/quality_assurance_tool:$IMAGE_TAG \
        "time.start=$START_I" "time.end=$END_I")

STATUS_CODE_CONTAINER="$(docker container wait $CONTAINER_ID)"
TIMESTAMP_NOW=$(date --date now "$FMT")
echo -n "["$TIMESTAMP_NOW"] - "
echo "Status code $CONTAINER_ID: $STATUS_CODE_CONTAINER"

FMT_TRANSFER_SCRIPT="+%Y-%m-%d %H:%M:%S"
export startdate=$(date -u --date="$START_I UTC" "$FMT_TRANSFER_SCRIPT")
export enddate=$(date -u --date="$END_I UTC" "$FMT_TRANSFER_SCRIPT")

TIMESTAMP_NOW=$(date --date now "$FMT")
echo -n "["$TIMESTAMP_NOW"] - "
echo "Start production "

/usr/bin/env bash $ROOT_DIR/crontab/sta_raw_to_sta_prod_transfer\ 1.sh