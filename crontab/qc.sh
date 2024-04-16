#!/usr/bin/env bash

PATH=$PATH:/usr/bin

ROOT_DIR=$1
# ROOT_DIR=/var/quality_assurance_tool

source $ROOT_DIR/crontab/functions.sh

START_MESSAGE="Start QC script."

mkdir -p $ROOT_DIR/outputs
source $ROOT_DIR/crontab/env_qc_settings
source $ROOT_DIR/crontab/.env_sta

cd $ROOT_DIR

# get timestamp latest execution QC

QC_LOG=$ROOT_DIR/cron_out
PATTERN_TIME_QC_LOG="\[\([0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\} [0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}\)\].*"
GREP_OUT_QC=$(grep_last_occurrences "$QC_LOG" "$START_MESSAGE" "1")
TIME_PREVIOUS_QC=$(parse_date "$GREP_OUT_QC" "$PATTERN_TIME_QC_LOG")
TIME_PREVIOUS_QC=${TIME_PREVIOUS_QC:-$(get_date "now")}
TIME_PREVIOUS_QC=$(get_date $TIME_PREVIOUS_QC"-15minutes")

print_current_time
echo $START_MESSAGE


# get timestamps file transfers

# DATA_TRANSFER_LOG=/home/nvds/Documents/RBINS/ODANext/qc_through_sensorthings/log_tmp.txt
DATA_TRANSFER_LOG=/home/RBINS.BE/bmdc/belgica_data_transfer/log.txt

## get 10000 occurences of "QC "
PATTERN_TIME_TRANSFER_LOG="\(^[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\} [0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}\).*"
GREP_OUT_TRANSF="$(grep_last_occurrences "$DATA_TRANSFER_LOG" "QC " "10")"

## define pattern to get time until which transfer is ready
PATTERN_TIME_QC_END=".*QC up to \([0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\} [0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}\).*"
DT_INT=$(echo $DT | tr -dc '0-9')
DT_INT=$(($DT_INT))
DT_UNIT=$(printf '%s\n' "${DT//[[:digit:]]/}")

# loop over extracted lines
COUNTER=0
while read -r LINE; do
    # extract timestamp of log entry transfer
    TIME_I=$(parse_date "$LINE" "$PATTERN_TIME_TRANSFER_LOG")
    # check for timestamps more recent than previous QC run
    echo -n "   "
    echo "$TIME_I" vs "$TIME_PREVIOUS_QC"
    if [[ "$TIME_I" > "$TIME_PREVIOUS_QC" ]]; then
        END_I=$(parse_date "$LINE" "$PATTERN_TIME_QC_END")
        START_I=$(get_date "$END_I UTC -$((DT_INT+OVERLAP))$DT_UNIT")

        # start container to do QC
        CONTAINER_ID=$(docker run \
                -d --rm --network=host --user "$(id -u):$(id -g)"\
                --workdir /app \
                -v "$CONFIG_FOLDER":/app/conf \
                -v "$OUTPUT_FOLDER":/app/outputs \
                -e DEV_SENSORS_USER="$SENSORS_USER" \
                -e DEV_SENSORS_PASS="$SENSORS_PASS" \
                rbinsbmdc/quality_assurance_tool:$IMAGE_TAG \
                "time.start=$START_I" "time.end=$END_I")
        
        STATUS_CODE_CONTAINER="$(docker container wait $CONTAINER_ID)"
        print_current_time
        echo "Status code $CONTAINER_ID: $STATUS_CODE_CONTAINER"

        # variables needed for transfer script
        FMT_TRANSFER_SCRIPT="+%Y-%m-%d %H:%M:%S"
        startdate=$(date -u --date="$START_I UTC" "$FMT_TRANSFER_SCRIPT")
        enddate=$(date -u --date="$END_I UTC" "$FMT_TRANSFER_SCRIPT")
        
        print_current_time
        echo "Start production "
        
        # run transfer script as other user
        sudo -i -u ndeville startdate="$startdate" enddate="$enddate"/usr/bin/env bash $ROOT_DIR/crontab/sta_raw_to_sta_prod_transfer\ 1.sh
        COUNTER=$((COUNTER+1))
    fi
done <<< "$GREP_OUT_TRANSF"
