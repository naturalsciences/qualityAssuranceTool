#!/usr/bin/env bash

PATH=$PATH:/usr/bin

ROOT_DIR=$1
source $ROOT_DIR/crontab/env_docker_historical
cd $ROOT_DIR

if [[ ! -r "counter.txt" ]]; then
    DH_COUNTER=0;
else
    DH_COUNTER=$(cat "counter.txt")
fi

DT_INT=$(echo $DT | tr -dc '0-9')
DT_INT=$(($DT_INT))
DT_UNIT=$(printf '%s\n' "${DT//[[:digit:]]/}")

FMT="+%Y-%m-%d %H:%M"

START_I=$(date -u --date="$START_TIME UTC +$((DT_INT*DH_COUNTER-1))$DT_UNIT" "$FMT")
END_I=$(date -u --date="$START_I UTC +$((DT_INT+1))$DT_UNIT" "$FMT")

keyctl link @u @s
DEV_SENSORS_USER=$(keyctl print $(keyctl search $(keyctl get_persistent @u) user SENSORS_USER))
DEV_SENSORS_PASS=$(keyctl print $(keyctl search $(keyctl get_persistent @u) user SENSORS_PASS))

CONFIG_FOLDER=$ROOT_DIR/$CONFIG_FOLDER
OUTPUT_FOLDER=$ROOT_DIR/$OUTPUT_FOLDER

docker run \
    --rm -d --network=host --user "$(id -u):$(id -g)"\
    --workdir /app \
    -v $CONFIG_FOLDER:/app/conf \
    -v $OUTPUT_FOLDER:/app/outputs \
    -e DEV_SENSORS_USER=$DEV_SENSORS_USER \
    -e DEV_SENSORS_PASS=$DEV_SENSORS_PASS \
    nbmdc/quality_assurance_tool:v0.2 \
    "time.start=$START_I" "time.end=$END_I"
 
DH_COUNTER=$((DH_COUNTER+1))
echo $DH_COUNTER > $ROOT_DIR/counter.txt