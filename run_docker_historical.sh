#!/usr/bin/env bash

PATH=$PATH:/usr/bin
source env_docker_historical

if [[ ! -r "counter.txt" ]]; then
    DH_COUNTER=0;
else
    DH_COUNTER=$(cat "counter.txt")
fi

DT_INT=$(echo $DT | tr -dc '0-9')
DT_INT=$(($DT_INT))
DT_UNIT=$(printf '%s\n' "${DT//[[:digit:]]/}")

FMT="+%Y-%m-%d %H:%M"

START_I=$(date -u --date="$START_TIME UTC +$((DT_INT*DH_COUNTER))$DT_UNIT" "$FMT")
END_I=$(date -u --date="$START_I UTC +$((DT_INT))$DT_UNIT" "$FMT")

docker run \
    --rm -d --network=host --user "$(id -u):$(id -g)"\
    --workdir /app \
    -v $CONFIG_FOLDER:/app/conf \
    -v $OUTPUT_FOLDER:/app/outputs \
    -e DEV_SENSORS_USER=$(keyctl print $(keyctl search @u user SENSORS_USER)) \
    -e DEV_SENSORS_PASS=$(keyctl print $(keyctl search @u user SENSORS_PASS)) \
    nbmdc/quality_assurance_tool:tmp \
    "time.start=$START_I" "time.end=$END_I"

DH_COUNTER=$((DH_COUNTER+1))
echo $DH_COUNTER > counter.txt
