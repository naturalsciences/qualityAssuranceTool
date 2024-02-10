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

TEST="-$((DT_INT*DH_COUNTER)) $DT_UNIT"
START_I=$(date --date=now"-$((DT_INT*DH_COUNTER))$DT_UNIT" "$FMT")
END_I=$(date --date="$START_I-$((DT_INT))$DT_UNIT" "$FMT")

echo $START_I "---->" $END_I
# docker run -it --network=host --user "$(id -u):$(id -g)" --workdir /app -v ./conf:/app/conf -v ./outputs:/app/outputs -e DEV_SENSORS_USER=$DEV_SENSORS_USER -e DEV_SENSORS_PASS=$DEV_SENSORS_PASS vdsnils/quality_assurance_tool:v0.3 "time.start=$(date --date=$now-'16minutes' +'%Y-%m-%d %H:%M')" "time.end=$(date --date=$now-'1minute' +'%Y-%m-%d %H:%M')"
DH_COUNTER=$((DH_COUNTER+1))

echo $DH_COUNTER > counter.txt
