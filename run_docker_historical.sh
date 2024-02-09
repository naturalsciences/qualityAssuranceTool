#!/usr/bin/env bash

PATH=$PATH:/usr/bin

. env_docker_historical

if [[ ! -r "counter.txt" ]]; then
    DH_COUNTER=0;
else
    DH_COUNTER=$(cat "counter.txt")
fi

echo $START_TIME
# START_I=$(date --date=now"-1 day" "+%Y-%m-%d %H:%M")
START_I=$(date --date=now"-$((1*$DH_COUNTER)) day" "+%Y-%m-%d %H:%M")
echo $START_I
echo $DH_COUNTER
# docker run -it --network=host --user "$(id -u):$(id -g)" --workdir /app -v ./conf:/app/conf -v ./outputs:/app/outputs -e DEV_SENSORS_USER=$DEV_SENSORS_USER -e DEV_SENSORS_PASS=$DEV_SENSORS_PASS vdsnils/quality_assurance_tool:v0.3 "time.start=$(date --date=$now-'16minutes' +'%Y-%m-%d %H:%M')" "time.end=$(date --date=$now-'1minute' +'%Y-%m-%d %H:%M')"
DH_COUNTER=$((DH_COUNTER+1))
echo $DH_COUNTER > counter.txt
