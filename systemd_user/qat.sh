#!/usr/bin/env bash
#

touch /tmp/testing_now

docker run -d --network=host --user "$(id -u):$(id -g)" --workdir /app -v ./conf:/app/conf -v ./outputs:/app/outputs -e HYDRA_FULL_ERROR=1 -e DEV_SENSORS_USER=$DEV_SENSORS_USER -e DEV_SENSORS_PASS=$DEV_SENSORS_PASS vdsnils/quality_assurance_tool:test "time.start=$(date --date=$now-'16minutes' +'%Y-%m-%d %H:%M')" "time.end=$(date --date=$now-'1minute' +'%Y-%m-%d %H:%M')" >> /tmp/outputtesting 2>&1
