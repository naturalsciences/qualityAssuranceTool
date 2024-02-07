PATH=$PATH:/usr/bin

cd /home/nvadmin/Documents/ODANext/qc_through_sensorthings
docker run -it --network=host --user "$(id -u):$(id -g)" --workdir /app -v ./conf:/app/conf -v ./outputs:/app/outputs -e DEV_SENSORS_USER=$DEV_SENSORS_USER -e DEV_SENSORS_PASS=$DEV_SENSORS_PASS vdsnils/quality_assurance_tool:v0.3 "time.start=$(date --date=$now-'16minutes' +'%Y-%m-%d %H:%M')" "time.end=$(date --date=$now-'1minute' +'%Y-%m-%d %H:%M')"
