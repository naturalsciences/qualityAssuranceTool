#!/usr/bin/env bash
docker build --network=docker_default --no-cache --build-arg ID_U=$(id -u) -t qc_sensorthings .