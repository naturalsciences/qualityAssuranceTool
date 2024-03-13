#!/usr/bin/env bash

DATA_TRANSFER_LOG=/home/RBINS.BE/bmdc/belgica_data_transfer/log.txt
LINE_ID="QC"

LOGLINE=$(grep $LINE_ID $DATA_TRANSFER_LOG | tail -1)

PATTERN=".*QC up to \([0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\} [0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}\)"
END_I=$(echo "$LOGLINE" | sed -n "s/$PATTERN.*/\1/p")
# START_I=$(echo "$LOGLINE" | sed -n 's/.*QC up to \([0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\} [0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}\).*/\1/p')