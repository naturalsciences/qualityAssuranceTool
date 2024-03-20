#!/usr/bin/env bash


FMT="+%Y-%m-%d %H:%M:%S"

print_current_time(){
        local TIMESTAMP_NOW=$(date --date now "$FMT")
        echo -n "["$TIMESTAMP_NOW"] - "
}

get_date(){
        local DATE=${1:-now}
        local DATETIME_OUT=$(date -u --date="$DATE" "$FMT")
        echo $DATETIME_OUT
}

parse_date(){
    local LINE=$1
    local PATTERN=${2:-".*QC up to \([0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\} [0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}\).*"}
    local DATE=$(echo "$LINE" | sed -n "s/$PATTERN/\1/p")
    echo $DATE
}

grep_last_occurrences(){
        local FILE=$1
        local STR=$2
        local NB=${3:-1}
        echo -e "$(grep "$STR" "$FILE" | tail -"$NB")"
}

export print_current_time
export get_date
export parse_date
export grep_last_occurrences