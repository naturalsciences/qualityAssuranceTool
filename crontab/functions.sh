#!/usr/bin/env bash


# datetime format
FMT="+%Y-%m-%d %H:%M:%S"

# print the current time
print_current_time(){
        local TIMESTAMP_NOW=$(date --date now "$FMT")
        echo -n "["$TIMESTAMP_NOW"] - "
}

# transform the first (and only) argument to date in FMT format
get_date(){
        local DATE=${1:-now}
        local DATETIME_OUT=$(date --date="$DATE" "$FMT")
        echo $DATETIME_OUT
}

# parse the dat in the first argument, based on the pattern provided in the second argument
parse_date(){
    local LINE=$1
    local PATTERN=${2:-".*QC up to \([0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\} [0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}\).*"}
    local DATE=$(echo "$LINE" | sed -n "s/$PATTERN/\1/p")
    echo $DATE
}

# get the last n (3rd argument) occurrences of a string (2nd argument) in a file (3rd argument)
grep_last_occurrences(){
        local FILE=$1
        local STR=$2
        local NB=${3:-1}
        echo -e "$(grep "$STR" "$FILE" | tail -"$NB")"
}

# export functions to be usable in script sourcing this file
export print_current_time
export get_date
export parse_date
export grep_last_occurrences