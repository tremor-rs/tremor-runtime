#!/bin/sh

group="tremor-runtime"
benchmark=$(basename $0)
benchmark=${benchmark%.*}
hash=$(git rev-parse HEAD)
date=$(date +"%s")

publish() {
    cat <<EOF | curl -X POST -H "Content-Type: application/json" "http://$HOST/groups/$group/benchmarks/$benchmark/runs" -d @-
{
  "hash": "$hash",
  "date": $date,
  "result": "$1"
}
EOF
}
