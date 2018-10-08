#!/bin/sh

host="distsysberlinc1n7.dev.bo1.csnzoo.com:9000"

group="tremor-runtime"
benchmark=$(basename $0)
benchmark=${benchmark%.*}
hash=$(git rev-parse HEAD)
date=$(date +"%s")

publish() {
    cat <<EOF | curl -X POST -H "Content-Type: application/json" "http://$host/groups/$group/benchmarks/$benchmark/runs" -d @-
{
  "hash": "$hash",
  "date": $date,
  "result": "$1"
}
EOF
}
