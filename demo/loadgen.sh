#!/bin/sh
set -x
# Output configuration
TOPIC=${TOPIC:-"demo"}
PRODUCERS=${PRODUCERS:-"kafka:9092"}
OUTPUT_CONFIG="$TOPIC|$PRODUCERS"
LOAD_FILE=${LOAD_FILE:-"data.json.xz"}
MPS=${MPS:-"10"}
SLEEP=${SLEEP:-"0"}

sleep $SLEEP

set +x
export RUST_BACKTRACE=1
while true
do
	xzcat $LOAD_FILE
done | \
./tremor-runtime --input "stdin" \
                 --output "kafka" --output-config "${OUTPUT_CONFIG}" \
                 --parser "json" \
                 --classifier "constant" --classifier-config "default" \
                 --grouping "bucket" --grouping-config "[{\"name\":\"default\",\"rate\":$MPS}]"
