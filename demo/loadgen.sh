#!/bin/sh
set -x
# Offramp configuration
TOPIC=${TOPIC:-"demo"}
PRODUCERS=${PRODUCERS:-"kafka:9092"}
OFFRAMP_CONFIG="$TOPIC|$PRODUCERS"
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
./tremor-runtime --on-ramp "stdin" \
                 --off-ramp "kafka" --off-ramp-config "${OFFRAMP_CONFIG}" \
                 --parser "json" \
                 --classifier "constant" --classifier-config "default" \
                 --grouping "bucket" --grouping-config "[{\"class\":\"default\",\"rate\":$MPS}]"
