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
export RUST_BACKTRACE=full
while true
do
	xzcat $LOAD_FILE
done | \
./tremor-runtime --config "${CONFIG_FILE:tremor.yaml}"
