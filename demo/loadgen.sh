#!/bin/sh
set -x
# Offramp configuration
LOAD_FILE=${LOAD_FILE:-"data.json.xz"}
CONFIG_FILE=${CONFIG_FILE:-"tremor.yaml"}
SLEEP=${SLEEP:-"0"}
sleep $SLEEP

set +x
export RUST_BACKTRACE=full
while true
do
	xzcat $LOAD_FILE
done | \
./tremor-runtime --config "${CONFIG_FILE}"
