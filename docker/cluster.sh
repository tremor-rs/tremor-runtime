#!/bin/sh
set -x

# Possible environment variables:
# TREMOR_DIR: The base directory for tremor config (`/etc/tremor`)
# CFG_DIR: Directory to load config from (`${TREMOR_DIR}/config`)
# LOGGER_FILE: the logger configuration (`${TREMOR_DIR}/logger.yaml`)
# SLEEP: Number of seconds to sleep before starting tremor

if [ -n "${SLEEP+x}" ]
then
    sleep "$SLEEP"
fi

if [ -z ${DATA_DIR+x} ]
then
    TREMOR_DIR="/data"
fi

if [ -z ${TREMOR_DIR+x} ]
then
    TREMOR_DIR="/etc/tremor"
fi

if [ -z ${LOGGER_FILE+x} ]
then
    LOGGER_FILE="${TREMOR_DIR}/logger.yaml"
fi

if [ -z ${RPC_IP+x} ]
then
    echo "RPC_IP is not set"
    return 1
fi

if [ -z ${API_IP+x} ]
then
    echo "API_IP is not set"
    return 1
fi

if [ -z ${JOIN+x} ]
then
    exec /usr/bin/tini /tremor -- bootstrap --remove-on-sigterm --db-dir "${DATA_DIR}" --rpc "${RPC_IP}" --api "${API_IP}"
else
    exec /usr/bin/tini /tremor -- start --remove-on-sigterm --db-dir "${DATA_DIR}" --rpc "${RPC_IP}" --api "${API_IP}" --join "${JOIN}"
fi