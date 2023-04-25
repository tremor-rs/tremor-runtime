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
    echo "DATA_DIR is not set, defaulting to /data"
    DATA_DIR="/data"
fi

if [ ! -d "${DATA_DIR}"  ]
then
    mkdir -p "${DATA_DIR}"
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
    echo "RPC_IP is not set, defaulting to ${POD_IP}:9000"
    RPC_IP="${POD_IP}:9000"
fi

if [ -z ${API_IP+x} ]
then
    echo "API_IP is not set, defaulting to ${POD_IP}:8000"
    API_IP="${POD_IP}:8000"
fi

if [ ! -z ${WORKER+x} ]
then
    ARGS="--remove-on-sigterm --passive"
fi

# tremor-0.tremor.default.svc.cluster.local
# export MY_NODE_NAME='minikube'
# export MY_POD_NAME='tremor-0'
# export MY_POD_NAMESPACE='default'
if [ ${TREMOR_SEED} = "${MY_POD_NAME}.tremor.${MY_POD_NAMESPACE}.svc.cluster.local" ]
then
    exec /usr/bin/tini /tremor -- cluster bootstrap --db-dir "${DATA_DIR}" --rpc "${RPC_IP}" --api "${API_IP}" ${ARGS}
else
    exec /usr/bin/tini /tremor -- cluster start --db-dir "${DATA_DIR}" --rpc "${RPC_IP}" --api "${API_IP}" --join "${TREMOR_SEED}:8000" ${ARGS}
fi