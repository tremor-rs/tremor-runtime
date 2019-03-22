#!/bin/sh

set -x
if [ ! -z ${SLEEP+x} ]
then
    sleep "$SLEEP"
fi

if [ ! -z ${CONFIG+x} ]
then
    echo "${CONFIG}" > tremor.yaml
    CONFIG_FILE=tremor.yaml
else
    if [ -z ${CONFIG_FILE+x} ]
    then
        CONFIG_FILE=tremor.yaml
    fi
fi

if [ ! -z ${MAPPINGS+x} ]
then
    echo "${MAPPINGS}" > mapping.yaml
    MAPPING_FILE=mapping.yaml
else
    if [ -z ${MAPPING_FILE} ]
    then
        MAPPING_FILE=mapping.yaml
    fi
fi

if [ -z ${LOGGER_FILE+x} ]
then
   LOGGER_FILE="logger.yaml"
fi

exec ./tremor-server --config "${CONFIG_FILE}" "${MAPPING_FILE}" --logger-config "${LOGGER_FILE}"
