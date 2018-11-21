#!/bin/sh

set -x
if [ ! -z ${SLEEP+x} ]
then
    sleep $SLEEP
fi

if [ ! -z ${CONFIG+x} ]
then
    echo "${CONFIG}" > tremor.yaml
fi

if [ -z ${CONFIG_FILE+x} ]
then
    CONFIG_FILE=tremor.yaml
fi

exec ./tremor-runtime --config "${CONFIG_FILE}"
