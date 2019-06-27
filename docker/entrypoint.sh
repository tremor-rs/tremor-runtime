#!/bin/sh

set -x
if [ ! -z ${SLEEP+x} ]
then
    sleep "$SLEEP"
fi

if [ -z ${LOGGER_FILE+x} ]
then
   LOGGER_FILE="/etc/tremor/logger.yaml"
fi

exec ./tremor-server --config /etc/tremor/config/*.yaml --logger-config "${LOGGER_FILE}"
