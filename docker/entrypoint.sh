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

queries=`ls -1 /etc/tremor/config/*.trickle 2>/dev/null | wc -l`
if [ $queries != 0 ]
then
    exec ./tremor-server --config /etc/tremor/config/*.yaml --query /etc/tremor/config/*.trickle --logger-config "${LOGGER_FILE}"
else
    exec ./tremor-server --config /etc/tremor/config/*.yaml --logger-config "${LOGGER_FILE}"
fi



