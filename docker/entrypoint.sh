#!/bin/sh

set -x
if [ -n "${SLEEP+x}" ]
then
    sleep "$SLEEP"
fi


if [ "$#" != "0" ]
then
    ARGS=$*
else

    if [ -z ${LOGGER_FILE+x} ]
    then
        LOGGER_FILE="/etc/tremor/logger.yaml"
    fi

    ARTEFACTS="/etc/tremor/config/*.yaml"
    QUERIES=$(find /etc/tremor/config/ -name '*.trickle' -print 2>/dev/null | wc -l)
    if [ "$QUERIES" != 0 ]
    then
        ARTEFACTS="$ARTEFACTS /etc/tremor/config/*.trickle"
    fi
    ARGS="server run -f ${ARTEFACTS} --logger-config ${LOGGER_FILE}"
fi

exec /tremor "${ARGS}"



