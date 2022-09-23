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

if [ -z ${TREMOR_DIR+x} ]
then
    TREMOR_DIR="/etc/tremor"
fi

if [ -z ${CFG_DIR+x} ]
then
    CFG_DIR="${TREMOR_DIR}/config"
fi

if [ -z ${LOGGER_FILE+x} ]
then
    LOGGER_FILE="${TREMOR_DIR}/logger.yaml"
fi

if [ "$#" != "0" ]
then
    ARGS=$*
else

    ARTEFACTS=""

    # NOTE: find does not always provide sorting, we need to ensure that order is preserved
    # correctly


    # Load *.troy files
    TROYS=$(find /etc/tremor/config/ -not -path '*/.*' \( -type f -o -type l \) -name '*.troy' -print 2>/dev/null | sort)
    [ ! -z "$TROYS" ] && ARTEFACTS="$ARTEFACTS $TROYS"

    ARGS="--logger-config ${LOGGER_FILE} server run"

    if [ ! -z "${ARTEFACTS}" ]
    then
        ARGS="${ARGS} ${ARTEFACTS}"
    fi
fi

# IMPORTANT: do not quote ARGS, no matter what shellcheck tells you
exec /usr/bin/tini /tremor -- ${ARGS}
