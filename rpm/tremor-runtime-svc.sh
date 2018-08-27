#!/bin/sh

. /etc/tremor/tremor.conf

# Input configuration
ONRAMP=${ONRAMP:-"kafka"}
ONRAMP_CONFIG=${ONRAMP_CONFIG:-'{"group_id":"demo","topics":["demo"],"brokers":["kafka:9092"]}'}

# Offramp configuration
OFFRAMP=${OFFRAMP:-"stdout"}
OFFRAMP_CONFIG=${OFFRAMP_CONFIG:-""}

# Offramp configuration
DROP_OFFRAMP=${DROP_OFFRAMP:-"null"}
DROP_OFFRAMP_CONFIG=${DROP_OFFRAMP_CONFIG:-""}

# Parser Configuration
PARSER=${PARSER:-"raw"}
PARSER_CONFIG=${PARSER_CONFIG:-""}

# Classifier Configuration
CLASSIFIER=${CLASSIFIER:-"constant"}
CLASSIFIER_CONFIG=${CLASSIFIER_CONFIG:-${RULES:-""}}

# Grouping Configuration
GROUPING=${GROUPING:-"pass"}
GROUPING_CONFIG=${GROUPING_CONFIG:-${RULES:-""}}

# Limiting Configuration
LIMITING=${LIMITING:-"pass"}
LIMITING_CONFIG=${LIMITING_CONFIG:-""}

# Threads
THREADS=${THREADS:-"1"}

export RUST_BACKTRACE=1
./tremor-runtime --on-ramp "${ONRAMP}" --on-ramp-config "${ONRAMP_CONFIG}" \
                 --off-ramp "${OFFRAMP}" --off-ramp-config "${OFFRAMP_CONFIG}" \
                 --drop-off-ramp "${DROP_OFFRAMP}" --drop-off-ramp-config "${DROP_OFFRAMP_CONFIG}" \
                 --parser "${PARSER}" --parser-config "${PARSER_CONFIG}" \
                 --classifier "${CLASSIFIER}" --classifier-config "${CLASSIFIER_CONFIG}" \
                 --grouping "${GROUPING}" --grouping-config "${GROUPING_CONFIG}" \
                 --limiting "${LIMITING}" --limiting-config "${LIMITING_CONFIG}" \
                 --pipeline-threads "${THREADS}"

