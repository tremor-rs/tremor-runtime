#!/bin/sh
set -x
# Input configuration
INPUT=${INPUT:-"stdin"}
INPUT_CONFIG=${INPUT_CONFIG:-""}

# Output configuration
OUTPUT=${OUTPUT:-"stdout"}
OUTPUT_CONFIG=${OUTPUT_CONFIG:-""}

# Parser Configuration
PARSER=${PARSER:-"raw"}
PARSER_CONFIG=${PARSER_CONFIG:-""}

# Classifier Configuration
CLASSIFIER=${CLASSIFIER:-"static"}
CLASSIFIER_CONFIG=${CLASSIFIER_CONFIG:-""}

# Grouping Configuration
GROUPING=${GROUPING:-"pass"}
GROUPING_CONFIG=${GROUPING_CONFIG:-""}

# Limiting Configuration
LIMITING=${LIMITING:-"pass"}
LIMITING_CONFIG=${LIMITING_CONFIG:-""}

./traffic-shaping-tool --input "${INPUT}" --input-config "${INPUT_CONFIG}" \
                       --output "${OUTPUT}" --output-config "${OUTPUT_CONFIG}" \
                       --parser "${PARSER}" --parser-config "${PARSER_CONFIG}" \
                       --classifier "${CLASSIFIER}" --classifier-config "${CLASSIFIER_CONFIG}" \
                       --grouping "${GROUPING}" --grouping-config "${GROUPING_CONFIG}" \
                       --limiting "${LIMITING}" --limiting-config "${LIMITING_CONFIG}"

