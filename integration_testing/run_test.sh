#!/usr/bin/env sh

BASEDIR=$(dirname "$0")

. ${BASEDIR}/common.sh

TEST=$1
TEST_DIR="${BASEDIR}/${TEST}.test"
CONFIG_FILE="${BASEDIR}/${TEST}.test/config"
IN_FILE="${BASEDIR}/${TEST}.test/in.json.xz"
DATA_FILE="${BASEDIR}/${TEST}.test/in.json"
RULES_FILE="${BASEDIR}/${TEST}.test/rules.json"
OUT_FILE="${BASEDIR}/${TEST}.test/out.json.xz"
README_FILE="${BASEDIR}/${TEST}.test/README.md"

GEN_FILE="${BASEDIR}/${TEST}.test/gen.json"
EXP_FILE="${BASEDIR}/${TEST}.test/exp.json"
LOG_FILE="${BASEDIR}/${TEST}.test/log.txt"

if [ ! -d "${TEST_DIR}" ]
then
  error "${TEST}: directory '${TEST_DIR}' not found"
  exit 1
fi

if [ ! -f "${IN_FILE}" ]
then
  error "${TEST}: file '${IN_FILE}' not found"
  exit 1
fi

if [ ! -f "${RULES_FILE}" ]
then
  error "${TEST}: file '${RULES_FILE}' not found"
  exit 1
fi

if [ ! -f "${OUT_FILE}" ]
then
  error "${TEST}: file '${OUT_FILE}' not found"
  exit 1
fi

if [ ! -f "${CONFIG_FILE}" ]
then
  error "${TEST}: file '${CONFIG_FILE}' not found"
  exit 1
else
  . ${CONFIG_FILE}
fi

if [ ! -f "${README_FILE}" ]
then
    error "${TEST}: file '${README_FILE}' not found"
    exit 1
else
    . ${CONFIG_FILE}
fi

RULES=`cat ${RULES_FILE}`

ONRAMP=${ONRAMP:-"file"}
ONRAMP_CONFIG=${ONRAMP_CONFIG:-"$DATA_FILE"}

# Offramp configuration
OFFRAMP=${OFFRAMP:-"file"}
OFFRAMP_CONFIG=${OFFRAMP_CONFIG:-"${GEN_FILE}"}

# Offramp configuration
DROP_OFFRAMP=${DROP_OFFRAMP:-"null"}
DROP_OFFRAMP_CONFIG=${DROP_OFFRAMP_CONFIG:-""}

# Parser Configuration
PARSER=${PARSER:-"raw"}
PARSER_CONFIG=${PARSER_CONFIG:-""}

# Classifier Configuration
CLASSIFIER=${CLASSIFIER:-"mimir"}
CLASSIFIER_CONFIG=${CLASSIFIER_CONFIG:-${RULES:-""}}

# Grouping Configuration
GROUPING=${GROUPING:-"bucket"}
GROUPING_CONFIG=${GROUPING_CONFIG:-${RULES:-""}}

# Limiting Configuration
LIMITING=${LIMITING:-"pass"}
LIMITING_CONFIG=${LIMITING_CONFIG:-""}

THREADS=1

xzcat $IN_FILE > $DATA_FILE
cat ${README_FILE}
if cargo run -- --on-ramp "${ONRAMP}" --on-ramp-config "${ONRAMP_CONFIG}" \
      --off-ramp "${OFFRAMP}" --off-ramp-config "${OFFRAMP_CONFIG}" \
      --drop-off-ramp "${DROP_OFFRAMP}" --drop-off-ramp-config "${DROP_OFFRAMP_CONFIG}" \
      --parser "${PARSER}" --parser-config "${PARSER_CONFIG}" \
      --classifier "${CLASSIFIER}" --classifier-config "${CLASSIFIER_CONFIG}" \
      --grouping "${GROUPING}" --grouping-config "${GROUPING_CONFIG}" \
      --limiting "${LIMITING}" --limiting-config "${LIMITING_CONFIG}" \
      --pipeline-threads "${THREADS}" 2> ${LOG_FILE}
then
    if [ -z "${SHOULD_CRASH+x}" ]
    then
        xzcat ${OUT_FILE} > ${EXP_FILE}
        rm ${DATA_FILE}

        if diff ${GEN_FILE} ${EXP_FILE} > /dev/null
        then
            rm ${GEN_FILE} ${EXP_FILE} ${LOG_FILE}
            ok "${TEST}: passed"
        else
            error "${TEST}: failed"
            exit 1
        fi
    else
        error "${TEST}: execution failure expected but success received"
        exit 1
    fi
else
    rm ${DATA_FILE}
    if [ -z "${SHOULD_CRASH+x}" ]
    then
        error "${TEST}: execution failed"
        exit 1
    else
        ok "${TEST}: expected failure"
    fi

fi
