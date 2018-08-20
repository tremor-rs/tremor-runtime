#!/usr/bin/env /bin/sh

BASEDIR=$(dirname "$0")

. ${BASEDIR}/common.sh

TEST=$1
TEST_DIR="${BASEDIR}/${TEST}.test"
CONFIG_FILE="${BASEDIR}/${TEST}.test/config"
IN_FILE="${BASEDIR}/${TEST}.test/in.json.xz"
RULES_FILE="${BASEDIR}/${TEST}.test/rules.json"
OUT_FILE="${BASEDIR}/${TEST}.test/out.json.xz"

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

cargo run > /dev/null

ok "${TEST}: passed"
