#!/usr/bin/env sh
set -ex
BASEDIR=$(dirname "$0")
TEST_SUITE_FILE="${BASEDIR}/../query_warning.rs"
TMPDIR="${BASEDIR}/tmp"

if [ -z "${1}" ]
then
	echo "Usage: $0 TEST_NAME";
	exit 1
fi

NAME="${1}"
TARGET="${BASEDIR}/${1}"

if [ -d "${TARGET}" ]
then
	echo "A test dir with that name already exists.";
	exit 1
fi

cp -r ${BASEDIR}/_template ${TARGET}
git add ${TARGET}

sed -e '/^    \/\/ INSERT/a\
'"${NAME}," "${TEST_SUITE_FILE}" > "${TMPDIR}" && mv "${TMPDIR}" "${TEST_SUITE_FILE}"

for f in ${TARGET}/*
do
    echo "$f"
done
