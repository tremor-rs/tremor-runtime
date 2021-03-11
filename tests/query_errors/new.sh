#!/usr/bin/env sh

BASEDIR=$(dirname "$0")
if [ -z "x${1}x" ]
then
	echo "Please give the test a name";
fi

NAME="${1}"
TARGET="${BASEDIR}/${1}"

if [ -d "${TARGET}" ]
then
	echo "Please give the test a name";
fi

cp -r ${BASEDIR}/_template ${TARGET}
git add ${TARGET}

sed -e "/^   \/\/INSERT/a\
'"${NAME}," ${BASEDIR}/../query_error.rs > ${BASEDIR}/tmp && mv ${BASEDIR}/tmp ${BASEDIR}/../query_error.rs

for f in ${TARGET}/*
do
    echo "$f"
done
