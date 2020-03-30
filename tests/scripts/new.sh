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

sed -e "s;//INSERT;//INSERT\n    ${NAME},;" ../script.rs > tmp && mv tmp ../script.rs