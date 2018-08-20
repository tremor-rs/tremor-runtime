#!/usr/bin/env /bin/sh

BASEDIR=$(dirname "$0")

for test in `ls -d ${BASEDIR}/*.test | sed -e 's/\.test$//g' -e "s;^${BASEDIR}/;;g"`
do
	${BASEDIR}/run_test.sh $test
done
