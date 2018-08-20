#!/usr/bin/env /bin/sh

BASEDIR=$(dirname "$0")

for test in `ls -d ${BASEDIR}/*.test | sed -e 's/\.test$//g' -e "s;^${BASEDIR}/;;g"`
do
	  if ${BASEDIR}/run_test.sh $test
    then
        PASSED_TESTS="${PASSED_TESTS}$test "
    else
        FAILED_TESTS="${FAILED_TESTS}$test "
    fi
done

if [ ! -z "${PASSED_TESTS+x}" ]
then
    echo "Passed test cases: ${PASSED_TESTS}"
fi

if [ ! -z "${FAILED_TESTS+x}" ]
then
    echo "Failed test cases: ${FAILED_TESTS}"
    exit 1
fi

