#!/usr/bin/env bash

DOCKER_FILES="\
Dockerfile.learn\
"

old=$1
new=$2

echo -n "Updating Docker files:"
for f in ${DOCKER_FILES}
do
    echo -n " ${f}"
    sed -e "s;^FROM tremorproject/tremor:${old}$;FROM tremorproject/tremor:${new};" -i.release "${f}"
done
echo "."

echo "Updating CHANGELOG.md"
sed -e "s/^## Unreleased$/## [${new}]/" -i.release "CHANGELOG.md"

