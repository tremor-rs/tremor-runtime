#!/usr/bin/env bash

TOML_FILES="\
Cargo.toml \
tremor-api/Cargo.toml \
tremor-cli/Cargo.toml \
tremor-common/Cargo.toml \
tremor-influx/Cargo.toml \
tremor-pipeline/Cargo.toml \
tremor-script/Cargo.toml \
tremor-value/Cargo.toml
"
VERSION_TESTS="\
tremor-cli/tests/api-cli/command.yml \
tremor-cli/tests/api/command.yml\
"
DOCKER_FILES="\
Dockerfile.learn\
"
PACKAGES="\
tremor-common \
tremor-value \
tremor-script\
"
old=$1
new=$2

echo -n "Updating TOML files:"
for toml in ${TOML_FILES}
do
    echo -n " ${toml}"
    sed -e "s/^version = \"${old}\"$/version = \"${new}\"/" -i.release "${toml}"
done
echo "."

echo -n "Updating Version Tests:"
for f in ${VERSION_TESTS}
do
    echo -n " ${f}"
    sed -e "s/- '{\"version\":\"${old}\"/- '{\"version\":\"${new}\"/" -i.release "${f}"
done
echo "."

echo -n "Updating Docker files:"
for f in ${DOCKER_FILES}
do
    echo -n " ${f}"
    sed -e "s;^FROM tremorproject/tremor:${old}$;FROM tremorproject/tremor:${new};" -i.release "${f}"
done
echo "."

echo "Updating CHANGELOG.md"
sed -e "s/^## Unreleased$/## ${new}/" -i.release "CHANGELOG.md"