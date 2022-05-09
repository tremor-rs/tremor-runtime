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

echo "Updating tremor dependencies in tremor-script"
cd tremor-script
sed -e "s/^tremor-common = { version = \"${old}\"/tremor-common = { version = \"${new}\"/" -i.release "Cargo.toml"
sed -e "s/^tremor-influx = { version = \"${old}\"/tremor-common = { version = \"${new}\"/" -i.release "Cargo.toml"
sed -e "s/^tremor-value = { version = \"${old}\"/tremor-common = { version = \"${new}\"/" -i.release "Cargo.toml"
cd ..
cd tremor-api
echo "Updating tremor dependencies in tremor-api"
sed -e "s/^tremor-runtime = { version = \"${old}\"/tremor-common = { version = \"${new}\"/" -i.release "Cargo.toml"
sed -e "s/^tremor-common = { version = \"${old}\"/tremor-common = { version = \"${new}\"/" -i.release "Cargo.toml"
sed -e "s/^tremor-value = { version = \"${old}\"/tremor-common = { version = \"${new}\"/" -i.release "Cargo.toml"
sed -e "s/^tremor-script = { version = \"${old}\"/tremor-common = { version = \"${new}\"/" -i.release "Cargo.toml"
sed -e "s/^tremor-pipeline = { version = \"${old}\"/tremor-common = { version = \"${new}\"/" -i.release "Cargo.toml"
cd ..
cd tremor-cli
echo "Updating tremor dependencies in tremor-cli"
sed -e "s/^tremor-runtime = { version = \"${old}\"/tremor-common = { version = \"${new}\"/" -i.release "Cargo.toml"
sed -e "s/^tremor-common = { version = \"${old}\"/tremor-common = { version = \"${new}\"/" -i.release "Cargo.toml"
sed -e "s/^tremor-value = { version = \"${old}\"/tremor-common = { version = \"${new}\"/" -i.release "Cargo.toml"
sed -e "s/^tremor-script = { version = \"${old}\"/tremor-common = { version = \"${new}\"/" -i.release "Cargo.toml"
sed -e "s/^tremor-pipeline = { version = \"${old}\"/tremor-common = { version = \"${new}\"/" -i.release "Cargo.toml"
sed -e "s/^tremor-api = { version = \"${old}\"/tremor-common = { version = \"${new}\"/" -i.release "Cargo.toml"
cd ..
echo "Updating tremor dependencies in tremor-pipeline"
sed -e "s/^tremor-common = { version = \"${old}\"/tremor-common = { version = \"${new}\"/" -i.release "Cargo.toml"
sed -e "s/^tremor-value = { version = \"${old}\"/tremor-common = { version = \"${new}\"/" -i.release "Cargo.toml"
sed -e "s/^tremor-script = { version = \"${old}\"/tremor-common = { version = \"${new}\"/" -i.release "Cargo.toml"