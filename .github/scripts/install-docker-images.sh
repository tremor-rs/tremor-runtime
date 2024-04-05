#!/usr/bin/env bash

for test in elastic kafka aws; do
  image=$(grep "IMAGE:" tremor-connectors/tests/$test.rs | sed -e 's/.*"\(.*\)".*/\1/g')
  version=$(grep "VERSION:" tremor-connectors/tests/$test.rs | sed -e 's/.*"\(.*\)".*/\1/g')
  docker pull $image:$version
done