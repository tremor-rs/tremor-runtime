#!/usr/bin/env bash

for test in elastic kafka s3; do
  image=$(grep "IMAGE:" src/connectors/tests/$test.rs | sed -e 's/.*"\(.*\)".*/\1/g')
  version=$(grep "VERSION:" src/connectors/tests/$test.rs | sed -e 's/.*"\(.*\)".*/\1/g')
  docker pull $image:$version
done