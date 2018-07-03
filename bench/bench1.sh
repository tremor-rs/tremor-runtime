#!/bin/sh

. $(dirname $0)/common.inc

result=$(target/release/tremor-runtime -m -c ./bench/bench1.yaml)

echo "$result"
publish "$result"
