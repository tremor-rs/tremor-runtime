#!/bin/sh

. $(dirname $0)/common.inc

result=$(RUST_BACKTRACE=1 target/release/tremor-runtime -m -c ./bench/bench2.yaml)

echo "$result"
publish "$result"
