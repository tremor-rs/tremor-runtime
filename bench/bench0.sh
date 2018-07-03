#!/bin/sh

. $(dirname $0)/common.inc

result=$(RUST_BACKTRACE=1 target/release/tremor-runtime -m -c ./bench/bench0.yaml)

echo "$result"
publish "$result"
