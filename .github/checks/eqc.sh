#!/usr/bin/env bash
set -xeo pipefail

target/debug/tremor server run &

# stop tremor upon exiting
function stop_tremor {
    pkill tremor
}
trap stop_tremor EXIT

cd tremor-erl
rebar3 as eqc eqc
cd ..
pkill tremor

