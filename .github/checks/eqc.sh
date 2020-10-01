#!/usr/bin/env bash

target/debug/tremor server run &
cd tremor-erl || exit 1
rebar3 as eqc eqc
cd .. || exit 1
pkill tremor