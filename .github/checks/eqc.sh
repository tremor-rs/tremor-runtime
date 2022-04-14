#!/usr/bin/env bash
set -xeo pipefail

PIDFILE="$PWD/tremor.pid"
export TREMOR_PATH="$PWD/tremor-script/lib:$TREMOR_PATH"

for TROY_SAMPLE in $(ls tremor-erl/samples/*.troy)
do
    target/debug/tremor server run "$TROY_SAMPLE" --pid "$PIDFILE" &


    # stop tremor upon exiting
    function stop_tremor {
        if [ -f "$PIDFILE" ]
        then
            kill $(cat "$PIDFILE"); rm -f "$PIDFILE"
        fi
    }
    trap stop_tremor EXIT

    cd tremor-erl
    rebar3 as eqc eqc; stop_tremor; cd ..
done

