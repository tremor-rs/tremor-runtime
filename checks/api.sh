#!/usr/bin/env bash


help() {
    cat <<EOF
Usage: ${0##*/} [-hd] [-t TARGET] [-c CMD] [TEST]...
code sanity checker
  -h         show this help
  -a         run all chekcs
  -c         curl
  -t         tremor-tool
  -e         eqc
EOF
}

generate_json() {
    ~/go/bin/yaml2json < static/openapi.yaml > static/openapi.json
}
start_tremor() {
    target/debug/tremor-server &
}

stop_trempor() {
    pkill tremor-server
}

while getopts hacte opt; do
    case $opt in
        h)
            help
            exit 0
            ;;
        a)
            exec "$0" -cte
            ;;
        c)
            start_tremor
            cd tremor-api || exit 1
            ~/go/bin/coyote -c tests/coyote.yml
            cd .. || exit 1
            stop_trempor
        ;;
        t)
            start_tremor
            cd tremor-tool || exit 1
            ~/go/bin/coyote -c tests/coyote.yml
            cd .. || exit 1
            stop_trempor
        ;;
        e)
            start_tremor
            cd tremor-erl || exit 1
            erl -noshell -eval "eqc:registration(\"${EQC_LICENSE}\")" -eval "eqc:start()" -eval "init:stop()"
            rebar3 as eqc eqc
            cd .. || exit 1
            stop_trempor
        ;;
        *)
            help
            exit 0
            ;;
        
    esac
done