#!/bin/bash
# simple script for repeatedly sending messages over the same websocket connection
# based on: https://github.com/vi/websocat/issues/68#issuecomment-573003140

# exit the script when a command fails
set -o errexit

# catch exit status for piped commands
set -o pipefail

WS_ENDPOINT="ws://localhost:9899"

TCP_IP="127.0.0.1"
TCP_PORT=1234

send_message() {
  echo "-> ${1}"
  echo "$1" | nc "$TCP_IP" "$TCP_PORT" | jq
  echo ""
}

websocat -t -E tcp-l:"${TCP_IP}:${TCP_PORT}" reuse-raw:"${WS_ENDPOINT}" --max-messages-rev 1&
sleep 0.1

# empty message test
# TODO investigate ordering without this
send_message ""

##############################################################################

echo -e "Testing echo protocol...\n"
send_message '{"tremor":{"connect":{"protocol":"echo","alias":"echo-example"}}}'
send_message '{"echo-example":{"beep":"boop"}}'
send_message '{"echo-example":{"snot":"badger"}}'

echo -e "Testing microring protocol...\n"
send_message '{"tremor":{"connect":{"protocol":"microring"}}}'
send_message '{"microring":{"op":"status"}}'
# FIXME gives Unexpected error during client connect Illegel State Transition
#send_message '{"microring":{"op":"nonexistent"}}'

##############################################################################

# TODO do this with trap so that we do this always
kill %1
