#!/bin/bash

echo "Stopping WS by sending quit json string to port 4242"
for i in {1..5};
do
  if ! $(nc -zv  localhost 4242); then
    echo '"quit"' | websocat ws://localhost:4242
  else
    echo "Killing it softly at attempt ${i}"
    break;
  fi
done;

echo "If a pid file exists, hard kill"
if test -f before.pid; then
    kill -9 $( cat before.pid )
    # Remove pid file
    rm -f before.pid
fi