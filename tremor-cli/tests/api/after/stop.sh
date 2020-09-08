#!/bin/bash

echo "Stopping WS by sending quit json string to port 4242"
for i in {1..5};
do
  if ! $(nc -zv  localhost 4242); then
    echo '"quit"' | websocat ws://localhost:4242
  else
    echo "Killed at attempt ${i}"
    exit 0
  fi
done;
