#!/bin/sh

seconds=120

while true; do xzcat demo/data.json.xz; done | sh bench/test-$1.sh&
PID=$(echo $!)
echo PID: $PID
while !curl -s http://0.0.0.0:9898/metrics > /dev/null
do
  sleep 1
done
echo "Taking benchmarks"
for ((c=0; c<=$seconds; c++ ))
do
  curl -s http://0.0.0.0:9898/metrics | grep '^ts_input_successes '
  sleep 1
done | awk '{ print $2 }' > bench/results/$1.txt
kill $PID
pkill -P $$
