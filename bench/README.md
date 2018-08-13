# Benchmakring

## test data

* We use the `../demo/data.json.xz` from the demo which contains real world traffic to simulate close to reality messages
* We use a set of rules equivalent to the rules deployed in soft-production (`../demo/rules.js`) to simulate close to reality rules


## tests

Tests are located in bench/test-<test>.sh they contain a tremor-runtime config to test with.

* stdin on-ramp is a must
* null output should be used.

## running

To run a test use `./bench/run.sh <test>` name. So to run the `test-mimir-api.sh` test use `bench/run.sh mimir-api`.

Output is generated to `bench/results-<test>.txt`.


## Methodology

The test sends the `data.json` content into the pre-configured tremor, the process is run. For 120 seconds (two minutes) the processed metrics (measured by `ts_input_successes`) are collected. the `results.txt` will show totals.

## Graphing

For graphing an example `bench/graph.r` is provided and can be used/adjusted as required.
