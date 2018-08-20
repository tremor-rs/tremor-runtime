# Integration Testing Framework

## Writing tests

Each tests has a own folder. The name of the folder has to be `<test name>.test` The required files are:

* `README.md` - a description of the test
* `config` - a shell script that gets sourced
* `rules.json` - a json file with the rules to use for the test (can be formated)
* `in.json.xz` - the input file to use for testing - one rule per line, xz compressed.
* `out.json.xz` - the expected result of the test, xz compressed.

### config values

The following values are currently supported:

* `SHOULD_CRASH` - set to 1 if a test run is expected to fail (tremmor returning non zero)

In addition the normal tremor environment variables can be set however they should be used at with care as they will affect the test run.

## Running tests

Tests can be run as batch using `make it` or individually using the test runner under `./integration_testing/run_test.sh` with the test name (without `.test`!) passed as an argument.

Failed tests will leave the following artifacts for analysis:

* `log.txt` - The stderr output from tremor
* `gen.json` - The generated json output
* `exp.json` - The decompressed `out.json.xz` for convenience
