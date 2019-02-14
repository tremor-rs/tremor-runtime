# Integration Testing Framework

## Background

This runner runs two sorts of tests:

* basic tests do the following:
    * start tremor with a fixed configuration
    * push fixed input through the file system onramp
    * test the output against the expected output

These tests are designed such that new features can be added with appropriate tests - and you can create new tests from a template on the command line

In addition it will run a properties based test for the file/dirs/logs on ramp.

The properties-based test will generate a set of random messages and various file create/move/rename actions and run them.

There is a trivial identity function - the sorted output should be identical to the aggregated, sorted input. The runner will test that these are identical.

## Writing basic tests

Each basic test has a own folder. The name of the folder has to be `<test name>.test` The required files are:

* `README.md` - a description of the test
* `config` - a shell script that gets sourced to customize configuration
* `rules.json` - a json file with the rules to use for the test (can be formated)
* `in.json.xz` - the input file to use for testing - one rule per line, xz compressed.
* `out.json.xz` - the expected result of the test, xz compressed.

### config values

The following values are currently supported:

* `should_crash` - set to 1 if a test run is expected to fail (tremor returning non zero)

In addition the normal tremor environment variables can be set however they should be used at with care as they will affect the test run.

## Running tests

Tests can be run as batch using `make it` or individually using the test runner under `./integration_testing/runner` with the test name (without `.test`!) passed as an argument.

Failed tests will leave the following artifacts for analysis:

* `log.txt` - The stderr output from tremor
* `gen.json` - The generated json output
* `exp.json` - The decompressed `out.json.xz` for convenience
