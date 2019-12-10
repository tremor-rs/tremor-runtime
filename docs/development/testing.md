# Testing

This is a short canned synopsis of testing in the tremor project.

## Coyote

Coyote is used for testing the tremor API via `curl` and via the command line `tremor-tool`

### Install Coyote

```bash
brew install golang
go get -u github.com/landoop/coyote
```

### Running Coyote against the API

Change directory to `tremor-runtime/tremor-api`

Then, run

```bash
$GOPATH/bin/coyote -c tests/coyote.yml
```

### Running Coyote against tremor-tool

Change directory to `tremor-runtime/tremor-tool`

Then, run

```bash
$GOPATH/bin/coyote -c tests/coyote.yml
```

### Viewing Coyote HTML reports

Via a simple python server:

```bash
python3 -m http.server 8000
```

A HTML based graphical report of [the results](http://0.0.0.0:8000/coyote.html).

## EQC

EQC or 'QuickCheck' is a specification-based testing tool for Erlang supporting a test methodology called property-based testing. Programs are tested by writing properties - preconditions, postconditions and invariants. QuickCheck uses random generation to create constructive ( should pass ) and destructive ( should fail ) tests given the specified properties. This allows suitably defined specifications to cover a far greater set of use cases than would ordinarily be possible to write manually.

Further to this, QuickCheck can reduce a set of failing testcases to the minimal testcase that forces any failing test to fail its specification. This drastically reduces the amount of QA and developer time required to verify or prove a piece of code works given a suitably defined specification.

Follow the installation steps outlined [here](https://docs.csnzoo.com/tremor/eqc-api-testing/installation/)

### Start tremor

You need to start the tremor to run the tests:

```bash
cargo run -p tremor-server
```

### Running EQC

In `tremor-runtime/tremor-erl` run:

```bash
rebar3 as eqc eqc
```
