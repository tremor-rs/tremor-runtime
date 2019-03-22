# Testing

This is a short canned synopsis of testing in the tremor project.

## Coyote

Coyote is used for testing the tremor API via `curl` and via the command line `tremor-tool`

### Install Coyote

```bash
$ brew install golang
$ go get -u github.com/landoop/coyote
```

### Running Coyote against the API

Change directory to `tremor-runtime/tremor-api`

Then, run

```bash
$ $GOPATH/bin/coyote -c tests/coyote.yml
```

### Running Coyote against tremor-tool

Change directory to `tremor-runtime/tremor-tool`

Then, run

```bash
$ $GOPATH/bin/coyote -c tests/coyote.yml
```

### Viewing Coyote HTML reports

Via a simple python server:

```bash
$ python3 -m http.server 8000
```

A HTML based graphical report of [the results](http://0.0.0.0:8000/coyote.html).

## EQC

EQC or 'QuickCheck' is a specification-based testing tool for Erlang supporting a
test methodology called property-based testing. Programs are tested by writing
properties - preconditions, postconditions and invariants. QuickCheck uses random
generation to create constructive ( should pass ) and destructive ( should fail )
tests given the specified properties. This allows suitably defined specifications to
cover a far greater set of use cases than would ordinarily be possible to write manually.

Further to this, QuickCheck can reduce a set of failing testcases to the minimal testcase
that forces any failing test to fail its specification. This drastically reduces the amount
of QA and developer time required to verify or prove a piece of code works given a suitably
defined specification.

### Licensing

In order to function, EQC needs to be licensed.

### Install Erlang

Get and install Erlang

```bash
$ brew install kerl
$ kerl build 21.2 21.2
$ kerl install 21.2 ~/kerl/21.2
$ . ~/kerl/21.2/activate
$ brew install rebar3
```

Get and install the latest EQC:

```bash
$ curl -O http://quviq-licencer.com/downloads/eqcR21.zip
$ unzip eqcR21.zip
$ cd 'Quviq QuickCheck version 1.44.1'
$ erl
```

Install EQC via the Erlang shell:

```erl
1> eqc_install:install().
%% ...
2> eqc:registration("xxxxxxxxxxxx").
```

### Running EQC

In `tremor-runtime/tremor-erl` run:

```bash
$ rebar as eqc eqc
```
