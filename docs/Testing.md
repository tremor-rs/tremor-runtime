# Testing


## Coyoto

### Installation
```bash
brew install golang
go get -u github.com/landoop/coyote
```

### Running

In `tremor-runtime/tremor-api` run

```bash
~/go/bin/coyote -c tests/coyote.yml
```

You can run `python3 -m http.server 8000` to see a graphical representation of [the results](http://0.0.0.0:8000/coyote.html).

## EQC

This requires a EQC license!

### Installation
Grab erlang and source it

```bash
brew install kerl
kerl build 21.2 21.2
kerl install 21.2 ~/kerl/21.2
. ~/kerl/21.2/activate
brew install rebar3
```

Get and install the latest EQC:

```
curl -O http://quviq-licencer.com/downloads/eqcR21.zip
unzip eqcR21.zip
cd 'Quviq QuickCheck version 1.44.1'
erl
```

Install EQC:
```erl
1> eqc_install:install().
%% ...
2> eqc:registration("xxxxxxxxxxxx").
```


### Running

In `tremor-runtime/tremor-erl` run:

```
rebar as eqc eqc
```
