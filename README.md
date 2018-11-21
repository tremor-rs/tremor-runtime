# tremor-runtime

This tool allows to configure a pipeline that moves data from a source to a destination. The pipeline consists of multiple steps which are executed in order. Each step is configurable on a plugin basis.



## Operations

Tremor is based on chaining operations that have inputs, outputs and additional configuration. Onramps - the operations that ingest data - take a special role in this.

The documentation for different operations can be generated (and opened) by running `make doc`. The `onramps` and `op` modules hold the relevant information.

For each operation the `Config` struct defines the parameters that can be passed to configure it and the description holds additional details and examples.


## Configuration file

Tremor uses yaml to configure pipelines, the configuration file has two top level nodes:

### `onramps`

A list that define the onramops started in tremor. Each onramp lives in it's own thread. Along with it's own configuration it has the key `pipeline` that defines the pipeline data from that onramp is send to. At least one needs to be present.


```yaml
onramps:
  - onramp::file:
      file: my-file.json
      pipeline: main
```

### `pipelines`

A list of pipelines started in tremor. Each pipeline lives in it's own thread. It consists of a `name` string, that is used to identify the pipeline. A list  of `steps`, which are operations - where each steps default output is the next step in the list. Along with a set of `subpipelines` which act as named pipeline that runs in the same thread as the main pipeline.

```yaml
# ...
pipelines:
  - name: main
    steps:
      - ...
    subpipelines:
      - ...
```

### `step`

A step is a single operation within the pipeline. The list of possible operations and their configuration can be find in the generated documentation. In addition two parameters can passed to the step: `on-error`, the step to perform if an error occurs in the step, and `outputs` a list of additional outputs - commonly used for routing.

There are two special steps:
- `pipeline:<pipeline name>` sends the event to a given pipeline
- `sub:<pipeline name>` sends the event to a given subpipeline of the current pipeline

```yaml
# ...
    steps:
      - classifier::json:
          rules:
            - class: 'info'
              rule: 'short_message:"INFO"'
            - class: 'error'
              rule: 'short_message:"ERROR"'
# ...
```

### `subpipelines`

A list of subpipelines that can be called form the main pipeline. Be aware that a subpipeline can only call sub pipelines that were defined before it. It has a `name` with which it can be called. Along with `steps` which follow the same rule as `steps` in a pipeline.
```yaml
# ...
    subpipelines:
      - name: done
        steps:
          - offramp::stdout
# ...
```

## Docker

### Dependencies

`tremor-runtime` required `rust 1.30.1`

### Building

You can build the tremor docker container (`tremor-runtime`) using `make tremor-image`.

### Configuration
To run the docker container you can either provide a `CONFIG_FILE` and mount the provided file to configure tremor or pass `CONFIG` which then will be written to `tremor.yaml` (the default config_file).


## Local demo mode

Docker needs to have at least 4GB of memory.

You need to be connected to the VPN.

To demo run `make demo-images`  to build the demo containers and then `make demo-run` to run the demo containers.

To [demo with Elasticsearch and kibana](#elastic-demo) 'make demo-elastic-run'. The 'demo-run' target does not run elsticsearch or kibana. In addition a full [kitchen sink demo](#kitchen-sink-demo) that also off-ramps data to influx and provides a  .

### Design

The demo mode logically follows the flow outlined below. It reads the data from data.json.xz, sends it at a fixed rate to the `demo` bucket on Kafka and from there reads it into the tremor container to apply classification and bucketing. Finally it off-ramps statistics of the data based on those steps.

![flow](docs/demo-flow.png)

### Configuration

#### Config file

The demo con be configured in (for example) the `demo/demo.yaml` file. A abbreviated version (with the critical elements) can be seen below. In the following sections we'll quickly discuss each of the configuration options available to customize the demo.

```yaml
version: '3.3'
services:
  # ...
  loadgen:
    # ...
    environment:
      - CONFIG_FILE=/configs/loadgen-250.yaml
    # ...
  tremor:
    # ...
    environment:
      - CONFIG_FILE=/configs/tremor-all.yaml
    # ...
```


#### Tremor

Configuration lives in `demo/config`.

#### Test data

The test data is read from the `demo/data.json.xz` file. This file needs to contain 1 event (in this case a valid JSON object) per line and be compressed with `xz`. Changing this document requires re-running `make demo-images`!


#### Kitchen Sink demo

The kitchen sink adds InfluxDb, Telegraf and Grafana to the base tremor demo with Elasticsearch and Kibana

```sh
make demo-all-run
```

To inject Grafana dashboards and configure InfluxDb for monitoring bootstrap Grafana and influx
once the system stabilizes. Make sure to install the [Demo Tools](#demo-tools)  first!

```sh
make demo-all-bootstrap
```

##### Grafana

You can access [Grafana](http://localhost:3000/login) with the credentials `admin`/`tremor`. Navigate to the `Tremor Demo` dashboard.

##### Kibana

You can access [Kibana](http://localhost:5601/app/kibana). To use it first set up a new index under *Management* -> *Index Patterns*. The pattern should be `demo` and the time filter should be set to `I don't want to use the Time Filter`. After saving navigate to *Discover*.

#### Demo tools

The influx client and Telegraf can be installed locally for dev insights into the demo experience as follows:

```sh
brew install jq
brew install influxdb
brew install telegraf
```

Only Telegraf and jq is required to run the demos, the influx cli is optional (and ships with influxdb on OS X in homebrew)

#### Benchmark Framework

The tremor-runtime supports a micro-benchmarking framework via specialized on-ramp ( blaster ) and off-ramp ( blackhole )
tremor input and output adapters. Benchmarks ( via blackhole ) output high dynamic range histogram latency reports to
standard output that are compatible with HDR Histogram's plot files [service](https://hdrhistogram.github.io/HdrHistogram/plotFiles.html)

To execute a benchmark, build tremor in **release** mode and run the examples from the tremor repo base directory:

```sh
./bench2/bench0.sh
```

### Local dependencies (mac only - not officially supported)

```bash
brew install bison
brew install flex
export PATH="/usr/local/Cellar/flex/$(brew list --versions flex | tr ' ' '\n' | tail -1)/bin/:$PATH"
export PATH="/usr/local/Cellar/bison/$(brew list --versions bison | tr ' ' '\n' | tail -1)/bin:$PATH"
```
