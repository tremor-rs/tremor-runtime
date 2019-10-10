# Tremor ![](https://github.com/wayfair-incubator/tremor-runtime/workflows/Rust/badge.svg) ![](https://github.com/wayfair-incubator/tremor-runtime/workflows/Security%20audit/badge.svg)

This tool allows configuring a pipeline that moves data from a source to a destination. The pipeline consists of multiple steps which are executed in order. Each step is configurable on a plugin basis.


## Audience

Tremor is built for users that have a high message volume to deal with and want to build pipelines to process, route or limit this event stream. While Tremor specializes in interacting with Kafka, other message systems should be easily pluggable.

## Use Cases

### Elastic Search data ingress and rate limiting

Tremor has been successfully used to replace logstash as a Kafka to Elastic Search ingress. In this scenario, it reduced the required compute resources by about 80% (YMMV) when decoding, classify and rate limiting the traffic. A secondary but perhaps more important effect was that tremors dynamic backpressure and rate limiting allowed the ElasticSearch system to stay healthy and current despite overwhelming amounts of logs during spikes.

### HTTP to Kafka bridge

Kafka optimizes its connection lifetime for long-lived, persistent connections. The rather long connection negotiation phase is a result of that optimization. For languages that have a short runtime this can be a disadvantage, such as PHP, or tools that only run for a short period, such as CLI tools. Tremor can be used to provide an HTTP(s) to Kafka bridge that allows putting events on a queue without the need for going through the Kafka connection setup instead only relying on HTTP as its transport.

### PHP Execution

Executing short-lived code, such as PHP scripts, against events from a queue can be challenging. It quickly results in either a poll loop or other problematic patterns. Tremor can embed runtimes, such as the PHP and then only execute the short-lived code when an event arrives.


### When to use Tremor

* You are currently using Logstash
* You have a high volume of events to handle
* You want to protect a downstream system from overload
* You have short running code, microservices or FaaS based code that you wish to connect to a queue
* **<more suggestions please, I sure have overlooked something>**

### When not to use Tremor

Note: Some of those restrictions are subject to change as tremor is a growing project. If you want to use tremor for any of the aftermentioned things and are willing to contribute to make it reallity your contributions are more then welcome.

* You require complex or advanced scripting on your events such as ruby shellouts in Logstash.
* Your events are neither JSON nor Influx line protocol encoded. (this might not an intentional limitation just the most used formats so far.)
* Your onramps or offramps are not supported. (Again this might not an intended limitation, contribution of more on or offramps are more then welcome.)

## Building

### Docker

Tremor runs in a docker image. If you wish to build a local image, clone this repository, and either run `make image` or run `docker-compose build`. Both will create an image called `tremor-runtime:latest`.

### Local builds

If you are not comfortable with managing library packages on your system or don't have experience with , please use the Docker image provided above. Local builds are not supported and purely at your own risk.

For local builds, tremor requires rust 2018 (version `1.31` or later), along with all the tools needed to build rust programs. For centos the packages `gcc`, `make`, `clang`, `openssl-static`, and `libstdc++-static` are required, for different distributions or operating systems, please install packages accordingly.

Bison version 3.0.5 or later is also required and needs to be set in the PATH variable of your system.

## Running locally

To run `tremor` locally and introspect its docker environment do the following:

```
make image
docker run tremor-runtime
```

A local shell can be gotten by finding the container id of the running docker container and using that to attach a shell to the image.

```
docker ps
```

This returns:
```
CONTAINER ID        IMAGE               COMMAND                CREATED             STATUS              PORTS               NAMES
fa7e3b4cec86        tremor-runtime      "/tremor-runtime.sh"   43 seconds ago      Up 42 seconds                           gracious_shannon
```

Executing a shell on that container will then give you local access:

```
docker exec -it 838f22d9cb98 sh
```

## Configuration file

Tremor uses YAML to configure pipelines.

### Operations

Tremor works by chaining operations that have inputs, outputs, and additional configuration. OnRamps - the operations that ingest data - take a unique role in this.

The documentation for different operations can found in in the [docs](doc/tremor_runtime/index.html). The `onramps` and `op` modules hold the relevant information.

For each operation, the `Config` struct defines the parameters that can be passed to configure it and the description holds additional details and examples.

### file sections

#### `onramps`

A list that defines the OnRamps started in tremor. Each onramp lives in a separate thread. Along with its configuration, it has the key `pipeline` that defines the pipeline data from that onramp is sent to. At least one needs to be present.


```yaml
onramps:
  - onramp::file:
      file: my-file.json
      pipeline: main
```

#### `pipelines`

A list of pipelines started in tremor. Each pipeline lives in its thread. It consists of a `name` string, that is used to identify the pipeline. A list of `steps`, which are operations - where each steps default output is the next step in the list. Along with a set of `subpipelines` which act as a named pipeline that runs in the same thread as the main pipeline.

```yaml
# ...
pipelines:
  - name: main
      steps:
        - ...
      subpipelines:
        - ...
```

#### `step`

A step is a single operation within the pipeline. The list of possible operations and their configuration can be found in the generated documentation. Also, two parameters can be passed to the step: `on-error`, the step to perform if an error occurs in step, and `outputs` a list of additional outputs - commonly used for routing.

There are two special steps:
- `pipeline::<pipeline name>` sends the event to a given pipeline
- `sub::<pipeline name>` sends the event to a given subpipeline of the current pipeline

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

#### `subpipelines`

A list of subpipelines that can be called form the main pipeline. Be aware that a subpipeline can only call subpipelines that were defined before it. It has a `name` with which it can be called. Along with `steps` which follow the same rule as `steps` in a pipeline.

```yaml
# ...
subpipelines:
  - name: done
    steps:
      - offramp::stdout
# ...
```

### Example

Please look at the [demo](demo/configs/tremor-demo.yaml) for a fully documented example

### Configuration file usage in the docker container

To use the configuration file as part of the Docker container, you can either mount the file and point the `CONFIG_FILE` to its location or define the `CONFIG` environment variable with the content of the file, and it will be created from this on startup. Note: If `CONFIG` is set `CONFIG_FILE` will be ignored.


## Local demo mode

**Note**: Docker should run with at least 4GB of memory!

To demo run `make demo`, this requires the tremor-runtime image to exist on your machine.

### Design

The demo mode logically follows the flow outlined below. It reads the data from data.json.xz, sends it at a fixed rate to the `demo` bucket on Kafka and from there reads it into the tremor container to apply classification and bucketing. Finally it off-ramps statistics of the data based on those steps.

```
╔════════════════════╗   ╔════════════════════╗   ╔════════════════════╗
║      loadgen       ║   ║       Kafka        ║   ║       tremor       ║
║ ╔════════════════╗ ║   ║ ┌────────────────┐ ║   ║ ┌────────────────┐ ║
║ ║ tremor-runtime ║─╬───╬▶│  bucket: demo  │─╬───╬▶│ tremor-runtime │ ║
║ ╚════════════════╝ ║   ║ └────────────────┘ ║   ║ └────────────────┘ ║
║          ▲         ║   ╚════════════════════╝   ║          │         ║
║          │         ║                            ║          │         ║
║          │         ║                            ║          ▼         ║
║ ┌────────────────┐ ║                            ║ ┌────────────────┐ ║
║ │  data.json.xz  │ ║                            ║ │     tremor     │ ║
║ └────────────────┘ ║                            ║ └────────────────┘ ║
╚════════════════════╝                            ║          │         ║
                                                  ║          │         ║
                                                  ║          ▼         ║
                                                  ║ ┌────────────────┐ ║
                                                  ║ │    grouping    │ ║
                                                  ║ └────────────────┘ ║
                                                  ║          │         ║
                                                  ║          │         ║
                                                  ║          ▼         ║
                                                  ║ ┌────────────────┐ ║
                                                  ║ │  stats output  │ ║
                                                  ║ └────────────────┘ ║
                                                  ╚════════════════════╝
```

### Configuration

#### Config file

The demo can be configured in (for example) the `demo/demo.yaml` file. An abbreviated version (with the critical elements) can be seen below. In the following sections, we'll quickly discuss each of the configuration options available to customize the demo.

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
      - CONFIG_FILE=/configs/tremor-demo.yaml
# ...
```

#### Tremor

Configuration lives in `demo/config`.

#### Test data

The test data is read from the `demo/data/data.json.xz` file. This file needs to contain 1 event (in this case a valid JSON object) per line and be compressed with `xz`.

#### Benchmark Framework

The tremor-runtime supports a micro-benchmarking framework via specialized on-ramp ( blaster ) and off-ramp ( blackhole )
Tremor input and output adapters. Benchmarks ( via blackhole ) output high dynamic range histogram latency reports to
standard output that is compatible with HDR Histogram's plot files [service](https://hdrhistogram.github.io/HdrHistogram/plotFiles.html)

To execute a benchmark, build tremor in **release** mode and run the examples from the tremor repo base directory:

```sh
./bench/bench0.sh
```
