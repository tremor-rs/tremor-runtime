# Tremor

[![Build Status]][actions-Tests] [![Quality Checks]][actions-Checks] [![License Checks]][actions-License-audit] [![Security Checks]][actions-Security-audit] [![Code Coverage]][codecov.io] [![Dependabot Status]][dependabot.com]

[Build Status]: https://github.com/wayfair-tremor/tremor-runtime/workflows/Tests/badge.svg
[actions-Tests]: https://github.com/wayfair-tremor/tremor-runtime/actions?query=workflow%3ATests
[Quality Checks]: https://github.com/wayfair-tremor/tremor-runtime/workflows/Checks/badge.svg
[actions-Checks]: https://github.com/wayfair-tremor/tremor-runtime/actions?query=workflow%3AChecks
[License Checks]: https://github.com/wayfair-tremor/tremor-runtime/workflows/License%20audit/badge.svg
[actions-License-audit]: https://github.com/wayfair-tremor/tremor-runtime/actions?query=workflow%3A%22License+audit%22
[Security Checks]: https://github.com/wayfair-tremor/tremor-runtime/workflows/Security%20audit/badge.svg
[actions-Security-audit]: https://github.com/wayfair-tremor/tremor-runtime/actions?query=workflow%3A%22Security+audit%22
[Code Coverage]: https://codecov.io/gh/wayfair-tremor/tremor-runtime/branch/master/graph/badge.svg
[codecov.io]: https://codecov.io/gh/wayfair-tremor/tremor-runtime
[Dependabot Status]: https://api.dependabot.com/badges/status?host=github&repo=wayfair-tremor/tremor-runtime
[dependabot.com]: https://dependabot.com

---

In short, Tremor is an event processing system. It was originally designed as a replacement for software such as [Logstash](https://www.elastic.co/products/logstash) or [Telegraf](https://www.influxdata.com/time-series-platform/telegraf/). However tremor has outgrown this singular use case by supporting more complex workflows such as aggregation, rollups, an ETL language, and a query language.

More about the [history](https://docs.tremor.rs/history/) and [architecture](dochttps://docs.tremor.rs/overview/) can be found in [the documentation](https://docs.tremor.rs/).

## Audience

Tremor is built for users that have a high message volume to deal with and want to build pipelines to process, route, or limit this event stream. While Tremor specializes in interacting with [Kafka](https://kafka.apache.org), other message systems should be easily pluggable.

## Use Cases

### Elastic Search data ingress and rate-limiting

Tremor has been successfully used to replace logstash as a Kafka to Elastic Search ingress. In this scenario, it reduced the required compute resources by about 80% (YMMV) when decoding, classify, and rate-limiting the traffic. A secondary but perhaps more important effect was that tremors dynamic backpressure and rate-limiting allowed the ElasticSearch system to stay healthy and current despite overwhelming amounts of logs during spikes.

### HTTP to Kafka bridge

Kafka optimizes its connection lifetime for long-lived, persistent connections. The rather long connection negotiation phase is a result of that optimization. For languages that have a short runtime, this can be a disadvantage, such as PHP, or tools that only run for a short period, such as CLI tools. Tremor can be used to provide an HTTP(s) to Kafka bridge that allows putting events on a queue without the need for going through the Kafka connection setup instead, only relying on HTTP as its transport.

### When to use Tremor

* You are currently using software such as Logstash or Telgraf
* You have a high volume of events to handle
* You want to protect a downstream system from overload
* You wish to perform ETL like tasks on data.

### When not to use Tremor

Note: Some of those restrictions are subject to change as tremor is a growing project. If you want to use tremor for any of the aftermentioned things and are willing to contribute to make it reality your contributions are more then welcome.

* Your events structure can not be represented by JSONesque data structures. (If unsure, feel free to reach out and create a ticket and explain your use case - [codecs](https://docs.tremor.rs/artefacts/codecs/) are easy to write!)
* Your onramps or offramps are not supported. (If you still wish to use tremor please reach out and create a ticket - [onramps](https://docs.tremor.rs/artefacts/onramps) and [offramps](https://docs.tremor.rs/artefacts/offramps/) too are easy to write!)

### Example use cases

We provide some usage examples of this in the `docs/workshop` folder. Those examples include a `docker-compose.yaml` for running them and can serve as a starting point for deploying tremor.

## Building

### Docker

Tremor runs in a docker image. If you wish to build a local image, clone this repository, and either run `make image` or run `docker-compose build`. Both will create an image called `tremor-runtime:latest`.

Note that sice the image is building tremor in release mode it requires some serious resources, we recommend allowing docker to use at least **12 but better 16 gigabytes of memory** and as many cores as there are to spare. Depending on the system building, the image can take up to an hour.


### Local builds

If you are not comfortable with managing library packages on your system or don't have experience with, please use the Docker image provided above. Local builds are not supported and purely at your own risk.

For local builds, tremor requires rust 2018 (version `1.31` or later), along with all the tools needed to build rust programs. Eg: for centos, the packages `gcc`, `make`, `cmake`, `clang`, `openssl`, and `libstdc++` are required. For different distributions or operating systems, please install the packages accordingly.  
**NOTE** AVX2, SSE4.2 or NEON are needed to build [simd-json](https://github.com/simd-lite/simd-json#cpu-target) used by tremor. So if you are building in vm, check which processor instruction are passed to it. Like `lscpu | grep Flags`  
For a more detailed guide on local builds, please refer to the [tremor development docs](https://docs.tremor.rs/development/quick-start/).

## Running locally

To run `tremor` locally and introspect its docker environment, do the following:

```bash
make image
docker run tremorproject/tremor:latest
```

A local shell can be gotten by finding the container id of the running docker container and using that to attach a shell to the image.

```bash
docker ps
```

This returns:

```text
CONTAINER ID        IMAGE               COMMAND                CREATED             STATUS              PORTS               NAMES
fa7e3b4cec86        tremor-runtime      "/tremor-runtime.sh"   43 seconds ago      Up 42 seconds                           gracious_shannon
```

Executing a shell on that container will then give you local access:

```bash
docker exec -it 838f22d9cb98 sh
```

## Configuration file

Tremor uses YAML, or [tremor-query](https://docs.tremor.rs/tremor-query/) to configure pipelines. For use in docker those should be mounted to `/etc/tremor/config`.

### Operations

Tremor works by chaining operations that have inputs, outputs, and additional configuration. OnRamps - the operations that ingest data - take a unique role in this.

The documentation for different operations can found in the [docs](https://docs.tremor.rs/tremor-script/). The `onramps` and `op` modules hold the relevant information.

For each operation, the `Config` struct defines the parameters that can be passed to configure it, and the description holds additional details and examples.

### file sections

#### `onramps`

A list that defines the OnRamps started in tremor. Each onramp lives in a separate thread. Along with its configuration, it has the key `pipeline` that defines the pipeline data from that onramp is sent to. At least one needs to be present.

```yaml
onramps:
  - onramp::file:
      file: my-file.json
      pipeline: main
```

### Example

Please look at the [demo](demo/configs/tremor) for a fully documented example

### Configuration file usage in the docker container

To use the configuration file as part of the Docker container mount the configuration files to `/etc/tremor/config`.

## Local demo mode

**Note**: Docker should run with at least 4GB of memory!

To demo run `make demo`, this requires the tremor-runtime image to exist on your machine.

### Design

The demo mode logically follows the flow outlined below. It reads the data from data.json.xz, sends it at a fixed rate to the `demo` bucket on Kafka and from there reads it into the tremor container to apply classification and bucketing. Finally, it off-ramps statistics of the data based on those steps.

```text
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

The demo can be configured in (for example) the `demo/configs/tremor/config/config.yaml` file.

#### Tremor

Configuration lives in `demo/configs`.

#### Test data

The test data is read from the `demo/data/data.json.xz` file. This file needs to contain 1 event (in this case, a valid JSON object) per line and be compressed with `xz`.

#### Benchmark Framework

The tremor-runtime supports a micro-benchmarking framework via specialized on-ramp ( blaster ) and off-ramp ( blackhole )
Tremor input and output adapters. Benchmarks ( via blackhole ) output high dynamic range histogram latency reports to
standard output that is compatible with HDR Histogram's plot files [service](https://hdrhistogram.github.io/HdrHistogram/plotFiles.html)

To execute a benchmark, build tremor in **release** mode and run the examples from the tremor repo base directory:

```bash
./bench/run <name>
```

### ARM/aarch64/NEON

to run and compile with neon use:

```bash
RUSTCFLAGS="-C cpu-target=native" cargo +nightly build --features neon --all
```
