<p align=center><img src="https://raw.githubusercontent.com/cncf/artwork/master/projects/tremor/horizontal/color/tremor-horizontal-color.png" width='35%'></p>
<p align=center><a href="https://landscape.cncf.io/selected=tremor">CNCF Early Stage Sandbox Project</p>
<p align=center><a href="https://landscape.cncf.io/category=streaming-messaging&format=card-mode&grouping=category">CNCF Streaming &amp; Messaging</p>

<hr>

[![Gitpod]][gitpod-hook]

<hr>

[![Build Status]][actions-tests]
[![ARM Builds]][drone.io]
[![Quality Checks]][actions-checks]
[![License Checks]][actions-license-audit]
[![Security Checks]][actions-security-audit]
[![Coverage]][coveralls.io]
[![codecov]][codecov report]
[![Dependabot Status]][dependabot.com]
[![CII Best Practices]][bestpractices]
[![GitHub]](LICENSE)
[![Discord]][discord-invite]

[build status]: https://github.com/tremor-rs/tremor-runtime/workflows/Tests/badge.svg
[actions-tests]: https://github.com/tremor-rs/tremor-runtime/actions?query=workflow%3ATests
[quality checks]: https://github.com/tremor-rs/tremor-runtime/workflows/Checks/badge.svg
[actions-checks]: https://github.com/tremor-rs/tremor-runtime/actions?query=workflow%3AChecks
[license checks]: https://github.com/tremor-rs/tremor-runtime/workflows/License%20audit/badge.svg
[actions-license-audit]: https://github.com/tremor-rs/tremor-runtime/actions?query=workflow%3A%22License+audit%22
[security checks]: https://github.com/tremor-rs/tremor-runtime/workflows/Security%20audit/badge.svg
[actions-security-audit]: https://github.com/tremor-rs/tremor-runtime/actions?query=workflow%3A%22Security+audit%22
[coverage]: https://coveralls.io/repos/github/tremor-rs/tremor-runtime/badge.svg?branch=main
[coveralls.io]: https://coveralls.io/github/tremor-rs/tremor-runtime?branch=main
[dependabot status]: https://api.dependabot.com/badges/status?host=github&repo=tremor-rs/tremor-runtime
[dependabot.com]: https://dependabot.com
[cii best practices]: https://bestpractices.coreinfrastructure.org/projects/4356/badge
[bestpractices]: https://bestpractices.coreinfrastructure.org/projects/4356
[github]: https://img.shields.io/github/license/tremor-rs/tremor-runtime
[discord]: https://img.shields.io/discord/752801695066488843.svg?label=&logo=discord&logoColor=ffffff&color=7389D8&labelColor=6A7EC2
[discord-invite]: https://bit.ly/tremor-discord
[arm builds]: https://cloud.drone.io/api/badges/tremor-rs/tremor-runtime/status.svg
[drone.io]: https://cloud.drone.io/tremor-rs/tremor-runtime
[gitpod]: https://gitpod.io/button/open-in-gitpod.svg
[gitpod-hook]: https://gitpod.io/#https://github.com/tremor-rs/tremor-runtime
[codecov]: https://codecov.io/gh/tremor-rs/tremor-runtime/branch/main/graph/badge.svg?token=d1bhuZGcOK
[codecov report]: https://codecov.io/gh/tremor-rs/tremor-runtime

---

In short, Tremor is an event processing system. It was originally designed as a replacement for software such as [Logstash](https://www.elastic.co/products/logstash) or [Telegraf](https://www.influxdata.com/time-series-platform/telegraf/). However tremor has outgrown this singular use case by supporting more complex workflows such as aggregation, rollups, an ETL language, and a query language.

More about the [history](https://www.tremor.rs/docs/history) and [architecture](https://www.tremor.rs/docs/overview) can be found in [the documentation](https://www.tremor.rs/docs/index).

## Audience

Tremor is a real-time event processing engine built for users that have a high message volume to deal with and want to build pipelines to process, route, or limit this event stream. Tremor supports vast number of connectors to interact with: TCP, UDP, HTTP, Websockets, Kafka, Elasticsearch, S3 and many more.

### When to use Tremor

- You want to apply traffic-shaping to a high volume of incoming events
- You want to distribute events based on their contents
- You want to protect a downstream system from overload
- You wish to perform ETL like tasks on data.

### When not to use Tremor

Note: Some of those restrictions are subject to change as tremor is a growing project.

We currently do not recommend tremor where:

- Your event structure is not mappable to a JSON-like data structure.
  - If in doubt, please reach out and create a ticket so we can assist and advice
  - In many cases ( textual formats ) a [preprocessor](https://www.tremor.rs/docs/artefacts/preprocessors/), [postprocessor](https://www.tremor.rs/docs/artefacts/postprocessors/) or [codec](https://www.tremor.rs/docs/artefacts/codecs/) is sufficient and these are relatively easy to contribute.
- You need connectivity to a system, protocol or technology that is not currently supported directly or indirectly by the existing set of [connectors](https://www.tremor.rs/docs/artefacts/connectors).
  - If in doubt, please reach out and create a ticket so we can assist and advise.
- You require complex and expensive operations on your event streams like joins of huge streams. Tremor is not built for huge analytical datasets, rather for tapping into infinite datastreams at their source (e.g. k8s events, syslog, kafka).

We accept and encourage contributions no matter how small so if tremor is compelling for your use case or project, then please get in touch, reach out, raise a ticket and we're happy to collaborate and guide contributions and contributors.

### Examples

See our [Demo](demo/README.md) for a complex Tremor setup that can easily be run locally by using docker compose.

Checkout the [Recipes](https://www.tremor.rs/docs/recipes/README) on our website. Each comes with a docker compose file to run and play with without requiring lots of dependencies.

## Building

### Docker

Tremor runs in a docker image. If you wish to build a local image, clone this repository, and either run `make image` or run `docker-compose build`. Both will create an image called `tremor-runtime:latest`.

Note that since the image is building tremor in release mode it requires some serious resources. We recommend allowing docker to use at least **12 but better 16 gigabytes of memory** and as many cores as there are to spare. Depending on the system building, the image can take up to an hour.

Providing too little resources to the docker machine can destabalize the docker build process. If you're encountering logs/errors like:

```
(signal: 9, SIGKILL: kill)
# OR
ERROR: Service 'tremor' failed to build : The command '/bin/sh -c cargo build --release --all --verbose' returned a non-zero code: 101
```

It is likely that your docker resources are starved. Consider increasing your resources ([Windows](https://docs.docker.com/docker-for-windows/#resources)/[Mac](https://docs.docker.com/docker-for-mac/#resources)) before trying again, posting in Discord, or raising an issue.

### Local builds

If you are not comfortable with managing library packages on your system or don't have experience with, please use the Docker image provided above. Local builds are not supported and purely at your own risk.

For local builds, tremor requires rust 2018 (version `1.31` or later), along with all the tools needed to build rust programs. Eg: for CentOS, the packages `gcc`, `make`, `cmake`, `clang`, `openssl`, and `libstdc++` are required. For different distributions or operating systems, please install the packages accordingly.
**NOTE** AVX2, SSE4.2 or NEON are needed to build [simd-json](https://github.com/simd-lite/simd-json#cpu-target) used by tremor. So if you are building in vm, check which processor instruction are passed to it. Like `lscpu | grep Flags`
For a more detailed guide on local builds, please refer to the [tremor development docs](https://www.tremor.rs/community/development/quick-start).

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

Tremor uses YAML, or [tremor-query](https://www.tremor.rs/docs/tremor-query/index) to configure pipelines. For use in docker those should be mounted to `/etc/tremor/config`.

Custom [tremor-script](https://www.tremor.rs/docs/tremor-script/index) and [tremor-query](https://www.tremor.rs/docs/tremor-query/index) modules and libraries should be mounted to `/usr/local/share/tremor`.

### Operations

Tremor works by chaining operations that have inputs, outputs, and additional configuration. OnRamps - the operations that ingest data - take a unique role in this.

The documentation for different operations can found in the [docs](https://www.tremor.rs/docs/operations/cli). The `onramps` and `op` modules hold the relevant information.

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
