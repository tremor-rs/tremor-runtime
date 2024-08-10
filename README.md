<p align=center><img src="https://raw.githubusercontent.com/cncf/artwork/master/projects/tremor/horizontal/color/tremor-horizontal-color.png" width='35%'></p>
<p align=center><a href="https://landscape.cncf.io/selected=tremor">CNCF Early Stage Sandbox Project</p>
<p align=center><a href="https://landscape.cncf.io/category=streaming-messaging&format=card-mode&grouping=category">CNCF Streaming &amp; Messaging</p>

<hr>

[![Gitpod]][gitpod-hook]

<hr>

[![Build Status]][actions-tests]
[![Quality Checks]][actions-checks]
[![License Checks]][actions-license-audit]
[![Security Checks]][actions-security-audit]
[![codecov]][codecov report]
[![Dependabot Status]][dependabot status]
[![CII Best Practices]][bestpractices]
[![GitHub]](LICENSE)
[![Discord]][discord-invite]

[build status]: https://github.com/tremor-rs/tremor-runtime/workflows/Tests/badge.svg
[actions-tests]: https://github.com/tremor-rs/tremor-runtime/actions?query=workflow%3ATests
[Quality Checks]: https://github.com/tremor-rs/tremor-runtime/workflows/Checks/badge.svg
[actions-checks]: https://github.com/tremor-rs/tremor-runtime/actions?query=workflow%3AChecks
[actions-license-audit]: https://github.com/tremor-rs/tremor-runtime/actions?query=workflow%3A%22License+audit%22
[License Checks]: https://github.com/tremor-rs/tremor-runtime/workflows/License%20audit/badge.svg
[actions-security-audit]: https://github.com/tremor-rs/tremor-runtime/actions?query=workflow%3A%22Security+audit%22
[Security Checks]: https://github.com/tremor-rs/tremor-runtime/workflows/Security%20audit/badge.svg
[dependabot status]: https://flat.badgen.net/github/dependabot/tremor-rs/tremor-runtime
[cii best practices]: https://bestpractices.coreinfrastructure.org/projects/4356/badge
[bestpractices]: https://bestpractices.coreinfrastructure.org/projects/4356
[github]: https://img.shields.io/github/license/tremor-rs/tremor-runtime
[discord]: https://img.shields.io/discord/752801695066488843.svg?label=&logo=discord&logoColor=ffffff&color=7389D8&labelColor=6A7EC2
[discord-invite]: https://bit.ly/tremor-discord
[gitpod]: https://gitpod.io/button/open-in-gitpod.svg
[gitpod-hook]: https://gitpod.io/#https://github.com/tremor-rs/tremor-runtime
[codecov]: https://codecov.io/gh/tremor-rs/tremor-runtime/branch/main/graph/badge.svg?token=d1bhuZGcOK
[codecov report]: https://codecov.io/gh/tremor-rs/tremor-runtime

---

In short, Tremor is an event- or stream-processing system. It is designed to perform well for high-volumetric data both in terms of consumption of memory and CPU resources and in terms of latency. The goal of Tremor is to be a convenient tool for the operator at the time of configuring Tremor and at runtime in a production setup. We provide our own LSP for Tremor configurations and take great care of providing insightful metrics and helpful error messages at runtime. All this while keeping the hot data-path as performant as possible.

Tremor is well suited for ETL workloads on structured and binary data (both schemaless and strictly schematic), aggregations, traffic shaping and routing purposes. 

Tremor speaks various protocols (TCP, UDP, HTTP, Websockets, DNS) and can connect to various external systems such as Kafka, Influx compatible stores, syslog, Open telemetry, Google Pubsub, Google BigQuery, S3 and many more.

* [Documentation v0.12](https://tremor.rs/docs/0.12/index)
* [Install instructions](https://tremor.rs/docs/0.12/getting-started/install)
* [Guides](https://tremor.rs/docs/0.12/guides/overview)
* [Reference](https://tremor.rs/docs/0.12/reference/index)
* [Architecture overview](https://tremor.rs/docs/0.12/about/architecture)

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

## Examples

See our [Demo](demo/README.md) for a complex Tremor setup that can easily be run locally by using docker compose.

Checkout the [Recipes](https://www.tremor.rs/docs/recipes/README) on our website. Each comes with a docker compose file to run and play with without requiring lots of dependencies.

## Packages

We do provide RPM, DEB and pre-compiled binaries (`x86_64` only) for each release.

Check out our [Releases Page](https://github.com/tremor-rs/tremor-runtime/releases).

## Docker

Docker images are published to both [Docker Hub](https://hub.docker.com/r/tremorproject/tremor) and [Github Packages Container Registry](https://ghcr.io).

| Container registry | Image name                        |
| ------------------ | --------------------------------- |
| docker.io          | `tremorproject/tremor`            |
| ghcr.io            | `tremor-rs/tremor-runtime/tremor` |

We publish our images with a set of different tags as explained below

| Image tags | Explanation                         | Example   |
| ---------- | ----------------------------------- | --------- |
| `edge`     | Tracking the `main` branch          |           |
| `latest`   | The latest release                  |           |
| `0.X.Y`    | The exact release                   | `0.12.1` |
| `0.X`      | The latest bugfix release for `0.X` | `0.12`    |
| `0`        | The latest minor release for `0`    | `0`       |

### Building the Docker Image

Tremor runs in a docker image. If you wish to build a local image, clone this repository, and either run `make image` or run `docker compose build`. Both will create an image called `tremorproject/tremor:latest`.

Note that since the image is building tremor in release mode it requires some serious resources. We recommend allowing docker to use at least **12 but better 16 gigabytes of memory** and as many cores as there are to spare. Depending on the system building, the image can take up to an hour.

Providing too little resources to the docker machine can destabilize the docker build process. If you're encountering logs/errors like:

```
(signal: 9, SIGKILL: kill)
# OR
ERROR: Service 'tremor' failed to build : The command '/bin/sh -c cargo build --release --all --verbose' returned a non-zero code: 101
```

It is likely that your docker resources are starved. Consider increasing your resources ([Windows](https://docs.docker.com/docker-for-windows/#resources)/[Mac](https://docs.docker.com/docker-for-mac/#resources)) before trying again, posting in Discord, or raising an issue.

### Running

To run `tremor` locally and introspect its docker environment, do the following:

```bash
make image
docker run tremorproject/tremor:latest
```

A local shell can be acquired by finding the container id of the running docker container and using that to attach a shell to the image.

```bash
docker ps
```

This returns:

```text
CONTAINER ID        IMAGE                            COMMAND                CREATED             STATUS              PORTS               NAMES
fa7e3b4cec86        tremorproject/tremor:latest      "/tremor-runtime.sh"   43 seconds ago      Up 42 seconds                           gracious_shannon
```

Executing a shell on that container will then give you local access:

```bash
docker exec -it fa7e3b4cec86 sh
```

## Building From Source

> :warning: Local builds are not supported and purely at your own risk. For contributing to Tremor please checkout our [Development Quick Start Guide](https://tremor.rs/docs/0.12/development/quick-start)

If you are not comfortable with managing library packages on your system or don't have experience with, please use the Docker image provided above. 

For local builds, tremor requires rust 2021 (version `1.62` or later), along with all the tools needed to build rust programs. Eg: for CentOS, the packages `gcc`, `make`, `cmake`, `clang`, `openssl`, and `libstdc++` are required. For different distributions or operating systems, please install the packages accordingly.
**NOTE** AVX2, SSE4.2 or NEON are needed to build [simd-json](https://github.com/simd-lite/simd-json#cpu-target) used by tremor. So if you are building in vm, check which processor instruction are passed to it. Like `lscpu | grep Flags`
For a more detailed guide on local builds, please refer to the [tremor development docs](https://www.tremor.rs/community/development/quick-start).

### ARM/aarch64/NEON

To run and compile with neon use:

```bash
RUSTCFLAGS="-C cpu-target=native" cargo +nightly build --features neon --all
```


## Configuration

Tremor is configured using `.troy` files written in our own [Troy](https://tremor.rs/docs/0.12/language/troy/) language.

Custom [Troy](https://tremor.rs/docs/0.12/language/troy/) modules can be loaded from any directory pointed to by the environment variable `TREMOR_PATH`.
Directory entries need to be separated by a colon `:`.

### Docker

For use in docker [Troy](https://tremor.rs/docs/0.12/language/troy/) files should be mounted to `/etc/tremor/config`.

Custom [Troy](https://tremor.rs/docs/0.12/language/troy/) modules and libraries should be mounted to `/usr/local/share/tremor`.
### Example

This very simple example will consume lines from stdin and send them to stdout.

```tremor
define flow example
flow
  # import some common pre-defined pipeline and connector definitions
  # to use here and save some typing
  use tremor::pipelines;
  use tremor::connectors;

  # create instances of the connectors and pipelines we need
  create connector console from connectors::console;
  create pipeline pass from pipelines::passthrough;

  # connect everything to form an event flow
  connect /connector/console to /pipeline/pass;
  connect /pipeline/pass to /connector/console;
end;

deploy flow example;
```

Run this example in file `example.troy` with docker:

```console
$ docker run -i -v"$PWD:/etc/tremor/config" tremorproject/tremor:latest
```

Please also look at the [demo](demo/configs/tremor/config) for a fully documented example.

For more involved examples check out our [Recipes](https://tremor.rs/docs/0.12/recipes/index).


## Local Demo

**Note**: Docker should run with at least 4GB of memory!

To demo run `make demo`, this requires the `tremorproject/tremor` image to exist on your machine.

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

The demo configuration can be inspected and changed in the `demo/configs/tremor/config/main.troy` file.
