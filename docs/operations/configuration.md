# Configuring Tremor

This is a short canned synopsis of tremor configuration.

Tremor supports dynamic reconfiguration since v0.4.

# Introduction

The tremor runtime is internally structured with repositories and
registries of configurable elements or artefacts. These artefacts
can be one of:

* Onramp - Specify to tremor *how* to connect to the outside world to ingest or consume
  external data. For example, the Kafka onramp consumes data from Kafka topics.

* Offramp - Specify to tremor *how* to connect to the outside world to publish data to
  external systems For example, the Elastic offramp pushes data to ElasticSearch via
  its bulk upload REST/HTTP API endpoint

* Pipeline - Specify to tremor *what* operations to perform on data ingested ( from any
  connected upstream source ) and *what* to contribute or publish downstream ( to any
  connected downstream target ).

* Binding - A binding is a specification of how to interconnect Onramps, Offramps and
  Pipelines. Binding specifications can be thought of a type of wiring harness

Specifications for onramps, offramps, pipelines and bindings in tremor should be
considered templates. They are stored in an internal tremor *repository*. A tremor
repository stores artefacts, much like git repositories qcode.

Live onramps, offramps and pipelines in tremor are in a runnable state. They consume
typically network bandwidth and some compute in the case of onramps and offramps. They
consume compute bandwidth and threads in the case of pipelines.

Live *instances* of tremor artefacts are stored in a *registry*. A tremor registry
can be thought of similarly to the Domain Name Service or DNS.

All 'live' or 'deployed' instances in tremor ( onramps, offramps, pipelines ) are
managed by a finite state machine.

# Deployment Types

In this section we explore the two basic types of deployment in tremor.

```text

+------------+      "publish"      +--------------+     "bind/deploy"     +--------------+
|            +-------------------->+              +---------------------->+              |
|            |                     |              |                       |              |
|  Artefact  |   onramp, pipeline  |   Artefact   |       mapping         |  Instance    |
|            |   offramp, binding  |  Repository  |                       |  Registry    |
|            |                     |              |                       |              |
|            +<--------------------+              +<----------------------+              |
+------------+       "find"        +--------------+    "unbind/undeploy"  +--------------+

```

Tremor leverages the registry/repository and publish-find-bind Service Oriented Architecture
patterns to drive its configuration model. Onramp, Offramp and Pipeline configurations can
be published as template specifications along with how they should be interconnected as
binding specifications.

As all artefacts in tremor are named, when a mapping is published, it deploys all the
required onramps, offramps and pipelines automatically. This also means that when a
mapping is deleted, that the corresponding live instances are undeployed.

All live or running artefacts have a corresponding state machine that manages its deployment
lifecycle. The FSM is a simplified version of the POA servant activator lifecycle from CORBA
and other Application Server Platforms.

## Static or Bootstrap deployments

Static or Bootstrap deployment allows tremor to be configured at startup with its registry and repository pre-populated with out of the box user defined configuration.

For example, in the following example, tremor is started with a
registry and repository that runs a micro benchmark on startup

```bash
$ target/debug/tremor-runtime -c repo.yaml -m reg.yaml
```

The repository:

```yaml
# File: repo.yaml

# define a blaster that replays data from an archived JSON log file
onramp:
  - id: blaster
    type: blaster
    config:
      source: ./demo/data/data.json.xz

# define a blackhole that runs a 40 second benchmark and then stops
offramp:
  - id: blackhole
    type: blackhole
    config:
      warmup_secs: 10
      stop_after_secs: 40
      significant_figures: 2

# define a passthrough pipeline for benchmarking
pipeline:
  - id: main
    interface:
      inputs:
        - in
      outputs:
        - out
    nodes:
      - id: passthrough
        op: passthrough
    links:
      in: [ passthrough ]
      passthrough: [ out ]

# define how blaster, the pipeline under test, and blackhole interconnect
binding: 
  - id: default
    links:
      '/onramp/blaster/{instance}/out': [ '/pipeline/main/{instance}/in' ]
      '/pipeline/main/{instance}/out': [ '/offramp/blackhole/{instance}/in' ]  
```

Static deployements support multip[le configurations. You can specify multiple configurations by using a list in the config instead of specifying one item as illustrated below:

```yaml
onramp:
  - id: kafka-in
    type: kafka
    codec: json
    config:
      brokers:
        - kafka:9092
      topics:
        - demo
      group_id: demo
  - id: kafka-again
    type: kafka
    codec: json
    config:
      brokers:
        - kafka:9092
      topics:
        - snotbadger
      group_id: demo
```

This file 'loads' the repository on startup with various specifications or templates, but it doesn't do anything. For that, we need to define one or many instances for tremor to deploy. This is done in the reg.yaml file:

```yaml
mapping:
  /binding/default/01: # deployment '01'
    instance: "01"     # .. deploys, blaster/01, blackhole/01, pipeline/main/01
```


## Interactive or Operational deployments

Tremor's registry and repository can be configured dynamically via tremor's REST API, and via the tremor-cli tool. For example:

```bash
$ tremor-cli api onramp publish blaster.yaml
$ tremor-cli api offramp publish offramp.yaml
$ tremor-cli api pipeline publish main.yaml
$ tremor-cli api binding publish benchmark.yaml
```

Or via curl:

```bash
$ curl --data-binary @deployment.yaml http://localhost:9898/binding/default/01
```
