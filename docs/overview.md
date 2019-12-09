# Architecture Overview

This is a short architectural overview of tremor

## Scope

We cover the runtime, data and distribution model of tremor from 50,000 feet.

## Goodness of fit

Tremor is designed for high volume messaging environments and is good for:

- Man in the middle bridging - Tremor is designed to bridge asynchronous upstream sources with synchronous downstream sinks. More generally, tremor excels at intelligently bridging from sync/async to async/sync so that message flows can be classified, dimensioned, segmented, routed using user defined logic whilst providing facilities to handle back-pressure and other hard distributed systems problems on behalf of its operators.
  - For example - Log data distributed from Kafka to ElasticSearch via Logstash can introduce significant lag or delays as logstash becomes a publication bottleneck. Untimely, late delivery of log data to the network operations centre under severity zero or at/over capacity conditions reduces our ability to respond to major outage and other byzantine events in a timely fashion.
- In-flight redeployment - Tremor can be reconfigured via it's API allowing workloads to be migrated to/from new systems and logic to be retuned, reconfigured, or reconditioned without redeployment.
- Simple event processing - Tremor adopts many principles from DEBS ( Distributed Event Based Systems ), ESP ( Event Stream Processor ) and the CEP ( Complex Event Processing ) communities. However in its current state of evolution tremor has an incomplete feature set in this regard. Over time tremor MAY evolve as an ESP or CEP solution but this is an explicit non-goal of the project.

## Tremor URLs

Since tremor v0.4, all internal artefacts and running instances of **onramps**, **offramps** and **pipelines** are dynamically configurable. The introduction of a dynamically configurable deployment model has resulted in the introduction of Tremor URLs.

The tremor API is built around this URL and the configuration space it enshrines:

| Example URL                  | Description                                                  |
| ---------------------------- | ------------------------------------------------------------ |
| `tremor://localhost:9898/`   | A local tremor instance<br />* Accessible on the local host<br />* REST API on port 9898 of the local host |
| `tremor:///`                 | The current tremor instance or 'self'                        |
| `tremor:///pipeline`         | A list of pipelines                                          |
| `tremor:///pipeline/bob`     | The pipeline identified as bob                               |
| `tremor:///onramp/alice`     | The onramp identified as Alice                               |
| `tremor:///binding/talk`     | A binding that allows alice and bob to connect               |
| `tremor:///binding/talk/tls` | An active conversation or instance of a talk between alice and bob |
|                              |                                                              |

The tremor REST API and configuration file formats also follow the same URL format.

In the case of configuration, a shorthand URL form is often used. In the configuration model, we discriminate artefacts by type, so it is often sufficient to infer the `tremor:///{artefact-kind}` component when specifying ( configuring ) artefacts.

In bindings, however, we minimally need to use the full URL path component  ( for example:`/pipeline/01` ).

At this time, the full URL form is not used in the configuration model.

## Runtime model

The tremor runtime is composed of multiple internal components that communicate via queues across multiple threads of control managed/coordinated by a set of control plane actors driven by the tremor REST API.

### Threading model

Tremor is a multi-threaded client-server system.

Currently, the threading model is simplistic:

- A thread is spawned per onramp
- A thread is spawned per offramp
- A thread is spawned per pipeline
- Threads communicate via queues

This is sufficient for tremor's primary use case as a log and metrics event processing system. In this and similar cases there are, and will only ever be, a small number of live onramps, pipelines and offramps.

The threading model is very likely to evolve over time as concurrency, threading and other primitives available in the rust ecosystem mature and evolve.

### Actor model

Tremor exploits the Actor model for supervision, management and providing essential control plane services such as supporting the tremor API and lifecycle management of onramps, pipelines and offramps.

As tremor is Rust-based, we have adopted the `actix` and `actix-web` projects which provide tremor with a good enough actor based system to supervise and manage facilities within tremor that are off the performance critical path.

## Event ordering

Events generally flow from onramps, where they are ingested into the system, through pipelines, into offramps where they are pushed to external systems.

Tremor imposes causal event ordering over ingested events and processes events deterministically. This does not mean that tremor imposes a total ordering over all ingested events, however ( ugh, because that is not tractable in a distributed system ).

Events flowing into tremor from multiple onramps are considered independant. Events flowing from multiple clients into tremor are also considered independant.

However, events sent by a *specific* client through a *specific* onramp into a *passthrough* pipeline would flow through tremor in their origin order and be passed to offramps in the same *origin* order.

Requests from multiple independent sources over the same pipeline may arbitrarily interleave, but should not re-order.

In pipelines, events are processed in depth first order. Where tremor operators have no intrinsic ordering ( such as a branch split ), tremor internally *imposes* an order.

Operator's may *arbitrarily* reorder messages. For example, a windowed operator might batch multiple events into a single batch. An iteration operator could reverse the batch and forward individual unbatched events in an order that is the reverse of the original ingest order for that batch of events.

However, the engine itself does not re-order events. Events are handled and processed in a strictly deterministic order by design.

## Pipeline Model

The core processing model of tremor is based on a directed-acyclic-graph based dataflow model.

Tremor pipelines are a graph of vertices ( nodes, or operators ) with directed edges ( or connections, or links ) between operators in the graph.

Events from the outside world in a tremor pipeline can only flow in one direction from inputs ( special operators that connect pipeline operators to onramps ) via operators to outputs ( special operators that connect pipeline operators to offramps).

Operators process events and may produce __zero or many__ output events for each event processed by the operator. As operators are the primary building block of tremor processing logic they are designed for extension.

Tremor pipelines understand three different types of events:

- Data events - these are data events delivered via onramps into a pipeline or to offramps from a pipeline. Most events that flow through a tremor pipeline are of this type.
- Signal events - these are synthetic events delivered by the tremor runtime into a pipeline under certain conditions.
- Contraflow events - these are synthetic events delivered the tremor runtime into a pipeline under certain conditions that are caused by the processing of events already in a tremor system. Back-pressure events exploit contraflow.

### Dataflow

Data-flow events are the bread and butter of tremor.

These are line of business data events ingested via onramps from external upstream systems, processed through pipelines and published downstream via offramps to downstream external systems.

### SignalFlow

Transparent to pipeline authors, but visible to onramp, offramp and operator developers are signal events. Signal events are synthetic events generated by the tremor-runtime and system that can be exploited by operators for advanced event handling purposes.

### Contraflow

A core conceit with distributed event-based systems arrizes due to their typically asynchronous nature. Tremor employs a relatively novel algorithm to handle back-pressure or other events that propagate *backwards* through a pipeline.

But pipelines are directed-acyclic-graphs ( DAGs ), so how do we back-propagate events without introducing cycles?

The answer is simple:

- There can be no cycles in a DAG
- DAGS are traversed in depth-first-search ( DFS ) order
- There can be no cycles in a DAG traversed in reverse-DFS order.
- If we join a DAG d, with its mirrored ( reversed ) DAG d'
  - We get another DAG where
    - Every output in the DAG d, can continue propagating events in its reverse DAG d', without cycles though its d' mirrored input
    - Branches in DAG d, become Combinators in DAG d'
    - Combinators in DAG d, become Branches in DAG d'
    - Any back-pressure or other events detected in the processing of existing events can result in a synthetic signalling event being injected into the reverse-DAG.
  - We call the injected events 'contraflow' events because they move *backwards* against the primary data flow.
- The cost or overhead of not injecting a contraflow event is zero
- The cost or overhead of an injected contraflow event ( in tremor ) is minimised through pruning - for example - operators that are not contraflow aware do not need to receive or process contraflow events - tremor optimises for this case.
- We call the output-input pairs at the heart of contraflow the 'pivot point'

Contraflow has been used in other event processing systems and was designed /inventedby one of the members of the tremor core team ( in a previous life ).

There are many other ways to handle back-pressure ( for example: those used by Spark, Storm, Hazelcast Jet, … ) but they stink in practice, so down with that sort of thing. Contraflow is far simpler to reason about and develop verifiable systems and code against.

Although the contraflow mechanism *may* seem complex, its far simpler than back-pressure handling by almost all other reasonable mechanisms and with far fewer negative side-effects and tradeoffs.

## Runtime facilities

Tremor's runtime is composed of a number of facilities that work together to provide service.

### Conductor

The tremor API is a REST based API that allows tremor operators to manage the lifecycle of onramps, offramps and pipelines deployed into a tremor based system.

The set of facilities in the runtime that are related to service lifecycle, activation and management are often referred to collectively as the tremor conductor or tremor control plane.

These terms can be used interchangeably.

Operators CAN conduct or orchestrate one or many tremor servers through its REST based API. The REST API is implemented as an actor in the tremor runtime.

The API actor in turn interfaces with registry and repository facilities. Tremor distinguishes between artefacts and instances. Artefacts in tremor have no runtime overhead.

Artefacts in tremor are declarative specifications of:

- Onramps - An onramp specification is a specific configuration of a supported onramp kind
- Offramps - An offramp specification is a specific configuration of a supported offramp kind
- Pipelines - A pipeline specification is a specific configuration of a pipeline graph
- Bindings - A binding specification describes how onramps, pipelines and offramps should be interconnected

Artefacts can be though of analagously to code. They are a set of instructions, rules or configurations. As such they are registered with tremor via its API and stored in tremor's artefact repository.

Deployment in tremor, is achieved through a mapping artefact. The mapping artefact specifies how artefacts should be deployed into one or many runtime instances, activated, and connected to live instances of onramps or offramps.

In tremor, publishing a mapping results in instances being deployed as a side-effect. By unpublishing or deleting a mapping instances are undeployed as a side-effect.

### Metrics

Metrics in tremor are implemented as a pipeline and deployed during startup. 

Metrics are builtin and can not be undeployed.

Operators __MAY__ attach offramps to the metrics service to distribute metrics to external systems, such as InfluxDB or Kafka.

## Data model

Tremor supports unstructured data.

Data can be raw binary, JSON, MsgPack, Influx or other structures.

When data is ingested into a tremor pipeline it can be any __supported__ format.

Tremor pipeline operators however, often assume *some* structure. For heirarchic or nested formats such as JSON and MsgPack, tremor uses the `serde` serialisation and deserialisation capabilities.

Therefore, the *in-memory* format for *JSON-like* data in tremor is effectively a `simd_json::Value`. This has the advantage of allowing tremor-script to work against YAML, JSON or MsgPack data with no changes or considerations in the tremor-script based on the origin data format.

For line oriented formats such as the Influx Line Protocol, or GELF these are typically transformed to tremor's in-memory format ( currently based on `serde`).

For raw binary or other data formats, tremor provides a growing set of codecs that convert external data to tremor in-memory form or that convert tremor in-memory form to an external data format.

In general, operators and developers should *minimize* the number of encoding and decoding steps required in the transit of data through tremor or between tremor instances.

The major overhead in most tremor systems is encoding and decoding overhead. To compensate that, as as JSON is the most dominent format, we [ported](https://github.com/simd-lite/simdjson-rs) [sims-json](https://github.com/lemire/simdjson) this reduces the cost of en- and decoding significantly compared to other JSON implementations in Rust.

### Distribution model

Tremor does not ( yet ) have an out-of-the-box network protocol. A native tremor protocol is planned in the immediate / medium term.

As such, the distribution model for tremor is currently limited to the set of available onramp and offramp connectors.

### Client/Server

Tremor, in its current form, is a client-server system. Tremor exposes a synchronous blocking RESTful API over HTTP for conducting operations related to its high throughput and relatively high performance pipeline-oriented data plane.

Tremor, in the near future, will add a clustering capability making it a distributed system. Tremor will still support client-server deployments through a 'standalone' mode of clustered operation.

Tremor in 'standalone' mode can be though of as client-server or a 'cluster of one' depending on your own bias or preferences, dear reader.