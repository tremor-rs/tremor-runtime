# Configuration Walkthrough

A short canned synopsis of configuration tremor.

This guide walks through configuring tremor via its API directly and via
its command line tool 'tremor-tool'. For the API, we use 'curl' on the command
line.

## Introduction

In this walkthrough we will deploy a simple tremor pipeline that generates a periodic
sequence of messages or heartbeats every second. The solution is composed of the following
tremor artefacts:

* A metronome onramp - our periodic message generator
* A stdout offramp - an offramp that serializes to standard output useful for debugging
* A simple pipeline - we simply pass the input ( metronome events ) to our output

In this walkthrough we configure a single onramp, offramp and pipeline but many other
configurations are possible.

## Prerequisites

### Write an onramp specification

```yaml
# File: metronome-onramp.yaml
id: metronome
type: metronome
config:
  interval: 1000
```

Or, JSON:

```json
{
  "config": {
    "interval": 1000
  },
  "id": "metronome",
  "type": "metronome"
}
```

### Write an offramp specification
```yaml
# File: metronome-offramp.yaml
id: stdout
type: stdout
```

### Write a pipeline specification

```yaml
# File: metronome-pipeline.yaml
---
id: main
interface:
  inputs: [ in ]
  outputs: [ out ]
links:
  in: [ out ]
```

### Write a binding specification

In tremor pipelines have no non-deterministic side-effects.

By design, tremor does not allow onramps or offramps to be specified as a part
of a pipeline. This would couple running pipelines to external connections. For
example, to an external kafka broker and topic. This isn't bad per se, but it
would allow a configuration or programming style that allows pipelines that are
not easily distributable, clusterable or scalable.

To be clear, therefore:
* All data processed by a tremor pipeline is always ingested via an event
* All events arrive into pipelines via 'input streams', operators that link a pipeline to the outside world
* All events leave a pipeline via 'output streams', operators that link a pipeline to the outside world
* Events always traverse a pipeline in graph order ( Depth-First-Search )
* Where there is no imposed ordering ( in a branch ), tremor imposes declaration order
* Synthetic events ( signals from the tremor system runtime, or contraflow that derive from events already in-flight in a pipeline ) follow the same rules, without exception.
* All in-flight events in a pipeline are processed to completion before queued events are processed.

As a result, in order to connect onramps, offramps and pipelines , we need to link them together.
We call this set of ( required ) links a 'binding specification'. It is ok *not* to connect a
pipeline input stream or output stream. But it is not ok to not connect the subset exposed in a
binding specification.

For our simple scenario, the following will suffice:

```yaml
# File: metronome-binding.yaml
id: default
links:
  '/onramp/metronome/{instance}/out': [ '/pipeline/main/{instance}/in' ]
  '/pipeline/main/{instance}/out': [ '/offramp/stdout/{instance}/in' ]
```

## Publish via the REST API / curl

### Publish onramp specification
```bash
$ curl -vs -stderr -X POST --data-binary @metronome-onramp.yaml http://localhost:9898/onramp
```

Check that it published ok:

```bash
$ curl -vs --stderr - -H "Accept: application/yaml" http://localhost:9898/onramp
- metronome
```

### Publish offramp specification
```bash
$ curl -vs -stderr -X POST --data-binary @metronome-offramp.yaml http://localhost:9898/offramp
```

Check that it published ok:

```bash
$ curl -vs --stderr - -H "Accept: application/yaml" http://localhost:9898/offramp
- stdout
```

### Publish pipeline specification

```bash
$ curl -vs -stderr -X POST --data-binary @metronome-pipeline.yaml http://localhost:9898/pipeline
```

Check that it published ok:

```bash
$ curl -vs --stderr - -H "Accept: application/yaml" http://localhost:9898/pipeline
- main
```

### Publish binding specification

```bash
$ curl -vs -stderr -X POST --data-binary @metronome-binding.yaml http://localhost:9898/binding
```

```bash
$ curl -vs --stderr - -H "Accept: application/yaml" http://localhost:9898/binding
- default
```

### Publish offramp specification

```bash
$ curl -vs -stderr -X POST --data-binary @metronome-offramp.yaml http://localhost:9898/offramp
```

Check that it published ok:

```bash
$ curl -vs --stderr - -H "Accept: application/yaml" http://localhost:9898/offramp
- default
```

## Publish via tremor-tool

The tremor-tool command allows the exact sample set of interactions as
above. For brevity we simpilify the examples in this section but the
steps are the same.

Tremor tool, however, makes it easier to switch between JSON and YAML

### Publish all specifications

Publish onramp, offramp, pipeline and binding:

```bash
$ tremor-tool api onramp create metronome-onramp.yaml
$ tremor-tool api offramp create metronome-offramp.yaml
$ tremor-tool api pipeline create metronome-pipeline.yaml
$ tremor-tool api binding create metronome-binding.yaml
```

Check all our artefacts have published ok:

```bash
$ tremor-tool api onramp list metronome-onramp.yaml
$ tremor-tool api offramp list metronome-offramp.yaml
$ tremor-tool api pipeline list metronome-pipeline.yaml
$ tremor-tool api binding list metronome-binding.yaml
```

## Limitations

Live deployments via the API only work with a single entity and passing a list using the API isn't supported. In order to achieve that you can use ['Static or Bootstrap Deployments](./Configuration/Static or Bootstrap deployments) 



## Deployment

Once all artefacts are published into the tremor repository we are ready to
deploy. We deploy instances, via bindings, through mapping specifications.

In all steps to this point, we have been populating the tremor repository. Like
a git repository the tremor repository stores artefacts, like git stores code.

When we publish a mapping we are deploying live instances of onramps, offramps
and pipelines, in our case, we want to:

* Deploy a single metronome onramp instance
* Deploy a single stdout offramp instance
* Deploy a single passthrough pipeline
* We want the onramp to connect to the pipeline
* We want the offramp to connect to the pipeline

In our final step we specify:
* We want to call our instance 'walkthrough'

```yaml
# File: metronome-mapping.yaml
instance: 'walkthrough'
```

Deploy via curl:

```bash
$ curl -vs -stderr -X POST --data-binary @metronome-mapping.yaml -H "Content-type: application/yaml" http://localhost:9898/binding/default/walkthrough
```

Deploy via tremor-tool:

```bash
$ tremor-tool api binding activate default walkthrough metronome-mapping.yaml
```