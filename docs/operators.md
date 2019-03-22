# Operators

Operators are part of the pipeline configuration. 

Operators process events and signals in the context of a pipeline. An operator, upon receiving
an event from an upstream operator or stream, MAY produce one or many events to one or many
downstream directly connected operators. An operator MAY drop events which halts any further
processing.

Operators allow the data processing capabilities of tremor to be extended or specialized without
changes to runtime behaviour, concurrency, event ordering or other aspects of a running tremor
systems.

Operators are created in the context of a pipeline and configured in the `nodes` section of
each pipeline. An operator MUST have an identifier that is unique for its owning pipeline.

Configuration is of the general form:

```yaml
pipeline:
  - id: example-pipeline
    nodes:
      - id: <pipeline unique node id>
        op: <namespace>::<opertor>
        config: 
          <config key>: <config value>
```

The `config` object is optional and only required for some operators.
Configuration consists of key / value pairs.

## runtime::tremor

The tremor script runtime that allows to modify events or their metadata. To learn more about Tremor Script please see the [related section](../tremor-script).

**Configuration options**:

* `script` - The script to execute.

**Outputs**:

* `out`

**Example**:

```yaml
- id: rt
    op: runtime::tremor
    config:
      script: |
        export index_type;

        _ { $index_type := index; }
```

## grouper::bucket

Bucket will perform a sliding window rate limiting based on event metadata. Limits are applied for every `$class`. In a `$class` each `$dimensions`  is allowed to pass `$rate` messages per second.

This operator does not support configuration.

**Metadata Variables**:

* `$class` - The class of an event. (String)
* `$rate` - Allowed events per second per class/dimension (Number)
* (Optional) `$dimensions` - The dimensions of the event. (Any)
* (Optional)`$cardinality` - the maximum number of dimensions kept track of at the same time (Number, default: `1000`)

**Outputs**:

* `out`
* `error` - Unprocessable events for example if `$class` or `$rate` are not set.
* `overflow` - Events that exceed the rate defined for them

**Example**:

```yaml
- id: group
  op: grouper::bucket
```

**Metrics**:

The bucket operator generates additional metrics. For each class the following two statistics are generated (as an example):

```json
{"measurement":"bucketing",
 "tags":{
   "action":"pass",
   "class":"test",
   "direction":"output",
   "node":"bucketing",
   "pipeline":"main",
   "port":"out"
 },
 "fields":{"count":93},
 "timestamp":1553012903452340000
}
{"measurement":"bucketing",
 "tags":{
   "action":"overflow",
   "class":"test",
   "direction":"output",
   "node":"bucketing",
   "pipeline":"main",
   "port":"out"
 },
 "fields":{"count":127},
 "timestamp":1553012903452340000
}
```

This tells us the following, up until this measurement was published in the class `test`:

* (`pass`) Passed 93 events 
* (`overflow`) Marked 127 events as overflow due to not fitting in the limit

## generic::backpressure

The backpressure operator is used to introduce delays based on downstream systems load. Longer backpressure steps are introduced every time the latency of a downstream system reached `timeout`, or an error occurs. On an successful transmission within the timeout limit, the delay is reset.

**Configuration options**:

* `timeout` - Maximum allowed 'write' time in milliseconds.
* `steps` - Array of values to delay when a we detect backpressure. (default: `[50, 100, 250, 500, 1000, 5000, 10000]`)

**Outputs**:

* `out`
* `overflow` - Events that are not let past due to active backpressure

**Example**:

```yaml
- id: bp
  op: generic::backpressure
  config:
    timeout: 100
```

## generic::batch

The batch operator is used to batch multiple events and send them in a bulk fashion. It also allows to set a timeout of how long the operator should wait for a batch to be filled.

Supported configuration options are:

* `count` - Elements per batch
* `timeout` - Maximum delay between the first element of a batch and the last element of a batch.

**Outputs**:

* `out`

**Example**:

```yaml
- id: batch
  op: generic::batch
  config:
    count: 300
```

## passthrough

Passes through the event without modifying it, used for debugging.

**Outputs**:

* `out`

**Example**:

```yaml
- id: passthrough
  op: passthrough
```

## debug::history

Generates a history entry in the event. Data is written to an array with the key provided in `name`, tagged with `"event: <op>(<event_id>)"`.

**Configuration options**:

* `op` - The operation name of this operator
* `name` - The field to store the history on

**Outputs**:

* `out`

**Example**:

```yaml
- id: history
  op: debug::history
  config:
    op: my-checkpoint
    name: event_history
```
