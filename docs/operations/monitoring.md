# Monitoring

Monitoring tremor is done using tremor itself. This has some interesting implications.

Each onramp, pipeline and offramp emits metrics on its operations into a pipeline called `/pipeline/system::metrics/system`. This allows to both write these messages to a destination system such as InfluxDB, as well as to a message queue such as Kafka.

Metrics are published to the `system::metrics` pipeline every 10 second or if an event was received by the pipeline (whatever happened later).

To enable monitoring, the `metrics_interval_s` key must be specified as part of onramp, pipeline and offramp config (depending on which metrics you want to capture), followed by the amount of seconds that should pass between emits.

Metrics are formatted following the same structure as the [Influx Codec](../artefacts/codecs.md#influx).

## Pipeline metrics

Example:
```json
{
  "measurement":"events",
  "tags":{
    "direction":"output",
    "node":"in",
    "pipeline":"tremor:///pipeline/main/01",
    "port":"out"
  },
  "fields":{"count":20},
  "timestamp":1553077007898214000
}
```

In influx format:

```influx
events,port=out,direction=output,node=in,pipeline=tremor:///pipeline/main/01 count=20 1553077007898214000
```

In this structure `measurement` is always `events` as that is what this is measuring. The number of events is always in the field `count` as we are counting them.

The `tags` section explains where this measurement was taken:

* `direction` means if this event came into the node `"input"` or came out of the node `"output"`
* `node` is the `id` of the node in a given pipeline
* `pipeline` is the `id` of the pipeline
* `port` is the point the event was received or send from

The example above measures all events that left the `in` of pipeline `main`.

In addition to the general pipeline metrics, some operators do generate their own metrics, for details please check on the documentation for the operator in question.

## Ramp metrics

```json
{
   "measurement":"ramp_events",
   "tags":{
      "port":"out",
      "ramp":"tremor:///offramp/main/01/in"
   },
   "fields":{"count":42},
   "timestamp":1576215344378248634
}
```

In influx format:

```influx
ramp_events,port=out,ramp=tremor:///offramp/main/01 count=42 1576215344378248634
```

In this structure `measurement` is always `ramp_events` as that is what this is measuring. The number of events is always in the field `count` as we are counting them.

The `tags` section explains where this measurement was taken:

* `ramp` is the `id` of the onramp/offramp
* `port` is one of `in`, `out` and `error`

The example above measures all events that were emitted out by the offramp `main`.

Notes:

* Preprocessor and codec level failures count as errors for onramp metrics.
* For onramps, count for `in` port is always zero since an event in tremor is something concrete only after the initial onramp processing. Furthermore, for stream-based onramps like tcp, the idea of counting `in` events does not make sense.
* If your pipeline is using the [batch operator](../artefacts/operators.md#genericbatch) and offramp is receiving events from it, no of events tracked at offramp is going to be dictated by the batching config.

## Operator level metrics

In addition to the metrics provided by the pipeline itself, some operators can  generate additional metrics.

The details are documented on a per operator level. Currently the following operators provide custom metrics:

* [grouper::bucket](../artefacts/operators.md#grouperbucket)

## Enriching

The metrics event holds the minimum required data to identify the event inside the instance. This has the purpose of allowing enrichment using tremors existing facilities. An example of enriching the metrics with the hostname would be the configuration:

```yaml
pipeline:
  - id: enrich
    interface:
      inputs:
        - in
      outputs:
        - out
    nodes:
      - id: enrich
        op: runtime::tremor
        config:
          script: |
            let event.tags.host = system::hostname();
            event;
    links:
      in: [ enrich ]
      enrich: [ out ]

binding:
  - id: enrich
    links:
      '/pipeline/system::metrics/system/out': [ '/pipeline/enrich/system/in' ]
      '/pipeline/enrich/system/out': [ '/offramp/system::stdout/system/in' ]
```

This will take the metrics from the metrics pipeline and execute a tremor script to add the `host` tag to each event.

## Example

Lets walk through a example to see how where and why metrics are generated. Lets configure the following configuration:

```yaml
onramp:
  - id: kafka-in-stores
    type: kafka
    codec: json
    config:
      # abbirivated
offramp:
  - id: elastic-out-stores
    type: elastic
    config:
      # abbirivated

pipeline:
  - id: main
    metrics_interval_s: 10
    interface:
      inputs:
        - in
      outputs:
        - out
    nodes:
      - id: runtime
        op: runtime::tremor
        config:
          script: |
            export class, rate, index, doc_type;
            _ { $index := string::format("{}_tremor", index_type); $doc_type := "_doc"; }
            logger_name="log_info_level" { $class := "logger_info"; $rate := 1875; return; }
            logger_name { $class := "logger"; $rate := 250; return; }
            index_type { $class := "default"; $rate := 25; return; }
      - id: bucket
        op: grouper::bucket
      - id: bp
        op: generic::backpressure
        config:
          timeout: 100
      - id: batch
        op: generic::batch
        config:
          count: 20
    links:
      in: [ runtime ]
      runtime: [ bucket ]
      bucket: [ bp ]
      bp: [ batch ]
      batch: [ out ]

binding:
  - id: stores-test
    links:
      '/onramp/kafka-in-stores/{instance}/out': [ '/pipeline/main/{instance}/in' ]
      '/pipeline/main/{instance}/out': [ '/offramp/elastic-out-stores/{instance}/in'  ]

```

```text
+---------+                                        +---------+
|  Kafka  |                                        | elastic |
+---------+                                        +---------+
     |                                                  ^
     v                                                  |
+---------+                                        +---------+
|   in    |                                        |   out   |
+----a----+                                        +----l----+
     |                                                  ^
     v                                                  |
+----b----+      +---------+      +---------+      +----k----+
| runtime c----->d bucket  f----->g   bp    i----->j  batch  |
+---------+      +----e----+      +----h----+      +---------+
                      |                |
                      |                |
                      v                v
```

Tremor instruments this pipeline on the points `a` to `l`. As follows (timestamp abbreviated by `…`, assuming `40` events were passed in):

* `a` as `events,node=in,port=out,direction=output,pipeline=tremor:///pipeline/main/01 count=40 …` counting the events entering the pipeline
* `b` as `events,node=runtime,port=in,direction=input,pipeline=tremor:///pipeline/main/01 count=40 …` as the events that make it to the runtime (should be equal to `a`)
* `c` as `events,node=runtime,port=out,direction=output,pipeline=tremor:///pipeline/main/01 count=40 …` as the events that make it out of the runtime (should be equal to `b`)
* `d` as `events,node=bucket,port=in,direction=input,pipeline=tremor:///pipeline/main/01 count=40 …` as the events that make it to the bucketer (should be equal to `c`)
* `e` as `events,node=bucket,port=overflow,direction=output,pipeline=tremor:///pipeline/main/01 count=10 …` as the events that the overflowed from the bucketer (aka they exceeded the limits in `$rate`) - we assume 10 of the 40 messages hit this criteria
* `f` as `events,node=bucket,port=out,direction=output,pipeline=tremor:///pipeline/main/01 count=30 …` as the events that make it out of the bucketer (we assume 30 here since in `e` we had 10 events overflow)
* `d` as `events,node=bp,port=in,direction=input,pipeline=tremor:///pipeline/main/01 count=30 …` as the events that make it to the  back-pressure (`bp`) step  (should be equal to `f`)
* `h` as `events,node=bp,port=overflow,direction=output,pipeline=tremor:///pipeline/main/01 count=5 …` as the events that the overflowed from the back-pressure step (aka the offramp asked is to back off a bit) - we assume 5 events fell into the back-pressure period.
* `i` as `events,node=bp,port=out,direction=output,pipeline=tremor:///pipeline/main/01 count=25 …` as the events that make it out of the back pressure step (we assume 25 here since in `h` we had 5 events overflow)
* `j` as `events,node=batch,port=in,direction=input,pipeline=tremor:///pipeline/main/01 count=25 …` as the events that make it to the batch step (should be equal to `i`)
* `k` as `events,node=batch,port=out,direction=output,pipeline=tremor:///pipeline/main/01 count=1 …` this is a tricky one, we got 25 events into this, however we batch by 20 events, so the first 20 events that come in get send out as a batch so we have `1` as a count - it should be noted that at the time of this snapshot `5` more events are currently held by the batch step but not send out. If we had a batch size of 30 defined we would see no output events here.
* `b` as `events,node=out,port=in,direction=input,pipeline=tremor:///pipeline/main/01 count=1 …` the events that make it to the output to be written, since we wrote only a single batch we get a count of `1` here as in `k`
