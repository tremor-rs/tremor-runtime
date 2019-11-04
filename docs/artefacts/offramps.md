# Offramps

Specify how tremor connects to the outside world in order to publish to external systems.

For example, the Elastic offramp pushes data to ElasticSearch via its bulk upload REST/HTTP API endpoint.

All offramps are specified of the form:

```yaml
offramp:
  - id: <unique offramp id>
    type: <offramp name>
    codec: <codec of the data>
    config:
      <key>: <value>
```

## System Offramps

Each tremor runtime comes with some pre-configured offramos that can be used.

### system::stdout

The offramp `/offramp/system::stdout/system` can be used to print to STDOUT. Data will be formatted as JSON.

### system::sderr

The offramp `/offramp/system::stderr/system` can be used to print to STDERR. Data will be formatted as JSON.

## Supported Offramps

### elastic

The elastic offramp writes to one or more ElasticSearch hosts. This is currently tested wiht ES v6.

Supported configuration options are:

* `endpoints` - A list of elastic search endpoints to send to.
* `concurrency` - Maximum number of parallel requests (default: 4).

Used metadata Variables:

* `index` - The index to write to (required).
* `doc_type` - The document type for elastic (required).
* `pipeline` - The elastic search pipeline to use (optional).

Example:

```yaml
offramp:
  - id: es
    type: elastic
    config:
      endpoints:
        - http://elastic:9200
```

### kafka

The Kafka offramp connects sends events to a Kafka topics. It uses librdkafka to handle connections and can use the full set of [librdkaka configuration options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).

The default codec is `json`.

Supported configuration options are:

* `topic` - The topic to send to.
* `brokers` - Broker servers to connect to. (Kafka nodes)
* `threads` - Number of threads to use for the Kafka offramp. (default: `4`)
* `hostname` - Hostname to identify the client with. (default: the systems hostname)
* `rdkafka_options` - An optional map of option to value, where both sides need to be strings.

Example:

```yaml
offramp:
  - id: kafka-out
    type: kafka
    config:
      brokers:
        - kafka:9092
      topic: demo
```

### ws

Sends events over a websocket connection. Each event is a websocket message.

The default [codec](../codecs) is `json`.

Supported configuration options are:

* `url` - Websocket endpoint to send data to.
* `binary` - If data should be send as binary instead of text (default: `fase`).

Example:

```yaml
onramp:
  - id: ws
    type: ws
    config:
      url: "ws://localhost:1234"
```

### udp

The UDP offramp sends data to a given host and port as UDP datagram.

The default codec is `json`.

When the UDP onramp gets a batch of messages it will send each element of the batch as a own UDP datagram.

Supported configuration options are:

* `host` - the local host to send data from
* `port` - the local port to send data from
* `dst_host` - the destination host to send data to
* `dst_port` - the destination port to send data to.

Example:

```yaml
offramp:
  - id: udp-out
    type: udp
    config:
      host: '10.11.12.13'
      port: 1234
      dst_host: '20.21.22.23'
      dst_port: 2345
```

### REST - Representational State Transfer

The REST offramp is used to send events or batches of events to a REST endpoint either via a `POST` or `PUT` request. By default, a `POST` request is used. Batched events are send in a single request.

Supprted configuration options are:

* `endpoints` - A vector of URLs to send the data to.
* `concurrency` - Number of paralel in flight requests (default: `4`)
* `put` - If a `PUT` request should be used istead of `POST` (default: `false`)
* `headers` - A map of headers to set for the requests

### REST offramp example for InfluxDB

Structure is given for context.

```yaml
offramp:
  - id: influxdb
    type: rest
    codec: influx
    config:
      endpoints:
        - http://influx/write?db=metrics
      headers:
        'Client': 'Tremor'
```

### file

The file offramp writes events to a file, one event per line. The file is overwritten if it exists.

The default codec is `json`.

Supported configuration options are:

* `file` - The file to write to.

Example:

```yaml
offramp:
  - id: in
    type: file
    config:
      file: /my/path/to/a/file.json
```

### stdout

The standard out offramp prints each event to the stdout output.

This operator does not support configuration.

Example:

```yaml
offramp:
  - id: console
    type: stdout
```

### blackhole

The blackhole offramp is used for benchmarking it takes measurements of the end to end times of each event traversing the pipeline and at the end prints a HDR ( High Dynamic Range ) [histogram](http://hdrhistogram.org/).

Supported configuration options are:

* `warmup_secs` - Number of seconds after startup in which latency won't be measured to allow for a warmup period.
* `stop_after_secs` - Stop tremor after a given number of seconds and print the histogram.
* `significant_figures` - Significant figures for the HDR histogram. (the first digits of each measurement that are kept as precise values)

Example:

```yaml
offramp:
  - id: bh
    type: blackhole
    config:
      warmup_secs: 10
      stop_after_secs: 40
```

### debug

The debug offramp is used to get an overview of how many events are put in wich classification.

This operator does not support configuration.

Used metadata Variables:

* `$class` - Class of the event to count by. (optional)

Example:

```yaml
offramp:
  - id: dbg
    type: debug
```

### tcp

This connects on a specified port for distributing outbound tcp data.

The offramp can leverage postprocessors to frame data after codecs are applied and events are forwarded
to external TCP protocol distribution endpoints.

The default [codec](../codecs) is `json`.

Supported configuration options are:

* `host` - The host to advertise as
* `port` - The TCP port to listen on
* `is_non_blocking` - Is the socket configured as non-blocking ( default: false )
* `ttl` - Set the socket's time-to-live ( default: 64 )
* `is_no_delay` - Set the socket's nagle ( delay ) algorithm to disabled ( default: true )

Example:

```yaml
offramp:
  - id: tcp
    type: tcp
    codec: json
    postprocessors:
      - gzip
      - base64
      - lines
    config:
      host: "localhost"
      port: 9000
```
