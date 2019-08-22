# Onramps

Specify how tremor connects to the outside world in order to receive from external systems.

For example, the Kafka onramp receives data from a Kafka cluster by creating a local record
consumer, connecting to a set of topics and ingesting Kafka record data.

All onramps are of the form:

```yaml
onramp:
  - id: <unique onramp id>
    type: <onramp name>
    preprocessors: # can be omitted
      - <preprocessor 1>
      - <preprocessor 2>
      - ...
    codec: <codec of the data>
    config:
      <key>: <value>
```

The [`codec`](../codecs) field is optional and if not provided will use Onramps default codec.

The `config` contains a map (key-value pairs) specific to the onramp type.

## Supported Onramps

### kafka

The Kafka onramp connects to one or more Kafka topics. It uses librdkafka to handle connections and can use the full set of [librdkaka configuration options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).

The default [codec](../codecs) is `json`.

Supported configuration options are:

* `group_id` - The Kafka consumer group id to use.
* `topics` - A list of topics to subscribe to.
* `brokers` - Broker servers to connect to. (Kafka nodes)
* `rdkafka_options` - A optional map of an option to value, where both sides need to be strings.

Example:

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
        - snotbadger
      group_id: demo
```

### udp

The ump onramp allows receiving data via UDP datagrams.

Supported configuration options are:

- `host` - The IP to listen on
- `port` - The Port to listen on

Example:

```yaml
onramp:
  - id: udp
    type: udp
    preprocessors:
      - decompress
      - gelf-chunking
      - decompress
    codec: json
    config:
      port: 12201
      host: '127.0.0.1'
```





### file

The file onramp reads the content of a file, line by line. And sends each line as an event. It has the ability to shut down the system upon completion. Files can be `xz` compressed.

The default [codec](../codecs) is `json`.

Supported configuration options are:

* `source` - The file to read from.
* `close_on_done` - Terminates tremor once the file is processed.

Example:

```yaml
onramp:
  - id: in
    type: file
    config:
      source: /my/path/to/a/file.json
```

### metronome

This sends a periodic tick down the output. It is an excelent tool to generate some test traffic to validate pipelines.

The default [codec](../codecs) is `pass`. (since we already output decoded JSON)

Supported configuration options are:

* `interval` - The interval in which events are sent in milliseconds.

Example:

```yaml
onramp:
  - id: metronome
    type: metronome
    config:
      interval: 10000
```

### blaster

NOTE: This onramp is for benchmarking use, it should not be deployed in a live production system.

The blaster onramp is built for performance testing, but it can be used for spaced out replays of events as well. Files to replay can be `xz` compressed. It will keep looping over the file.

The default [codec](../codecs) is `json`.

Supported configuration options are:

* `source` - The file to read from.
* `interval` - The interval in which events are sent in nanoseconds.
* `iters` - Number of times the file will be repeated.

Example:

```yaml
onramp:
  - id: blaster
    type: blaster
    codec: json
    config:
      source: ./demo/data/data.json.xz
```

### tcp

This listens on a specified port for inbound tcp data.

The onramp can leverage preprocessors to segment data before codecs are applied and events are forwarded
to pipelines.

The default [codec](../codecs) is `json`.

Supported configuration options are:
* `host` - The host to advertise as
* `port` - The TCP port to listen on
* `is_non_blocking` - Is the socket configured as non-blocking ( default: false )
* `ttl` - Set the socket's time-to-live ( default: 64 )

Example:

```yaml
onramp:
  - id: tcp
    type: tcp
    preprocessors:
      - base64
      - lines
    codec: json
    config:
      host: "localhost"
      port: 9000
      is_non_blocking: true
      ttl: 32
```
