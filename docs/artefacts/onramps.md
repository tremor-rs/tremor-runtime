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

The [`codec`](codecs.md) field is optional and if not provided will use Onramps default codec.

The `config` contains a map (key-value pairs) specific to the onramp type.

## Supported Onramps

### kafka

The Kafka onramp connects to one or more Kafka topics. It uses librdkafka to handle connections and can use the full set of [librdkaka configuration options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).

The default [codec](codecs.md#json) is `json`.

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

The udp onramp allows receiving data via UDP datagrams.

Supported configuration options are:

* `host` - The IP to listen on
* `port` - The Port to listen on

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

The default [codec](codecs.md#json) is `json`.

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

This sends a periodic tick downstream. It is an excellent tool to generate some test traffic to validate pipelines.

The default [codec](codecs.md#pass) is `pass`. (since we already output decoded JSON)

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

### crononome

This sends a scheduled tick down the offramp. Schedules can be one-off or repeating and use a cron-like format.

Multiple cron entries can be configured, each with a symbolic name and an optional JSON payload in addition to the cron expression.

Supported configuration options are:

* `entries` - A sequence of entries

Example

```yaml
onramp:
  - id: crononome
    type: crononome
    codec: json
    config:
      entries:
## every second
        - name: 1s
          expr: "* * * * * *"
## every 5 seconds
        - name: 5s
          expr: "0/5 * * * * *"
## every minute
        - name: 1m
          expr: "0 * * * * *"
          payload:
            snot: badger
```

Cron entries that are historic or in the past ( relative to the current UTC time ) will be ignored.
Cron entries beyond 2038 will not work due to underlying libraries ( rust, chrono, cron.rs ) suffering
from the [year 2038 problem](https://en.wikipedia.org/wiki/Year_2038_problem).

### blaster

NOTE: This onramp is for benchmarking use, it should not be deployed in a live production system.

The blaster onramp is built for performance testing, but it can be used for spaced out replays of events as well. Files to replay can be `xz` compressed. It will keep looping over the file.

The default [codec](codecs.md#json) is `json`.

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

The default [codec](codecs.md#json) is `json`.

Supported configuration options are:

* `host` - The IP to listen on
* `port` - The Port to listen on

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
```

### rest ( alpha )

The rest onramp listens on a specified port for inbound RESTful ( http ) data.

The onramp can leverage preprocessors to segment rest body content but does not currently
support codecs. Body content is presumed to be UTF-8 encoded.

Supported configuration options are:

* `host` - The host to advertise as
* `port` - The TCP port to listen on
* `resources` - A set of HTTP method / relative paths to accept
  * `path` - The ( possibly parameterized ) path for which a set of HTTP methods is acceptable
    * `allow`
      * `methods` - Array of acceptable HTTP methods for this path
      * `method` - GET, PUT, POST, PATCH, or DELETE
      * `params` - An optional set of required parameters
      * `status_code` - An override for the HTTP status code to return to with the response

Status codes:

|Method|Default status code|
|---|---|
|POST|`201`|
|DELETE|`200`|
|_other_|`204`|

Example:

```yaml
onramp:
  - id: rest
    type: rest
    preprocessors:
      - lines
    codec: json
    config:
      host: "localhost"
      port: 9000
      resources:
        - path: /write?db=test.db
          allow:
            - method: POST
              status_code: 204
```

Known limitations:

Currently paths and path parameters are neither checked nor validated, nor are required parameters.
Response status code configuration is also not currently respected. It is currently not possible to
configure rest onramps via swagger, raml or openapi configuration files.

### ws

Websocket onramp. Receiving either binary or text packages from a websocket connection. the url is: `ws://<host>:<port>/`

Supported configuration options are:

* `host` - The IP to listen on
* `port` - The Port to listen on

Example:

```yaml
onramp:
  - id: ws
    type: ws
    codec: json
    config:
      port: 12201
      host: '127.0.0.1'
```
