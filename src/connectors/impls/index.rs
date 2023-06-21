// Copyright 2021, The Tremor Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Note this file is just for documentation purposes

#![allow(clippy::doc_markdown)]
//! # Connectors
//!
//! This section introduces Tremor connectors.
//!
//! ## Overview
//!
//! In Tremor, a `connector` is how external event streams are integrated with tremor.
//! Connectors can be clients ( such as TCP or UDP clients ), servers ( such as an embedded HTTP or gRPC
//! server ), or services such as the `kv` and `wal` connectors or an abstraction over an API such as the
//! `discord` connector.
//!
//! ### Formats and Types
//!
//! Internally, tremor represents data as a set of heirarchic values with support for the
//! primitive types `null`, `integer`, `binary`, `boolean`, `floating point` and `string` and the
//! structural types of `array` and `record`. The syntax is backwards compatible with JSON
//! but not symmetric.
//!
//! A tremor value can include raw binary data but JSON cannot.
//!
//! Connectors translate external data formats, over external protocols to tremor internals.
//! Formats are transformed to the tremor value type system. And protocol, transport and API
//! interactions are translated to streams of event commands, event data and query streams.
//!
//! Tremor supports a suite of `codecs` for common format transformations to and from external
//! data formats such as for `json`, `yaml` or `csv`. Further, tremor can preprocess data before
//! it is transformed. So gzip compressed data can be decompressed before a codec is used to
//! convert the external data to a tremor value. If data is line delimited, the events can be
//! processed line by line.
//!
//! We call transformation from an external data format to the tremor value type `preprocessing`.
//!
//! We call transformation from the tremor value system to an external data format `postprocessing`.
//!
//! Codecs and processors can be blended together with connectors to compose a working solution.
//!
//! So, a TCP connector, with a line by line processor can produce and consume line delimited
//! batch events. The source might be `json` and the target ( also ) tcp could be `yaml` or `csv`.
//!
//! This allows for a relatively small set of `connectors`, `codecs` and `processors` to be
//! very widely applicable in production.
//!
//! ### Codecs
//!
//! Codecs convert data from external data formats to the native tremor value type system
//! and vice versa.
//!
//! Check the [codec guide](../codecs) to see the supported codecs.
//!
//! ### Preprocessors
//!
//! Preprocessor chains transform inbound chunks of streaming data before a configured
//! codec in a connector converts them to the native Tremor value type representation.
//!
//! Check the [preprocessor guide](../preprocessors) to see the supported codecs.
//!
//! ### Postprocessors
//!
//! Postprocessor chains transform outbound chunks of streaming data after a codec
//! converts them from native Tremor value type representation to the external form
//! indicated by the configured codec of a connector.
//!
//! Check the [postprocessor guide](../postprocessors) to see the supported codecs.
//!
//! ### Quality of Service
//!
//! Connectors are built so they maintain a usable connection to the system they connect to. This can be a remote server, a remote cluster, a local socket or file or a std stream.
//! When a connector encounters an error that hints at a broken connection, it attempts to re-establish the connectivity.
//! If configured, it will wait and retry if it doesn't succeed on its first attempt.
//!
//! The following connector will attempt at maximum 10 reconnects before it goes into a failing state. Between each retry it will wait for the given `interval_ms` which grows by the `growth_rate` multiplied by a random value between 0 and 1.
//! So the formula boils down to:
//!
//! ```
//! wait_interval = wait_interval * (growth_rate * random(0, 1))
//!
//! ```
//!
//! Example:
//!
//! ```tremor
//! define connector my_http from http_client
//! with
//!     reconnect = {
//!         "retry": {
//!             interval_ms: 1000,
//!             growth_rate: 2.0,
//!             max_retries: 10,
//!             randomized: true
//!         }
//!     },
//!     config = {
//!         "url": "http://localhost:80/api"
//!     }
//! end;
//! ```
//!
//! Some connectors provide transactional events, that will only be considered handled if a downstream connector handled it successfully. The runtimes Contraflow mechanism is used for propagating event acknowledgment or failure notification back to the originating connector. E.g. in case of [Kafka](./kafka.md#consumer) a successful acknowledgement of an event will commit the offset of the message that contained the successfully handled event.
//!
//!
//! ## Connector Types
//!
//!
//! Connectors can be grouped in a number of categories that roughly provide the same pattern with different implementations.
//!
//! ### Client & Server
//!
//! This are connectors that handle the kind of connections that usually are used for implementing, as the category suggests, clients and servers. [TCP](tcp), [HTTP](http), but also [WebSockets](ws) or [UNIX Sockets](unix-socket) fall into this categort.
//!
//! They all provide matching pairs of `_client` and `_server` implementations where the `_client` is the side that initiates a contact, while the `_server` is the side that awaits to be contacted.
//!
//! ### Reader & Writer
//!
//! This set of connectors deals with connections that rougly resamble files access. [File](file) is the obvious one, but others like [AWS S3](s3) follow this model.
//!
//! They generally have a `reader` and a `writer` side.
//!
//! ### Consumer & Producer
//!
//! This set of connectos connects to different messaging systems where there are two unidirecional connectiosn. One to `produce` new messages, and one to `consume` new messages.
//!
//! The most prominent example here is [Kafka Producer/Consumer](kafka).
//!
//! They generally have a `_producer` and `_consumer` implementaiton.
//!
//! ### Unique
//!
//! Some connectors don't fall into the above categories or are one sided. The [Discord](discord) or [DNS](dns) connector is a client but there is no server. The [KV](kv) or [Google Big Query](gbq) connector is a database connector, and connectors like the [WAL](wal) are different again.
//!
//! ## Development Only Connectors
//!
//! These connectors are generally intended for contributors to tremor and are
//! available only during development for debug builds of tremor.
//!
//! | Connector Name   | Description                   |
//! |------------------|-------------------------------|
//! | [`cb`](cb)       | Explain                       |
//! | [`bench`](bench) | Explain                       |
//! | [`null`](null)   | Explain                       |
//! | [`exit`](exit)   | Allow terminating the runtime |
//!
//! ## Configuration
//!
//! Connectors are configured via the deployment syntax in a Tremor `.troy` file.
//! As all other entities in Tremor, connectors need to be defined and created to be usable for forming an event flow.
//!
//! Configuration is done in the Connector Definition. Every Connector has a set of specific configuration values, that need to be provided with the `config` option. All other options are common for all Connectors.
//!
//! | Option               | Description                                                                                               | Type                                                        | Required                  | Default Value                                  |
//! |----------------------|-----------------------------------------------------------------------------------------------------------|-------------------------------------------------------------|---------------------------|------------------------------------------------|
//! | `codec`              | The [Codec] to apply to incoming raw data and outgoing events.                                            | [Codec] name as a string or [Codec] configuration as object | Depends on the Connector. |                                                |
//! | `metrics_interval_s` | The interval in seconds in which to emit metrics events to the [metrics connector](./metrics.md)          | Integer                                                     | optional                  | If omitted, no metrics events will be emitted. |
//! | `postprocessors`     | A list of [Postprocessors] to be executed on the encoded outgoing event data in the order specified here. | A list of [Postprocessor] names or configuration objects    | optional                  |                                                |
//! | `preprocessors`      | A list of [Preprocessors] to be executed on the raw incoming data in the order specified.                 | A list of [Preprocessor] names or configuration objects     | optional                  |                                                |
//! | `reconnect`          | A Reconnect configuration, defining how often and at which intervals to reconnect on connection loss      | See [Reconnect config](#reconnect-config)                   | optional                  | "none"                                         |
//!
//! ### Reconnect config
//!
//! By default Connectors will attempt to re-establish a failed connection, so that connectivity is preserved in the face of small recoverable glitches. But the connectors machinery inside the runtime also supports more involved retry strategies, because sometimes it makes sense to just wait out a small downtime.
//!
//! #### none
//!
//! Only attempt to reconnect once if an established connection fails. Do not retry. This the the **Default** `reconnect` configuration, if not explicitly specified.
//!
//! #### retry
//!
//! Attempt to reconnect when a previous attempt failed. The retries an be limited to `max_retries`, by default tremor will retry infinitely. An interval can be configured between retries, that can grow by a fixed rate or a randomized rate for implementing exponential random backoff.
//!
//! | Option      | Description                                                                           | Type                  | Required | Default Value                                                                                    |
//! |-------------|---------------------------------------------------------------------------------------|-----------------------|----------|--------------------------------------------------------------------------------------------------|
//! | interval_ms | Interval to wait between retries in milliseconds                                      | Unsigned Integer      | yes      |                                                                                                  |
//! | max_retries | The maximum number of retries to execute                                              | Unsigned Integer      | no       | If not specified, the Tremor runtime will retry until the connection could be established again. |
//! | growth_rate | The growth rate, by which the actual wait interval of the last attempt is multiplied. | Floating point number | no       | 1.5                                                                                              |
//! | randomized  | Whether or not to multiply the growth rate by a random float between 0.0 and 1.0 to introduce randomized wait intervals between retries | boolean               | no       | true |                                                                                             |
//!
//! ##### Example
//!
//! ```tremor
//! define connector my_file from tcp_client
//! with
//!     config = {
//!         "url": "http://example.org
//!     },
//!     codec = "json",
//!     postprocessors = ["gzip"]
//!     reconnect = {
//!         "retry": {
//!             "interval_ms": 100,
//!             "max_retries": 10,
//!             "growth_rate": 2.0
//!         }
//!     }
//! end;
//! ```
//!
//! This example configuration will attempt a maximum of 10 retries (if reconnect attempts fail), after waiting for an initial 100ms which is growing by a randomized rate of 2.0, so that wait times increase but not all instances will retry at the exact same time to avoid a thundering herd problem.
//!
//! :::note
//!
//! Use the `retry` reconnect config with care! Too many retries can hurt downstream systems and hurt the overall systems liveness in case of errors.
//!
//! :::
//!
//! ---
//! sidebar_label: Common Configuration
//! ---
//!
//! # Common Configuration
//!
//! This section explains common configuration options that are used across many connectors.
//!
//! ### tls
//!
//! The `tls` configuration option is for enabling and further configuring TLS encrypted transport, both for client and server side connections.
//!
//! #### Client
//!
//! The client side `tls` configuration can be set to a record with all available configuration options:
//!
//! | Option | Description                                                                                                              | Type   | Required | Default value                                                    |
//! |--------|--------------------------------------------------------------------------------------------------------------------------|--------|----------|------------------------------------------------------------------|
//! | cafile | Path to the pem-encoded certificate file of the CA to use for verifying the server certificate                           | string | no       |                                                                  |
//! | domain | The DNS domain used to verify the server's certificate.                                                                  | string | no       | If not provided the domain from the connection URl will be used. |
//! | cert   | Path to the pem-encoded certificate (-chain) to use as client-side certificate. (`cert` and `key` must be used together) | string | no       |                                                                  |
//! | key    | Path to the private key to use together with the client-side certificate in `cert`.                                      | string | no       |                                                                  |
//!
//! Example:
//!
//! ```tremor
//! define connector http from http_client
//! with
//!     config = {
//!         "url": "http://example.org/"
//!         "tls": {
//!             "cafile": "/path/to/ca_certificates.pem",
//!             "domain": "example.org",
//!             "cert"  : "/path/to/client_certificate_chain.pem",
//!             "key"   : "/path/to/client_private_key.pem"
//!         }
//!     },
//!     codec = "string"
//! end;
//! ```
//!
//! It can also be set to just a boolean value. If set to `true`, the CA file provided by the operating system are used to verify the server certificate and the domain of the connection URL is used for verifying the server's domain.
//!
//! Example:
//!
//! ```tremor
//! define connector tcp from tcp_client
//! with
//!     config = {
//!         "url": "example.org:12345"
//!         "tls": true
//!     },
//!     codec = "binary"
//! end;
//! ```
//!
//! Used by the following connectors:
//!
//! * [tcp_client](./tcp.md#client)
//! * [ws_client](./ws.md#secure-client)
//! * [http_client](./http.md#client)
//! * [elastic](./elastic.md)
//!
//! #### Server
//!
//! The server side `tls` configuration is used to configure server-side TLS with certificate (`cert`) and private key (`key`).
//!
//! | Option | Description                                                                              | Type   | Required | Default value |
//! |--------|------------------------------------------------------------------------------------------|--------|----------|---------------|
//! | cert   | Path to the pem-encoded certificate file to use as the servers TLS certificate.          | string | yes      |               |
//! | key    | Path to the private key corresponding to the public key inside the certificate in `cert` | string | yes      |               |
//!
//! Used by the following connectors:
//!
//! * [tcp_server](./tcp.md#server)
//! * [ws_server](./ws.md#secure-server)
//! * [http_server](./http.md#server)
//!
//! ### auth
//!
//! Configuration for HTTP based connectors for setting the `Authorization` header.
//!
//! Used by connectors:
//!
//! * [http_client](./http.md#client)
//! * [elastic](./elastic.md)
//!
//! #### basic
//!
//! Implements the [`Basic` Authentication Scheme](https://datatracker.ietf.org/doc/html/rfc7617).
//!
//! Requires `username` and `password` fields.
//!
//! Example:
//!
//! ```tremor
//! define connector client from http_client
//! with
//!     codec = "json",
//!     config = {
//!         "url": "http://localhost:80/path?query",
//!         "auth": {
//!             "basic": {
//!                 "username": "snot",
//!                 "password": "badger"
//!             }
//!         }
//!     }
//! end;
//! ```
//!
//! #### bearer
//!
//! Implements [Bearer Token Authorization](https://datatracker.ietf.org/doc/html/rfc6750#section-2.1).
//!
//! It only needs the token to use as a string.
//!
//! Example:
//!
//! ```tremor
//! define connector client from elastic
//! with
//!     config = {
//!         "nodes": [
//!             "http://localhost:9200"
//!         ],
//!         "auth": {
//!             "bearer": "token"
//!         }
//!     }
//! end;
//! ```
//!
//! This will add the following header to each request:
//!
//! ```
//! Authorization: Bearer token
//! ```
//!
//! #### elastic_api_key
//!
//! Implements elasticsearch [ApiKey auth](https://www.elastic.co/guide/en/elasticsearch/reference/8.2/security-api.html#security-api-keys).
//!
//! Requires fields `id` which must contain the api key id and `api_key` which contains the api key to use.
//!
//! Example:
//!
//! ```tremor
//! define connector elastic_keyed from elastic
//! with
//!     config = {
//!         "nodes": [
//!             "http://localhost:9200"
//!         ],
//!         "auth": {
//!             "elastic_api_key": {
//!                 "id": "ZSHpKIEBc6SDIeISiRsT",
//!                 "api_key": "1lqrzNhRSUWmzuQqy333yw"
//!             }
//!         }
//!     }
//! end;
//! ```
//!
//! #### gcp
//!
//! Provides auto-renewed tokens for GCP service authentication.
//!
//! Token used is scoped to https://www.googleapis.com/auth/cloud-platform
//! Looks for credentials in the following places, preferring the first location found:
//!
//! * A JSON file whose path is specified by the GOOGLE_APPLICATION_CREDENTIALS environment variable.
//! * A JSON file in a location known to the gcloud command-line tool.
//! * On Google Compute Engine, it fetches credentials from the metadata server.
//!
//! Example:
//!
//! ```tremor
//! define connector gcp_client from http_client
//! with
//!     codec = "json",
//!     config = {
//!         "url": "http://google.api.snot",
//!         "auth": "gcp"
//!     }
//! end;
//! ```
//!
//! #### none
//!
//! No `Authorization` is used.
//!
//! ### GCP
//!
//! All gcp connectors, such as [gpubsub](./gpubsub.md), [gcs](./gcs.md), [gbq](./gbq.md) and [gcl](./gcl.md), share the same authentication configuration.
//!
//! As part of their config they have a `token` field, which can be used to configure the authentication. The `token` field can be configured in three ways:
//!
//! #### service account file
//!
//! A service account file can be used to authenticate with GCP. The file can be specified using `file` as a token option like this:
//!
//! ```tremor
//! define connector gbq from gbq_writer
//! with
//!     codec = "json",
//!     config = {
//!         "token": {"file": "/path/to/the/service_account.json"},
//!     }
//! end;
//! ```
//!
//! This mehtod is most useful when you have control over the file system of the machine running tremor.
//!
//! #### enbedded service account json
//!
//! A service account json can be embedded directly into the configuration. The json can be specified using `json` as a token option like this:
//!
//! ```tremor
//! define connector gbq from gbq_writer
//! with
//!     codec = "json",
//!     config = {
//!         "token": {
//!             "json": {
//!                 # ... service account json ...
//!             }
//!         },
//!     }
//! end;
//! ```
//!
//! This method is most useful when deploying a pipeline in a tremor cluster where the pipeline may be spun up on different machines.
//!
//! #### environment variable
//!
//! The authentication can be read from the environment variable `GOOGLE_APPLICATION_CREDENTIALS`. The environment variable should point to a service account file.
//! Alternatively, if tremor is running inside Google Cloud, the authentication will automatically use the credentials of the machine it is running on.
//!
//! ```tremor
//! define connector gbq from gbq_writer
//! with
//!     codec = "json",
//!     config = {
//!         "token": "env",
//!     }
//! end;
//! ```
//!
//! This method is most when running tremor on a single machine or container, ideally incide the google cloud without the need for multi or more then one service account to run the pipelines.
//!
//! ### Socket Options
//!
//! Socket options to set before binding or connecting a socket.
//!
//! Used by the [TCP](./tcp.md) and [UDP](./udp.md) connectors.
//!
//! #### UDP Socket Options
//!
//! | Option       | Description                                                                                                                                       | Type    | Required | Default value |
//! |--------------|---------------------------------------------------------------------------------------------------------------------------------------------------|---------|----------|---------------|
//! | SO_REUSEADDR | Allow reuse of local addresses during binding of a socket. This allows to quickly re-bind an address if the underlying socket is still lingering. | boolean | no       | true          |
//! | SO_REUSEPORT | Allows multiple sockets to be bound to the same port. Packets will be distributed across all receiving sockets.                                   | boolean | no       | false         |
//!
//! #### TCP Socket Options
//!
//! | Option       | Description                                                                                                                                       | Type    | Required | Default value |
//! |--------------|---------------------------------------------------------------------------------------------------------------------------------------------------|---------|----------|---------------|
//! | SO_REUSEADDR | Allow reuse of local addresses during binding of a socket. This allows to quickly re-bind an address if the underlying socket is still lingering. | boolean | no       | true          |
//! | SO_REUSEPORT | Allows multiple sockets to be bound to the same port. Packets will be distributed across all receiving sockets.                                   | boolean | no       | false         |
//! | TCP_NODELAY  | If set to `true`, this disables [Nagle's Algorithm](https://en.wikipedia.org/wiki/Nagle%27s_algorithm).                                           | boolean | no       | true          |
//!
//!
//!
//! [Codec]: ../codecs/index.md
//! [Postprocessors]: ../postprocessors/index.md
//! [Postprocessor]: ../postprocessors/index.md
//! [Preprocessors]: ../preprocessors/index.md
//! [Preprocessor]: ../preprocessors/index.md
