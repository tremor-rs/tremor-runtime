// Copyright 2022, The Tremor Team
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

#![allow(clippy::doc_markdown)]
//! :::note
//!    Authentication happens over the [GCP autentication](./index.md#GCP)
//! :::
//!
//! ## `gcl_writer`
//!
//! The GCL Writer connector integrates [Google Cloud Logging](https://cloud.google.com/logging) from
//! the Google Cloud Platform [Operations Suite](https://cloud.google.com/products/operations). This connector
//! allows json logs to be published to Cloud Logging.
//!
//!
//! ## Configuration
//!
//! | option             | description                                                                                                                                                                                                                                                                                                                                        |
//! |--------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
//! | `log_name`         | The default `log_name` for this configuration or `default` if not provided.  The `log_name` can be overridden on a per-event basis in metadata                                                                                                                                                                                                     |
//! | `resource`         | A default monitored resource object that is assigned to all log entries in entries that do not specify a value for resource. A comprehensive [list](https://cloud.google.com/logging/docs/api/v2/resource-list) of resources is available and resources can be discovered via the `gcloud` client with ` gcloud logging resource-descriptors list` |
//! | `partial_success`  | This setting sets the behaviour of the connector with respect to whether valid entries should be written even if some entries in a batch set to Google Cloud Logging are invalid. Defaults to false                                                                                                                                                |
//! | `dry_run`          | This setting enables a sanity check for validating that log entries are well formed and and valid by exercising the connector to write entries where resulting entries are not persisted. Useful primarily during initial exploration and configuration or after large configuration changes as a sanity check. Defaults to false                  |
//! | `default_severity` | This setting sets a default log severity that can be overriden on a per event basis through metadata                                                                                                                                                                                                                                               |
//! | `labels`           | This setting sets a default set of labels that can be overriden on a per event basis through metadata                                                                                                                                                                                                                                              |
//! | `connect_timeout`  | The timeout in nanoseconds for connecting to the Google API                                                                                                                                                                                                                                                                                        |
//! | `request_timeout`  | The timeout in nanoseconds for each request to the Google API                                                                                                                                                                                                                                                                                      |
//! | `concurrency`      | The number of simultaneous in-flight requests ( defaults to 4 )                                                                                                                                                                                                                                                                                    |
//! | `token`              | The authentication token see [GCP autentication](./index.md#GCP)      
//!                                                                                                                                                                                                                                                                            |
//! The timeouts are in nanoseconds.
//!
//! ```tremor
//! use std::time::nanos;
//! define connector gcl_writer from gcl_writer
//! with
//!     config = {
//!         "connect_timeout": nanos::from_seconds(1), # defaults to 1 second
//!         "request_timeout: nanos::from_seconds(10),# defaults to 10 seconds
//!         "token": "env", # required  - The GCP token to use for authentication, see [GCP authentication](./index.md#GCP)
//!         # Concurrency - number of simultaneous in-flight requests ( defaults to 4 )
//!         # "concurrency" = 4,
//!     }
//! end
//! ```
//!
//! ### Metadata
//!
//! Metadata can optionally be provided on a per event basis for events flowing to this connector's sink.
//!
//! The metadata is encapsulated in the `$gcl_writer` record and is may optionally specify one or many of the
//! following fields.
//!
//! | field             | description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
//! |-------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
//! | `log_name`        | Overrides the default configured `log_name` for this event only                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
//! | `log_severity`    | Overrides the default log severity for this event only                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
//! | `resource`        | Overrides the default configured `resource`, if provided                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
//! | `insert_id`       | An optional unique identifier for the log entry. If you provide a value, then Logging considers other log entries in the same project, with the same timestamp, and with the same `insert_id` to be duplicates which are removed in a single query result. However, there are no guarantees of de-duplication in the export of logs                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
//! | `http_request`    | Optional information about the HTTP request associated with this log entry, if applicable                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
//! | `labels`          | An optional map of system-defined or user-defined key-value string pairs related to the entry                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
//! | `operation`       | Optional information about an operation associated with the log entry, if applicable                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
//! | `trace`           | Optional. The REST resource name of the trace being written to [Cloud Trace](https://cloud.google.com/trace) in association with this log entry. For example, if your trace data is stored in the Cloud project "my-trace-project" and if the service that is creating the log entry receives a trace header that includes the trace ID "12345", then the service should use "projects/my-tracing-project/traces/12345". The trace field provides the link between logs and traces. By using this field, you can navigate from a log entry to a trace.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
//! | `span_id`         | Optional. The ID of the Cloud Trace span associated with the current operation in which the log is being written. For example, if a span has the REST resource name of "projects/some-project/traces/some-trace/spans/some-span-id", then the spanId field is "some-span-id". A Span represents a single operation within a trace. Whereas a trace may involve multiple different microservices running on multiple different machines, a span generally corresponds to a single logical operation being performed in a single instance of a microservice on one specific machine. Spans are the nodes within the tree that is a trace. Applications that are instrumented for tracing will generally assign a new, unique span ID on each incoming request. It is also common to create and record additional spans corresponding to internal processing elements as well as issuing requests to dependencies. The span ID is expected to be a 16-character, hexadecimal encoding of an 8-byte array and should not be zero. It should be unique within the trace and should, ideally, be generated in a manner that is uniformly random. |
//! | `trace_sampled`   | The sampling decision of the trace associated with the log entry. True means that the trace resource name in the trace field was sampled for storage in a trace backend. False means that the trace was not sampled for storage when this log entry was written, or the sampling decision was unknown at the time. A non-sampled trace value is still useful as a request correlation identifier. The default is False                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
//! | `source_location` | Optional. Source code location information associated with the log entry, if any                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
//! | `timestamp`       | Optional. Overwrites the timestamp from ingest_ns to this value. the timestamp is provided in nanoseconds.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
//!
//!
//! #### HTTP Request metadata
//!
//! Optional related set of HTTP request data relevant to the log entry JSON payload.
//!
//! | field                                | description                                                        |
//! |--------------------------------------|--------------------------------------------------------------------|
//! | `request_method`                     | The HTTP verb for the request                                      |
//! | `request_url`                        | The URL, path and params for the request                           |
//! | `request_size`                       | The size in bytes of the request body                              |
//! | `status`                             | The status of the response to the request                          |
//! | `response_size`                      | The size in bytes of the response body                             |
//! | `user_agent`                         | The `user_agent` header value                                        |
//! | `remote_ip`                          | The recorded remote IP address, if available                       |
//! | `server_ip`                          | The server IP address, if available                                |
//! | `referer`                            | The referer, if available                                          |
//! | `latency`                            | The round trip latency in nanoseconds since epoch                  |
//! | `cache_lookup`                       | True if there was a cache lookup for the request                   |
//! | `cache_hit`                          | True if there was a cache lookup, and it was a hit                 |
//! | `cache_validated_with_origin_server` | True, if there was a validated cache lookup with the origin server |
//! | `cache_fill_bytes`                   | Bytes of the cache response, if there was a cache hit              |
//! | `protocol`                           | The effective protocol eg: websockets, grpc                        |
//!
//! #### Operation metadata
//!
//! Optional operation metadata field relevant to the log entry of the form:
//!
//! ```tremor
//! {
//!     "id": "a unique id for the operation",
//!     "producer": "id of the producer of the operation",
//!     "first": true, # is this the first of a related sequence
//!     "last": true, # is this the last of a related sequence
//! }
//! ```
//!
//! #### Source location metadata
//!
//! Optional source code location information if available of the form
//!
//! ```tremor
//! {
//!     "file": "path/to/file.rs",
//!     "line": 200,
//!     "function": "snot_badger_transformer",
//! }
//! ```
//!
//! ### Payload structure
//!
//! The event value is transformed to JSON and transmitted as a JSON Payload with the log entry and any provided optional metadata.
//!
//! ### Example
//!
//! A worked example flow that uses a metronome source to inject log events into GCP cloud logging periodically which has the basic visual structure as below.
//!
//! ```mermaid
//! graph LR
//!     A[metronome] -->|every 500ms| B(main)
//!     B -->|payload to log entry| C[gcl_writer]
//!     C -->|gRPC LogEvent message| D{GCP Cloud Logging}
//! ```
//!
//! ```troy
//! define flow main
//! flow
//!   use std::time::nanos;
//!   use tremor::connectors;
//!   use tremor::system;
//!   use integration;
//!   use google::cloud::logging as gcl;
//!
//!   # We use a metronome as an event source in this
//!   # example. We fire periodic events every 500
//!   # milliseconds
//!   define connector metronome from metronome
//!   with
//!     config = {"interval": nanos::from_millis(500)}
//!   end;
//!
//!   # Our connection to the GCP cloud logging service
//!   define connector google_cloud_logging from gcl_writer
//!   with
//!     config = {
//!       # Default log_name
//!       "log_name": "projects/my-project-id/logs/test-gcl",
//!       # If connecting external from GCP, use a global resource
//!       "resource": {
//!         "type": "global",
//!         "labels": {
//!           "project_id": "my-project-id"
//!         }
//!       },
//!
//!       # 500ms connection timeout
//!       "connect_timeout": nanos::from_millis(500),
//!
//!       # 1s request timeout
//!       "request_timeout": nanos::from_seconds(1),
//!
//!       # Use `debug` log severity by default
//!       "default_severity": gcl::severity::DEBUG,
//!
//!       # Indicate tremor version
//!       "labels": {
//!         "tremor-version": system::version()
//!       }
//!     }
//!   end;
//!   
//!   define pipeline main
//!   pipeline
//!     define script add_metadata_overrides
//!     script
//!         use std::time::nanos;
//!         use google::cloud::logging as gcl;
//!
//!         # Example of setting metadata for each log event
//!         let $gcl_writer = {
//!           # "log_name": "projects/my-project-id/logs/test-gcl2",
//!           "log_severity": gcl::severity::INFO,
//!           "insert_id": "x" + gcl::gen_trace_id_string(),
//!          "http_request": {
//!            "request_method": "GET",
//!            "request_url": "https://www.tremor.rs/",
//!            "request_size": 0,
//!            "status": 200,
//!            "response_size": 1024,
//!            "user_agent": "tremor",
//!            "remote_ip": "164.90.232.184",
//!            "server_ip": "localhost",
//!            "referer": "https://www.tremo.rs",
//!            "latency": nanos::from_millis(10),
//!          },
//!           "labels": {
//!             "tremor-override": "crash-overrun",
//!           },
//!           "operation": {
//!             "id": "snot-id-" + gcl::gen_span_id_string(),
//!             "producer": "github.com/tremor-rs/gcl_writer/test",
//!             "first": true,
//!             "last": true,
//!           },
//!           "trace": gcl::gen_trace_id_string(),
//!           "span_id": gcl::gen_span_id_string(),
//!           "trace_sampled": false,
//!           "source_location": { "file": "snot.rs", "line": 10, "function": "badger" },
//!         };
//!         event
//!     end;
//!
//!     create script add_metadata_overrides;
//!     select event from in into add_metadata_overrides;
//!     select event from add_metadata_overrides into out;
//!   end;
//!
//!   define pipeline exit
//!   pipeline    
//!     select {
//!       "exit": 0,
//!     } from in into out;
//!   end;
//!
//!
//!   create connector file from integration::write_file;
//!   create connector metronome;
//!   create connector google_cloud_logging;
//!   create pipeline main;
//!
//!
//!   connect /connector/metronome to /pipeline/main;
//!   connect /pipeline/main to /connector/google_cloud_logging;
//!   connect /pipeline/main to /connector/file;
//! end;
//! deploy flow main;
//! ```

use rand::Rng;
use tremor_script::{tremor_fn, Registry};
use tremor_value::Value;

pub(crate) mod writer;

// The span ID within the trace associated with the log entry.
//  For Trace spans, this is the same format that the Trace API v2
//  uses: a 16-character hexadecimal encoding of an 8-byte array,
// such as 000000000000004a
fn random_span_id_value(ingest_ns_seed: u64) -> Value<'static> {
    let mut rng = tremor_common::rand::make_prng(ingest_ns_seed);
    let span_id: String = (0..8)
        .map(|_| rng.gen::<u8>())
        .map(|b| format!("{b:02x}"))
        .collect();
    Value::from(span_id)
}

fn random_trace_id_value(ingest_ns_seed: u64) -> Value<'static> {
    let mut rng = tremor_common::rand::make_prng(ingest_ns_seed);
    let span_id: String = (0..16)
        .map(|_| rng.gen::<u8>())
        .map(|b| format!("{b:02x}"))
        .collect();
    Value::from(span_id)
}

/// Extend function registry with `GCP Cloud Logging` support
pub fn load(registry: &mut Registry) {
    registry
        .insert(tremor_fn! (google_logging|gen_span_id_string(ctx) {
            Ok(random_span_id_value(ctx.ingest_ns()))
        }))
        .insert(tremor_fn! (google_logging|gen_trace_id_string(ctx) {
            Ok(random_trace_id_value(ctx.ingest_ns()))
        }));
}

#[cfg(test)]
mod test {
    use super::*;
    use test_case::test_case;
    use tremor_script::{registry, EventContext};

    #[test_case("gen_span_id_string" ; "google_logging::gen_span_id_string")]
    #[test_case("gen_trace_id_string" ; "google_logging::gen_trace_id_string")]
    fn test_tremor_fns(fun_name: &str) {
        let mut ok_count = 0;
        let mut reg = registry::registry();
        load(&mut reg);

        let f = reg.find("google_logging", fun_name);

        if let Ok(f) = f {
            let context = EventContext::default();
            let r = f.invoke(&context, &[]);
            assert!(r.is_ok());
            ok_count += 1;
        }

        assert_eq!(1, ok_count);
    }
}
