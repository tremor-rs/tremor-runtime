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

/// benchmarking connector
pub(crate) mod bench;
/// connector for checking guaranteed delivery and circuit breaker logic
pub(crate) mod cb;
/// Clickhouse connector
pub(crate) mod clickhouse;
/// Crononome
pub(crate) mod crononome;
/// Discord connector
pub(crate) mod discord;
/// DNS
pub(crate) mod dns;
/// Elasticsearch Connector
pub(crate) mod elastic;
/// Exit Connector
pub(crate) mod exit;
/// file connector implementation
pub(crate) mod file;
/// Google Cloud Platform
pub(crate) mod gbq;
pub(crate) mod gcl;
pub(crate) mod gcs;
pub(crate) mod gpubsub;
/// HTTP
pub(crate) mod http;
/// Kafka consumer and producer
pub(crate) mod kafka;
/// KV
pub(crate) mod kv;
/// Home of the famous metrics collector
pub(crate) mod metrics;
/// Metronome
pub(crate) mod metronome;
/// Never send any events and swallow all events it receives into the void.
pub(crate) mod null;

/// Cluster wide shared kv store
pub(crate) mod cluster_kv;
/// `WebSockets`
pub(crate) mod ws;

/// Some common things for object storage connectors like gcs and s3
pub(crate) mod object_storage;

/// `OpenTelemetry`
pub(crate) mod otel;
/// AWS S3 connectors
pub(crate) mod s3;
/// std streams connector (stdout, stderr, stdin)
pub(crate) mod stdio;
/// tcp server and client connector impls
pub(crate) mod tcp;
/// udp connector impls
pub(crate) mod udp;
/// Unix Domain socket impls
#[cfg(unix)]
pub(crate) mod unix_socket;
/// Write Ahead Log
pub(crate) mod wal;

/// One shot connector
pub(crate) mod oneshot;
