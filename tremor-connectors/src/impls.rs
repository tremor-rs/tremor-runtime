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
#[cfg(feature = "bench")]
pub mod bench;
/// connector for checking guaranteed delivery and circuit breaker logic
#[cfg(feature = "circut-breaker")]
pub mod cb;
/// Exit Connector
#[cfg(feature = "exit")]
pub mod exit;

/// Clickhouse connector
#[cfg(feature = "clickhouse")]
pub mod clickhouse;
/// Crononome
#[cfg(feature = "crononome")]
pub mod crononome;
/// Discord connector
#[cfg(feature = "discord")]
pub mod discord;
/// DNS
#[cfg(feature = "dns")]
pub mod dns;
/// Elasticsearch Connector
#[cfg(feature = "elasticsearch")]
pub mod elastic;

/// file connector implementation
#[cfg(feature = "file")]
pub mod file;
/// Google Cloud Platform
#[cfg(feature = "gcp")]
pub mod gbq;
#[cfg(feature = "gcp")]
pub mod gcl;
#[cfg(feature = "gcp")]
pub mod gcs;
#[cfg(feature = "gcp")]
pub mod gpubsub;
/// HTTP
#[cfg(feature = "http")]
pub mod http;
/// Kafka consumer and producer
#[cfg(feature = "kafka")]
pub mod kafka;
/// KV
#[cfg(feature = "kv")]
pub mod kv;
/// Home of the famous metrics collector
#[cfg(feature = "metrics")]
pub mod metrics;
/// Metronome
#[cfg(feature = "metronome")]
pub mod metronome;
/// Never send any events and swallow all events it receives into the void.
#[cfg(feature = "null")]
pub mod null;

/// `WebSockets`
#[cfg(feature = "websocket")]
pub mod ws;

/// `OpenTelemetry`
#[cfg(feature = "otel")]
pub mod otel;
/// AWS S3 connectors
#[cfg(feature = "aws")]
pub mod s3;
/// std streams connector (stdout, stderr, stdin)
#[cfg(feature = "stdio")]
pub mod stdio;
/// tcp server and client connector impls
#[cfg(feature = "tcp")]
pub mod tcp;
/// udp connector impls
#[cfg(feature = "udp")]
pub mod udp;
/// Unix Domain socket impls
#[cfg(all(unix, feature = "unix-socket"))]
pub mod unix_socket;
/// Write Ahead Log
#[cfg(feature = "wal")]
pub mod wal;
