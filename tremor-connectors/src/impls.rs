// Copyright 2021-2024, The Tremor Team
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

#[cfg(feature = "bench")]
/// benchmarking connector
pub mod bench;

#[cfg(feature = "circut-breaker")]
/// connector for checking guaranteed delivery and circuit breaker logic
pub mod cb;

#[cfg(feature = "exit")]
/// Exit Connector
pub mod exit;

#[cfg(feature = "clickhouse")]
/// Clickhouse connector
pub mod clickhouse;

#[cfg(feature = "crononome")]
/// Crononome
pub mod crononome;

#[cfg(feature = "discord")]
/// Discord connector
pub mod discord;

#[cfg(feature = "dns")]
/// DNS
pub mod dns;

#[cfg(feature = "elasticsearch")]
/// Elasticsearch Connector
pub mod elastic;

#[cfg(feature = "file")]
/// file connector implementation
pub mod file;

#[cfg(feature = "http")]
/// HTTP
pub mod http;

#[cfg(feature = "kafka")]
/// Kafka consumer and producer
pub mod kafka;

#[cfg(feature = "kv")]
/// KV
pub mod kv;

#[cfg(feature = "metrics")]
/// Home of the famous metrics collector
pub mod metrics;

#[cfg(feature = "metronome")]
/// Metronome
pub mod metronome;

#[cfg(feature = "null")]
/// Never send any events and swallow all events it receives into the void.
pub mod null;

#[cfg(feature = "websocket")]
/// `WebSockets`
pub mod ws;

#[cfg(feature = "stdio")]
/// std streams connector (stdout, stderr, stdin)
pub mod stdio;

#[cfg(feature = "tcp")]
/// tcp server and client connector impls
pub mod tcp;

#[cfg(feature = "udp")]
/// udp connector impls
pub mod udp;

#[cfg(all(unix, feature = "unix-socket"))]
/// Unix Domain socket impls
pub mod unix_socket;

#[cfg(feature = "wal")]
/// Write Ahead Log
pub mod wal;
