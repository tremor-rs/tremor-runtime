// Copyright 2023, The Tremor Team
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
//! The Pulsar connectors [`pulsar_consumer`](#pulsar_consumer) and [`pulsar_producer`](#pulsar_producer) provide integration with [Apache Pulsar](https://pulsar.apache.org/) and compatible.
//! Consuming from Pulsar and producing to Pulsar are handled by two separate connectors.
//!
//! ## `pulsar_producer`
//!
//! To produce events as pulsar messages, the a connector needs to be defined from the `pulsar_producer` connector type.
//!
//! ### Configuration
//!
//! It supports the following configuration options:
//!
//! | Option            | Description                                                                                                                                                      | Type                           | Required | Default Value                                               |
//! |-------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------|----------|-------------------------------------------------------------|
//! | `address`         | URLs to the cluster proxy.
//! | `topic`           | The topic to produce events to.                                                                                                                                  | string                         | yes      |                                                             |
//!
//! Example configuration for `pulsar_producer`:
//!
//! ```tremor
//!     define connector producer from pulsar_producer
//!     with
//!         # Enables metrics at a 1 second interval
//!         metrics_interval_s = 1,
//!         # event payload is serialized to JSON
//!         codec = "json",
//!
//!         # Pulsar specific producer configuration
//!         config = {
//!             # List of broker bootstrap servers
//!             "address": pulsar://127.0.0.1:6650,
//!             # the topic to send to
//!             "topic": "persistent://default/public/tremor_test"
//!         }
//!     end;
//! ```
//!
//! ### Event Metadata
//!
//! To control how the `pulsar_producer` produces events as pulsar messages, the following metadata options are available:
//!
//! ```tremor
//! let $pulsar_producer = {
//!     "key": "message_key",   # pulsar message key as string or bytes
//!     "headers": {            # message headers  
//!         "my_bytes_header": <<"badger"/binary>>,
//!         "my_string_header": "string"
//!     },
//!     "timestamp": 12345,     # message timestamp
//!     "partition": 3          # numeric partition id to publish message on
//! };
//! ```

use std::time::Duration;

use pulsar::TokioExecutor;

mod client;
pub(crate) mod producer;

const PULSAR_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

pub(crate) type PulsarProducer = pulsar::Producer<TokioExecutor>;
