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
//! The Kafka connectors [`kafka_consumer`](#kafka_consumer) and [`kafka_producer`](#kafka_producer) provide integration with [Apache Kafka](https://kafka.apache.org/) and compatible
//! products such as [Confluent Kafka](https://www.confluent.io/) and [Redpanda](https://redpanda.com/). Consuming from Kafka and producing to Kafka are handled by two separate connectors.
//!
//! Both Kafka connectors in Tremor are built on top of [librdkafka] version 1.8.2 and expose the full complement
//! of [configuration settings](https://github.com/edenhill/librdkafka/blob/v1.8.2/CONFIGURATION.md). Care __SHOULD__ be
//! taken when configuring `kafka` with tremor to ensure that the configuration settings make sense given the logic
//! required of the resulting system.
//!
//! ## `kafka_consumer`
//!
//! To consume from kafka, one needs to define a connector from `kafka_consumer`.
//!
//! ### Configuration
//!
//! It supports the following configuration options:
//!
//! | Option     | Description                                                                                                                                                         | Type            | Required | Default Value   |
//! |------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|----------|-----------------|
//! | `group_id` | The consumer group id to register with to the kafka cluster. Corresponds to the librdkafka `group.id` setting.                                                      | string          | yes      |                 |
//! | `topics`   | The topics to consumer from.                                                                                                                                        | list of strings | yes      |                 |
//! | `brokers`  | URLs to the cluster bootstrap servers to connect to. Corresponds to the librdkafka `bootstrap.servers` setting.                                                     | list of strings | yes      |                 |
//! | `mode`     | Determines the working mode of the connector. The following modes are supported: `performance` (Default), `transactional` and `custom`. See below for more details. | See below.      | yes      | `"performance"` |
//!
//!
//! #### Mode
//!
//! ##### Performance
//!
//! The mode describes how the connector will consume from kafka. Using the mode `"performance"`, this connector will automatically store all offsets of messages it receives and commit them to the broker every 5 secons. This mode has the lowest overhead and is best suited for performance-sensitive applications where a single missed message or a failed message isn ot a big deal. This is the default mode.
//!
//! This mode essentially sets the following [librdkafka configuration options]:
//!
//! Example:
//!
//! ```tremor
//! define connector perf_consumer from kafka_consumer
//! with
//!     codec = "json",
//!     config = {
//!         "brokers": [
//!             "localhost:9092"
//!         ],
//!         "group_id": "my_consumer_group",
//!         "topics": [
//!             "my_topic"
//!         ],
//!         "mode": "performance"
//!     }
//! end;
//! ```
//!
//! ##### Transactional
//!
//! The mode `"transactional"` is for workloads where each and every kafka message needs to be handled successfully in order to make progress. In this mode, the offset for every message
//! is only stored once it has been handled successfully by a downstream connector. The setting `commit_interval` of the mode determines how often the offsets are committed to the kafka cluster.
//! The default is 5 seconds. If this setting is set to `0`, the connector will immediately commit every single event offset directly to the kafka cluster. This will lead to lots of traffic towards the group-coordinator, and should be avoided for high volume loads. This is the safest setting though.
//!
//! Failed events will be replayed. If an event fails the partition offset of the event is reset, so the consumer will start consuming again from that point. This can lead to consuming kafka messages more than once, but it guarantees at-least-once delivery in the face of failing events.
//!
//! Example:
//!
//! ```tremor
//! use std::time::nanos;
//!
//! define connector transactional_consumer from kafka_consumer
//! with
//!     codec = "json",
//!     config = {
//!         "brokers": [
//!             "localhost:9092"
//!         ],
//!         "group_id": "my_consumer_group",
//!         "topics": [
//!             "my_topic"
//!         ],
//!         "mode": {
//!             "transactional": {
//!                 # this setting can be ommitted and defaults to 5 seconds
//!                 # if set to `0`, the connector commits offsets immediately
//!                 "commit_interval": nanos::from_seconds(1)
//!             }
//!         }
//!     }
//! end;
//! ```
//!
//! ##### Custom
//!
//! The mode `custom` allows custom configuration of the connector and the underlying `librdkafka`. It contains two settings:
//!
//! * `rdkafka_options`: librdkafka configuration options. For possible options consult the [librdkafka configuration options]. The options `group.id`, `client.id` and `bootstrap.servers` are set from the connector config and cannot be overwritten.
//! * `retry_failed_events`: If set to `true` this connector will behave as in `transactional` mode and reset the offset for every failed kafka message, effectively retrying it. Default: `false`
//!
//! In order to customize your settings, it might be useful to know how the different modes can be achieved using the `custom` mode and adapting it to your needs.
//! [Transactional](#transactional) mode translates to the following `rdkafka_options`:
//!
//! ```js
//! {
//!     "mode": {
//!         "custom": {
//!             "rdkafka_options": {
//!                 "enable.auto.commit": true,
//!                 "enable.auto.offset.store": false,
//!                 "auto.commit.interval.ms": 5000
//!             },
//!             "retry_failed_events": true
//!         }
//!     }
//! }
//! ```
//!
//! [Performance](#performance) mode translates to the following `rdkafka_options`:
//!
//! ```js
//! {
//!     "mode": {
//!         "custom": {
//!             "rdkafka_options": {
//!                 "enable.auto.commit": true,
//!                 "enable.auto.offset.store": true,
//!                 "auto.commit.interval.ms": 5000
//!             },
//!             "retry_failed_events": false
//!         }
//!     }
//! }
//! ```
//!
//! For detailed semantics on how the consumer behaves with which settings, please consult the [librdkafka configuration options].
//!
//! Example configuration for `kafka_consumer`:
//!
//! ```tremor title="config.troy"
//! use std::time::nanos;
//!
//! define connector consumer from kafka_consumer
//! with
//!     metrics_interval_s = 1,
//!     reconnect = {
//!             "retry": {
//!                 "interval_ms": 3000,
//!                 "max_retries": 10
//!             }
//!         },
//!     codec = "json",
//!     # Kafka specific consumer configuration
//!     config = {
//!         # List of broker bootstrap servers
//!         "brokers": [
//!             "127.0.0.1:9092"
//!         ],
//!         "group_id": "test1", # Consumer group id
//!
//!         # required - list of subscription topics to register with
//!         "topics": [
//!             "tremor_test"
//!         ],
//!         "mode": {
//!             "custom": {
//!                 # Whether or not to retry failed attempts
//!                 # When true - resets the offset to a failed message for retry
//!                 #  - Warning: where persistent failure is expected, this will lead to persistent errors
//!                 # When false - Only commits offset for a successful acknowledgement
//!                 "retry_failed_events": false,
//!
//!                 # librdkafka configuration settings ( indicative illustrative example )
//!                 "rdkafka_options": {
//!                     "enable.auto.commit": "false",      # this will only commit a message offset if the event has been handled successfully
//!                     "auto.commit.interval.ms": 5000,    # this will auto-commit the current offset every 5s
//!                     "enable.auto.offset.store": true,
//!                     "debug": "consumer"                 # enable librdkafka debug logging of the consumer
//!                     "enable.partition.eof": false,      # do not send an EOF if a partition becomes empty
//!                     "auto.offset.reset": "beginning",   # always start consuming from the beginning of all partitions
//!                 }
//!             }
//!         }
//!         
//!     }
//! end;
//! ```
//!
//! #### Event Metadata
//!
//! Events consumed from a `kafka_consumer` connector will have the following event metadata:
//!
//! ```js
//! {
//!     "kafka_consumer": {
//!         "key": ..., # binary message key
//!         "headers": {
//!             "kafka_message_header": ..., # binary header value
//!             ...
//!         },
//!         "topic": "my_topic",    # topic name
//!         "partition": 1,         # numeric parition id of the message
//!         "offset": 12,           # numeric message offset in the given partition
//!         "timestamp": 1230000000 # optional message timestamp in nanoseconds           
//!     }
//! }
//! ```
//!
//! It can be accessed in [scripts](../../language/scripts.md) or [`select` statements](../../language/pipelines.md) in the following way:
//!
//! ```tremor
//! match $ of
//!     case %{ present kafka_consumer } => $kafka_consumer.partition
//!     default => -1 # invalid partition
//! end;
//! ```
//!
//! ## `kafka_producer`
//!
//! To produce events as kafka messages, the a connector needs to be defined from the `kafka_producer` connector type.
//!
//! ### Configuration
//!
//! It supports the following configuration options:
//!
//! | Option            | Description                                                                                                                                                      | Type                           | Required | Default Value                                               |
//! |-------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------|----------|-------------------------------------------------------------|
//! | `brokers`         | URLs to the cluster bootstrap servers to connect to. Corresponds to the librdkafka `bootstrap.servers` setting.                                                  | list of strings                | yes      |                                                             |
//! | `topic`           | The topic to produce events to.                                                                                                                                  | string                         | yes      |                                                             |
//! | `key`             | The message key to add to the produced kafka messages. Can be overwritten by event metadata value `$kafka_producer.key`.                                         | string                         | no       |                                                             |
//! | `rdkafka_options` | librdkafka configuration. For possible options consult the [librdkafka configuration docs](https://github.com/edenhill/librdkafka/blob/v1.8.0/CONFIGURATION.md). | json record with string values | no       | By default only `client.id` and `bootstrap.servers` is set. |
//!
//! Example configuration for `kafka_producer`:
//!
//! ```tremor
//!     define connector producer from kafka_producer
//!     with
//!         # Enables metrics at a 1 second interval
//!         metrics_interval_s = 1,
//!         # event payload is serialized to JSON
//!         codec = "json",
//!
//!         # Kafka specific producer configuration
//!         config = {
//!             # List of broker bootstrap servers
//!             "brokers": [
//!                 "127.0.0.1:9092",
//!             ],
//!             # the topic to send to
//!             "topic": "tremor_test"
//!         }
//!     end;
//! ```
//!
//! ### Event Metadata
//!
//! To control how the `kafka_producer` produces events as kafka messages, the following metadata options are available:
//!
//! ```tremor
//! let $kafka_producer = {
//!     "key": "message_key",   # kafka message key as string or bytes
//!     "headers": {            # message headers  
//!         "my_bytes_header": <<"badger"/binary>>,
//!         "my_string_header": "string"
//!     },
//!     "timestamp": 12345,     # message timestamp
//!     "partition": 3          # numeric partition id to publish message on
//! };
//! ```
//!
//! :::note
//!
//! It is important to provide the metadata options underneath the key `kafka_producer`, otherwise they will be ignored.
//!
//! :::
//!
//! [librdkafka configuration options]: https://github.com/edenhill/librdkafka/blob/v1.8.2/CONFIGURATION.md
//! [librkafka]: https://github.com/edenhill/librdkafka

/// Kafka consumer connector
pub mod consumer;
/// Kafka producer connector
pub mod producer;

use crate::{errors::error_connector_def, prelude::*};
use beef::Cow;
use rdkafka::{error::KafkaError, ClientContext, Statistics};
use rdkafka_sys::RDKafkaErrorCode;
use simd_json::ObjectHasher;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Arc,
    },
    time::Duration,
};
use tokio::sync::broadcast::Sender as BroadcastSender;
use tokio::task::JoinHandle;

const KAFKA_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, thiserror::Error)]
enum Error {
    #[error("Unknown stats client_type \"{0}\"")]
    UnknownStatsClient(String),
    #[error("Provided rdkafka_option that will be overwritten: {0}")]
    OptionOverwritten(String),
    #[error("Cannot enable `retry_failed_events` and `enable.auto.commit` and `enable.auto.offset.store` at the same time.")]
    CommitConfigConflict,
}

/// verify broker host:port pairs in kafka connector configs
fn verify_brokers(
    alias: &alias::Connector,
    brokers: &[String],
) -> Result<(String, Option<u16>), crate::Error> {
    let mut first_broker: Option<(String, Option<u16>)> = None;
    for broker in brokers {
        match broker.split(':').collect::<Vec<_>>().as_slice() {
            [host] => {
                first_broker.get_or_insert_with(|| ((*host).to_string(), None));
            }
            [host, port] => {
                let port: u16 = port.parse().map_err(|_| {
                    error_connector_def(alias, &format!("Invalid broker: {host}:{port}"))
                })?;
                first_broker.get_or_insert_with(|| ((*host).to_string(), Some(port)));
            }
            b => {
                let e = format!("Invalid broker: {}", b.join(":"));
                return Err(error_connector_def(alias, &e));
            }
        }
    }
    first_broker.ok_or_else(|| error_connector_def(alias, "Missing brokers."))
}

/// Returns `true` if the error denotes a failed connect attempt
/// for both consumer and producer
fn is_failed_connect_error(err: &KafkaError) -> bool {
    matches!(
        err,
        KafkaError::ClientConfig(_, _, _, _)
            | KafkaError::ClientCreation(_)
            // TODO: what else?
            | KafkaError::Global(RDKafkaErrorCode::UnknownTopicOrPartition | RDKafkaErrorCode::UnknownTopic | RDKafkaErrorCode::AllBrokersDown)
    )
}

struct TremorRDKafkaContext<Ctx>
where
    Ctx: Context + Send + Sync + 'static,
{
    ctx: Ctx,
    connect_tx: Sender<KafkaError>,
    metrics_tx: BroadcastSender<EventPayload>,
    active: AtomicBool,
    // for synchronizing when the consumer should clear its assignment cache
    last_rebalance_ts: Arc<AtomicU64>,
}

impl<Ctx> TremorRDKafkaContext<Ctx>
where
    Ctx: Context + Send + Sync + 'static,
{
    const PRODUCER: &'static str = "producer";
    const CONSUMER: &'static str = "consumer";
    const TX_MSGS: Cow<'static, str> = Cow::const_str("tx_msgs");
    const TX_MSG_BYTES: Cow<'static, str> = Cow::const_str("tx_msg_bytes");
    const QUEUED_MSGS: Cow<'static, str> = Cow::const_str("queued_msgs");
    const CONNECTOR: Cow<'static, str> = Cow::const_str("connector");
    const KAFKA_PRODUCER_STATS: &'static str = "kafka_producer_stats";

    const RX_MSGS: Cow<'static, str> = Cow::const_str("rx_msgs");
    const RX_MSG_BYTES: Cow<'static, str> = Cow::const_str("rx_msg_bytes");
    const PARTITIONS_ASSIGNED: Cow<'static, str> = Cow::const_str("partitions_assigned");
    const CONSUMER_LAG: Cow<'static, str> = Cow::const_str("consumer_lag");
    const KAFKA_CONSUMER_STATS: &'static str = "kafka_consumer_stats";

    fn consumer(
        ctx: Ctx,
        connect_tx: Sender<KafkaError>,
        metrics_tx: BroadcastSender<EventPayload>,
        last_rebalance_ts: Arc<AtomicU64>,
    ) -> Self {
        Self {
            ctx,
            connect_tx,
            metrics_tx,
            active: AtomicBool::new(true),
            last_rebalance_ts,
        }
    }

    fn producer(
        ctx: Ctx,
        connect_tx: Sender<KafkaError>,
        metrics_tx: BroadcastSender<EventPayload>,
    ) -> Self {
        Self {
            ctx,
            connect_tx,
            metrics_tx,
            active: AtomicBool::new(true),
            last_rebalance_ts: Arc::new(AtomicU64::new(0)), // not used for the producer, just a dummy here
        }
    }

    fn on_connection_lost(&self) -> JoinHandle<()> {
        let ctx = self.ctx.clone();

        // only actually notify the notifier if we didn't do so before
        // otherwise we would flood the connector and reconnect multiple times
        if self.active.swap(false, Ordering::AcqRel) {
            tokio::task::spawn(async move {
                if let Err(e) = ctx.notifier().connection_lost().await {
                    error!("{ctx} Error notifying the connector of a lost connection: {e}");
                }
            })
        } else {
            // do nothing
            tokio::task::spawn(async move {})
        }
    }

    // TODO: add full connector id to tags
    fn handle_stats(&self, stats: Statistics) -> anyhow::Result<()> {
        let metrics_payload = match stats.client_type.as_str() {
            Self::PRODUCER => {
                let timestamp = u64::try_from(stats.time)? * 1_000_000_000;
                let mut fields = Object::with_capacity_and_hasher(3, ObjectHasher::default());
                fields.insert(Self::TX_MSGS, Value::from(stats.txmsgs));
                fields.insert(Self::TX_MSG_BYTES, Value::from(stats.txmsg_bytes));
                fields.insert(Self::QUEUED_MSGS, Value::from(stats.msg_cnt));

                let mut tags = Object::with_capacity_and_hasher(1, ObjectHasher::default());
                tags.insert(Self::CONNECTOR, Value::from(self.ctx.alias().to_string()));

                make_metrics_payload(Self::KAFKA_PRODUCER_STATS, fields, tags, timestamp)
            }
            Self::CONSUMER => {
                let timestamp = u64::try_from(stats.time)? * 1_000_000_000;

                // consumer stats
                let mut fields = Object::with_capacity_and_hasher(4, ObjectHasher::default());
                fields.insert(Self::RX_MSGS, Value::from(stats.rxmsgs));
                fields.insert(Self::RX_MSG_BYTES, Value::from(stats.rxmsg_bytes));
                if let Some(cg) = stats.cgrp {
                    fields.insert(Self::PARTITIONS_ASSIGNED, Value::from(cg.assignment_size));
                }
                let mut consumer_lag = 0_i64;
                for topic in stats.topics.values() {
                    for partition in topic.partitions.values() {
                        if partition.desired && !partition.unknown && partition.consumer_lag >= 0 {
                            consumer_lag += partition.consumer_lag;
                        }
                    }
                }
                fields.insert(Self::CONSUMER_LAG, Value::from(consumer_lag));
                let mut tags = Object::with_capacity_and_hasher(1, ObjectHasher::default());
                tags.insert(Self::CONNECTOR, Value::from(self.ctx.alias().to_string()));
                make_metrics_payload(Self::KAFKA_CONSUMER_STATS, fields, tags, timestamp)
            }
            other => {
                return Err(Error::UnknownStatsClient(other.to_string()).into());
            }
        };
        self.metrics_tx.send(metrics_payload)?;
        Ok(())
    }
}

impl<Ctx> ClientContext for TremorRDKafkaContext<Ctx>
where
    Ctx: Context + Send + Sync + 'static,
{
    /// log messages from librdkafka with connector alias prefixed
    fn log(&self, level: rdkafka::config::RDKafkaLogLevel, fac: &str, log_message: &str) {
        match level {
            rdkafka::config::RDKafkaLogLevel::Emerg
            | rdkafka::config::RDKafkaLogLevel::Alert
            | rdkafka::config::RDKafkaLogLevel::Critical
            | rdkafka::config::RDKafkaLogLevel::Error => {
                error!(target: "librdkafka", "{} librdkafka: {} {}", self.ctx, fac, log_message);
            }
            rdkafka::config::RDKafkaLogLevel::Warning => {
                warn!(target: "librdkafka", "{} librdkafka: {} {}", self.ctx, fac, log_message);
            }
            rdkafka::config::RDKafkaLogLevel::Notice => {
                info!(target: "librdkafka", "{} librdkafka: {} {}", self.ctx, fac, log_message);
            }
            rdkafka::config::RDKafkaLogLevel::Info => {
                info!(target: "librdkafka", "{} librdkafka: {} {}", self.ctx, fac, log_message);
            }
            rdkafka::config::RDKafkaLogLevel::Debug => {
                debug!(target: "librdkafka", "{} librdkafka: {} {}", self.ctx, fac, log_message);
            }
        }
    }

    fn stats(&self, stats: rdkafka::Statistics) {
        if let Err(e) = self.handle_stats(stats) {
            warn!("{} Error handling kafka stats: {}", self.ctx, e);
        }
    }

    fn error(&self, error: KafkaError, reason: &str) {
        error!("{} Kafka Error {}: {}", &self.ctx, &error, reason);
        if !self.connect_tx.is_closed() {
            // still in connect phase
            if is_fatal_error(&error) || is_failed_connect_error(&error) {
                if let Err(e) = self.connect_tx.try_send(error) {
                    // if we error here, the queue is full, so we already notified the connector, ignore
                    warn!("{} Error sending connect message: {e}", self.ctx);
                }
            }
        } else if is_fatal_error(&error) {
            // we are out of connect phase
            // issue a reconnect upon fatal errors
            self.on_connection_lost();
        }
    }
}

/// Kafka error indicating success
pub(crate) const NO_ERROR: KafkaError = KafkaError::Global(RDKafkaErrorCode::NoError);

/// check if a kafka error is fatal for producer and consumer
fn is_fatal_error(e: &KafkaError) -> bool {
    match e {
        // Generic fatal errors
        KafkaError::AdminOp(code)
        | KafkaError::ConsumerCommit(code)
        | KafkaError::Global(code)
        | KafkaError::GroupListFetch(code)
        | KafkaError::MessageConsumption(code)
        | KafkaError::MessageProduction(code)
        | KafkaError::MetadataFetch(code)
        | KafkaError::OffsetFetch(code)
        | KafkaError::SetPartitionOffset(code)
        | KafkaError::StoreOffset(code) => matches!(
            code,
            RDKafkaErrorCode::Fatal
                | RDKafkaErrorCode::AllBrokersDown
                | RDKafkaErrorCode::InvalidProducerEpoch
                | RDKafkaErrorCode::UnknownMemberId
        ),
        // Check if it is a fatal transaction error
        KafkaError::Transaction(e) => e.is_fatal(),
        // subscription failed for the consumer
        KafkaError::Subscription(_) => true,

        // This is required due to `KafkaError` being mared as `#[non_exhaustive]`
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{ClientContext, TremorRDKafkaContext};
    use crate::{channel::bounded, unit_tests::FakeContext};
    use rdkafka::Statistics;
    use std::{
        collections::HashMap,
        sync::{atomic::AtomicU64, Arc},
        time::Duration,
    };
    use tokio::{sync::broadcast::channel as broadcast, time::timeout};
    use tremor_system::connector::Msg;
    use tremor_value::literal;

    #[tokio::test(flavor = "multi_thread")]
    async fn context_on_connection_loss() -> anyhow::Result<()> {
        let (ctx_tx, mut ctx_rx) = bounded(1);
        let (connect_tx, _connect_rx) = bounded(1);
        let (metrics_tx, _metrics_rx) = broadcast(1);
        let fake_ctx = FakeContext::new(ctx_tx);
        let ctx = TremorRDKafkaContext::consumer(
            fake_ctx,
            connect_tx,
            metrics_tx,
            Arc::new(AtomicU64::new(0)),
        );
        ctx.on_connection_lost().await?;
        let msg = timeout(Duration::from_secs(1), ctx_rx.recv())
            .await?
            .expect("no message");
        assert!(matches!(msg, Msg::ConnectionLost));

        // no second time
        ctx.on_connection_lost().await?;

        Ok(())
    }

    #[test]
    fn metrics_tx() -> anyhow::Result<()> {
        let (ctx_tx, _ctx_rx) = bounded(1);
        let (connect_tx, _connect_rx) = bounded(1);
        let (metrics_tx, mut metrics_rx) = broadcast(1);
        let fake_ctx = FakeContext::new(ctx_tx);
        let ctx = TremorRDKafkaContext::consumer(
            fake_ctx,
            connect_tx,
            metrics_tx,
            Arc::new(AtomicU64::new(0)),
        );

        let s = Statistics {
            name: "snot".to_string(),
            client_id: "snot".to_string(),
            client_type: "consumer".to_string(),
            ts: 100,
            time: 100,
            age: 0,
            replyq: 2,
            msg_cnt: 4,
            msg_size: 5,
            msg_max: 1000,
            msg_size_max: 42,
            tx: 2,
            tx_bytes: 0,
            rx: 1,
            rx_bytes: 0,
            txmsgs: 12,
            txmsg_bytes: 42,
            rxmsgs: 42,
            rxmsg_bytes: 42,
            simple_cnt: 42,
            metadata_cache_cnt: 42,
            brokers: HashMap::new(),
            topics: HashMap::new(),
            cgrp: None,
            eos: None,
        };
        ctx.stats(s);
        let metrics_msg = metrics_rx.try_recv()?;
        assert_eq!(
            &literal!({
                "measurement": "kafka_consumer_stats",
                "tags": {
                    "connector": "fake::fake"
                },
                "fields": {
                    "rx_msgs": 42,
                    "rx_msg_bytes": 42,
                    "consumer_lag": 0,
                },
                "timestamp": 100_000_000_000_u64
            }),
            metrics_msg.suffix().value()
        );
        Ok(())
    }

    #[test]
    fn unknown_statistics_client_type() {
        let (ctx_tx, _ctx_rx) = bounded(1);
        let (connect_tx, _connect_rx) = bounded(1);
        let (metrics_tx, metrics_rx) = broadcast(1);
        let fake_ctx = FakeContext::new(ctx_tx);
        let ctx = TremorRDKafkaContext::consumer(
            fake_ctx,
            connect_tx,
            metrics_tx,
            Arc::new(AtomicU64::new(0)),
        );
        let s = Statistics::default();
        ctx.stats(s);
        assert!(metrics_rx.is_empty());
    }

    #[test]
    fn log_coverage() {
        let (ctx_tx, _ctx_rx) = bounded(1);
        let (connect_tx, _connect_rx) = bounded(1);
        let (metrics_tx, _metrics_rx) = broadcast(1);
        let fake_ctx = FakeContext::new(ctx_tx);
        let ctx = TremorRDKafkaContext::consumer(
            fake_ctx,
            connect_tx,
            metrics_tx,
            Arc::new(AtomicU64::new(0)),
        );

        ctx.log(rdkafka::config::RDKafkaLogLevel::Emerg, "consumer", "snot");
        ctx.log(rdkafka::config::RDKafkaLogLevel::Alert, "consumer", "snot");
        ctx.log(
            rdkafka::config::RDKafkaLogLevel::Critical,
            "consumer",
            "snot",
        );
        ctx.log(rdkafka::config::RDKafkaLogLevel::Error, "consumer", "snot");
        ctx.log(
            rdkafka::config::RDKafkaLogLevel::Warning,
            "consumer",
            "snot",
        );
        ctx.log(rdkafka::config::RDKafkaLogLevel::Notice, "consumer", "snot");
        ctx.log(rdkafka::config::RDKafkaLogLevel::Info, "consumer", "snot");
        ctx.log(rdkafka::config::RDKafkaLogLevel::Debug, "consumer", "snot");
    }
}
