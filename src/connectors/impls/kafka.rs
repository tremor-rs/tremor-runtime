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
pub(crate) mod consumer;
pub(crate) mod producer;

use crate::connectors::prelude::*;
use async_broadcast::Sender as BroadcastSender;
use async_std::{channel::Sender, task::JoinHandle};
use beef::Cow;
use core::future::Future;
use futures::future;
use halfbrown::HashMap;
use rdkafka::{error::KafkaError, util::AsyncRuntime, ClientContext, Statistics};
use rdkafka_sys::RDKafkaErrorCode;
use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};
use tremor_script::EventPayload;
use tremor_value::Value;

const KAFKA_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

pub(crate) struct SmolRuntime;

impl AsyncRuntime for SmolRuntime {
    type Delay = future::Map<smol::Timer, fn(Instant)>;

    fn spawn<T>(task: T)
    where
        T: Future<Output = ()> + Send + 'static,
    {
        // This needs to be smol::spawn we can't use async_std::task::spawn
        smol::spawn(task).detach();
    }

    fn delay_for(duration: Duration) -> Self::Delay {
        // This needs to be smol::Timer we can't use async_io::Timer
        futures::FutureExt::map(smol::Timer::after(duration), |_| ())
    }
}

/// verify broker host:port pairs in kafka connector configs
fn verify_brokers(alias: &Alias, brokers: &[String]) -> Result<(String, Option<u16>)> {
    let mut first_broker: Option<(String, Option<u16>)> = None;
    for broker in brokers {
        match broker.split(':').collect::<Vec<_>>().as_slice() {
            [host] => {
                first_broker.get_or_insert_with(|| ((*host).to_string(), None));
            }
            [host, port] => {
                let port: u16 = port.parse().map_err(|_| {
                    err_connector_def(alias, &format!("Invalid broker: {host}:{port}"))
                })?;
                first_broker.get_or_insert_with(|| ((*host).to_string(), Some(port)));
            }
            b => {
                let e = format!("Invalid broker: {}", b.join(":"));
                return Err(err_connector_def(alias, &e));
            }
        }
    }
    first_broker.ok_or_else(|| err_connector_def(alias, "Missing brokers."))
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

    fn new(
        ctx: Ctx,
        connect_tx: Sender<KafkaError>,
        metrics_tx: BroadcastSender<EventPayload>,
    ) -> Self {
        Self {
            ctx,
            connect_tx,
            metrics_tx,
            active: AtomicBool::new(true),
        }
    }

    fn on_connection_lost(&self) -> JoinHandle<()> {
        let ctx = self.ctx.clone();

        // only actually notify the notifier if we didn't do so before
        // otherwise we would flood the connector and reconnect multiple times
        if self.active.swap(false, Ordering::AcqRel) {
            async_std::task::spawn(async move {
                if let Err(e) = ctx.notifier().connection_lost().await {
                    error!("{ctx} Error notifying the connector of a lost connection: {e}");
                }
            })
        } else {
            // do nothing
            async_std::task::spawn(async move {})
        }
    }

    // TODO: add full connector id to tags
    fn handle_stats(&self, stats: Statistics) -> Result<()> {
        let metrics_payload = match stats.client_type.as_str() {
            Self::PRODUCER => {
                let timestamp = u64::try_from(stats.time)? * 1_000_000_000;
                let mut fields = HashMap::with_capacity(3);
                fields.insert(Self::TX_MSGS, Value::from(stats.txmsgs));
                fields.insert(Self::TX_MSG_BYTES, Value::from(stats.txmsg_bytes));
                fields.insert(Self::QUEUED_MSGS, Value::from(stats.msg_cnt));

                let mut tags = HashMap::with_capacity(1);
                tags.insert(Self::CONNECTOR, Value::from(self.ctx.alias().to_string()));

                make_metrics_payload(Self::KAFKA_PRODUCER_STATS, fields, tags, timestamp)
            }
            Self::CONSUMER => {
                let timestamp = u64::try_from(stats.time)? * 1_000_000_000;

                // consumer stats
                let mut fields = HashMap::with_capacity(4);
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
                let mut tags = HashMap::with_capacity(1);
                tags.insert(Self::CONNECTOR, Value::from(self.ctx.alias().to_string()));
                make_metrics_payload(Self::KAFKA_CONSUMER_STATS, fields, tags, timestamp)
            }
            other => {
                return Err(format!("Unknown stats client_type \"{other}\"").into());
            }
        };
        self.metrics_tx.try_broadcast(metrics_payload)?;
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
                self.connect_tx.close();
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
    use std::collections::HashMap;
    use std::time::Duration;

    use super::{ClientContext, TremorRDKafkaContext};
    use crate::connectors::unit_tests::FakeContext;
    use crate::connectors::Msg;
    use crate::errors::Result;
    use async_broadcast::broadcast;
    use async_std::channel::bounded;
    use async_std::prelude::FutureExt;
    use rdkafka::Statistics;
    use tremor_value::literal;

    #[async_std::test]
    async fn context_on_connection_loss() -> Result<()> {
        let (ctx_tx, ctx_rx) = bounded(1);
        let (connect_tx, _connect_rx) = bounded(1);
        let (metrics_tx, _metrics_rx) = broadcast(1);
        let fake_ctx = FakeContext::new(ctx_tx);
        let ctx = TremorRDKafkaContext::new(fake_ctx, connect_tx, metrics_tx);
        ctx.on_connection_lost().await;
        let msg = ctx_rx.recv().timeout(Duration::from_secs(1)).await??;
        assert!(matches!(msg, Msg::ConnectionLost));

        // no second time
        ctx.on_connection_lost().await;
        assert!(ctx_rx.is_empty());

        Ok(())
    }

    #[test]
    fn metrics_tx() -> Result<()> {
        let (ctx_tx, _ctx_rx) = bounded(1);
        let (connect_tx, _connect_rx) = bounded(1);
        let (metrics_tx, mut metrics_rx) = broadcast(1);
        let fake_ctx = FakeContext::new(ctx_tx);
        let ctx = TremorRDKafkaContext::new(fake_ctx, connect_tx, metrics_tx);

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
                "timestamp": 100000000000_u64
            }),
            metrics_msg.suffix().value()
        );
        Ok(())
    }

    #[test]
    fn closed_metrics_rx() -> Result<()> {
        let (ctx_tx, _ctx_rx) = bounded(1);
        let (connect_tx, _connect_rx) = bounded(1);
        let (metrics_tx, metrics_rx) = broadcast(1);
        metrics_rx.close();
        let fake_ctx = FakeContext::new(ctx_tx);
        let ctx = TremorRDKafkaContext::new(fake_ctx, connect_tx, metrics_tx);

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
        Ok(())
    }

    #[test]
    fn unknown_statistics_client_type() -> Result<()> {
        let (ctx_tx, _ctx_rx) = bounded(1);
        let (connect_tx, _connect_rx) = bounded(1);
        let (metrics_tx, metrics_rx) = broadcast(1);
        let fake_ctx = FakeContext::new(ctx_tx);
        let ctx = TremorRDKafkaContext::new(fake_ctx, connect_tx, metrics_tx);
        let s = Statistics::default();
        ctx.stats(s);
        assert!(metrics_rx.is_empty());

        Ok(())
    }

    #[test]
    fn log_coverage() -> Result<()> {
        let (ctx_tx, _ctx_rx) = bounded(1);
        let (connect_tx, _connect_rx) = bounded(1);
        let (metrics_tx, _metrics_rx) = broadcast(1);
        let fake_ctx = FakeContext::new(ctx_tx);
        let ctx = TremorRDKafkaContext::new(fake_ctx, connect_tx, metrics_tx);

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

        Ok(())
    }
}
