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

use crate::{
    connectors::{metrics::make_metrics_payload, Context},
    errors::{err_conector_def, Result},
};
use async_broadcast::Sender as BroadcastSender;
use async_std::channel::Sender;
use beef::Cow;
use core::future::Future;
use futures::future;
use halfbrown::HashMap;
use rdkafka::{error::KafkaError, util::AsyncRuntime, ClientContext};
use rdkafka_sys::RDKafkaErrorCode;
use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};
use tremor_script::EventPayload;
use tremor_value::Value;

const KAFKA_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

pub struct SmolRuntime;

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
fn verify_brokers(id: &str, brokers: &[String]) -> Result<(String, Option<u16>)> {
    let mut first_broker: Option<(String, Option<u16>)> = None;
    for broker in brokers {
        match broker.split(':').collect::<Vec<_>>().as_slice() {
            [host] => {
                first_broker.get_or_insert_with(|| ((*host).to_string(), None));
            }
            [host, port] => {
                let port: u16 = port
                    .parse()
                    .map_err(|_| err_conector_def(id, &format!("Invalid broker: {host}:{port}")))?;
                first_broker.get_or_insert_with(|| ((*host).to_string(), Some(port)));
            }
            b => {
                let e = format!("Invalid broker: {}", b.join(":"));
                return Err(err_conector_def(id, &e));
            }
        }
    }
    first_broker.ok_or_else(|| err_conector_def(id, "Missing brokers."))
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

    fn on_connection_lost(&self) {
        let ctx = self.ctx.clone();

        // only actually notify the notifier if we didn't do so before
        // otherwise we would flood the connector and reconnect multiple times
        if self.active.fetch_and(false, Ordering::AcqRel) {
            async_std::task::spawn(async move {
                if let Err(e) = ctx.notifier().connection_lost().await {
                    error!("{ctx} Error notifying the connector of a lost connection: {e}");
                }
            });
        }
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

    // TODO: add full connector id to tags
    #[allow(clippy::cast_sign_loss)]
    fn stats(&self, stats: rdkafka::Statistics) {
        let metrics_payload = if stats.client_type.eq(Self::PRODUCER) {
            let timestamp = stats.time as u64 * 1_000_000_000;
            let mut fields = HashMap::with_capacity(3);
            fields.insert(Cow::const_str("tx_msgs"), Value::from(stats.txmsgs));
            fields.insert(
                Cow::const_str("tx_msg_bytes"),
                Value::from(stats.txmsg_bytes),
            );
            fields.insert(Cow::const_str("queued_msgs"), Value::from(stats.msg_cnt));

            let mut tags = HashMap::with_capacity(1);
            tags.insert(
                Cow::const_str("connector"),
                Value::from(self.ctx.alias().to_string()),
            );

            make_metrics_payload("kafka_producer_stats", fields, tags, timestamp)
        } else if stats.client_type.eq(Self::CONSUMER) {
            let timestamp = stats.time as u64 * 1_000_000_000;

            // consumer stats
            let mut fields = HashMap::with_capacity(4);
            fields.insert(Cow::const_str("rx_msgs"), Value::from(stats.rxmsgs));
            fields.insert(
                Cow::const_str("rx_msg_bytes"),
                Value::from(stats.rxmsg_bytes),
            );
            if let Some(cg) = stats.cgrp {
                fields.insert(
                    Cow::const_str("partitions_assigned"),
                    Value::from(cg.assignment_size),
                );
            }
            let mut consumer_lag = 0_i64;
            for topic in stats.topics.values() {
                for partition in topic.partitions.values() {
                    if partition.desired && !partition.unknown && partition.consumer_lag >= 0 {
                        consumer_lag += partition.consumer_lag;
                    }
                }
            }
            fields.insert(Cow::const_str("consumer_lag"), Value::from(consumer_lag));
            let mut tags = HashMap::with_capacity(1);
            tags.insert(
                Cow::const_str("connector"),
                Value::from(self.ctx.alias().to_string()),
            );
            make_metrics_payload("kafka_consumer_stats", fields, tags, timestamp)
        } else {
            warn!(
                "{} Unknown stats client_type: \"{}\"",
                &self.ctx, stats.client_type
            );
            return;
        };

        if let Err(e) = self.metrics_tx.try_broadcast(metrics_payload) {
            warn!("{} Error sending kafka stats: {e}", self.ctx);
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
