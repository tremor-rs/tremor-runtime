//! This module defines the interface for input connectors.
//! Input connectors are used to get data into the system
//! to then be processed.

use futures::prelude::*;
use pipeline::{Msg, Pipeline};
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext};
use rdkafka::error::KafkaResult;
use rdkafka::Message;
use rdkafka_sys;
use std::io::{self, BufRead, BufReader};

pub trait Input {
    fn enter_loop(&self, pipeline: Pipeline);
}

pub fn new(name: &str, opts: &str) -> Inputs {
    match name {
        "kafka" => Inputs::Kafka(KafkaInput::new(opts)),
        "stdin" => Inputs::Stdin(StdinInput::new(opts)),
        _ => panic!("Unknown classifier: {}", name),
    }
}

pub enum Inputs {
    Kafka(KafkaInput),
    Stdin(StdinInput),
}

impl Input for Inputs {
    fn enter_loop(&self, pipeline: Pipeline) {
        match self {
            Inputs::Kafka(i) => i.enter_loop(pipeline),
            Inputs::Stdin(i) => i.enter_loop(pipeline),
        }
    }
}

pub struct StdinInput {}

impl StdinInput {
    fn new(_opts: &str) -> Self {
        Self {}
    }
}
impl Input for StdinInput {
    fn enter_loop(&self, pipeline: Pipeline) {
        let stdin = io::stdin();
        let stdin = BufReader::new(stdin);
        for line in stdin.lines() {
            debug!("Line: {:?}", line);
            match line {
                Ok(line) => {
                    let msg = Msg::new(None, line);
                    let _ = pipeline.run(msg);
                }
                Err(_) => (),
            }
        }
    }
}

pub struct KafkaInput {
    pub consumer: LoggingConsumer,
}
// A simple context to customize the consumer behavior and print a log line every time
// offsets are committed
pub struct LoggingConsumerContext;

impl ClientContext for LoggingConsumerContext {}

impl ConsumerContext for LoggingConsumerContext {
    fn commit_callback(
        &self,
        result: KafkaResult<()>,
        _offsets: *mut rdkafka_sys::RDKafkaTopicPartitionList,
    ) {
        match result {
            Ok(_) => info!("Offsets committed successfully"),
            Err(e) => warn!("Error while committing offsets: {}", e),
        };
    }
}

// Define a new type for convenience
pub type LoggingConsumer = StreamConsumer<LoggingConsumerContext>;
impl KafkaInput {
    fn new(opts: &str) -> Self {
        let opts: Vec<&str> = opts.split('|').collect();
        match opts.as_slice() {
            [group_id, topic, brokers] => {
                debug!(
                    "Starting Kafka input: GroupID: {}, Topic: {}, Brokers: {}",
                    group_id, topic, brokers
                );
                let context = LoggingConsumerContext;
                let consumer: LoggingConsumer = ClientConfig::new()
                    .set("group.id", group_id)
                    .set("bootstrap.servers", brokers)
                    .set("enable.partition.eof", "false")
                    .set("session.timeout.ms", "6000")
                // Commit automatically every 5 seconds.
                    .set("enable.auto.commit", "true")
                    .set("auto.commit.interval.ms", "5000")
                // but only commit the offsets explicitly stored via `consumer.store_offset`.
                    .set("enable.auto.offset.store", "false")
                    .set_log_level(RDKafkaLogLevel::Debug)
                    .create_with_context(context)
                    .expect("Consumer creation failed");
                consumer
                    .subscribe(&[topic])
                    .expect("Can't subscribe to specified topic");
                KafkaInput { consumer: consumer }
            }
            _ => panic!("Invalid options for Kafka input, use <groupid>|<topioc>|<producers>"),
        }
    }
}

impl Input for KafkaInput {
    fn enter_loop(&self, pipeline: Pipeline) {
        for message in self.consumer.start().wait() {
            match message {
                Err(_e) => {
                    warn!("Input error");
                }
                Ok(Err(_m)) => {
                    warn!("Input error");
                }
                Ok(Ok(m)) => {
                    // Send a copy to the message to every output topic in parallel, and wait for the
                    // delivery report to be received.
                    match m.payload_view::<str>() {
                        Some(Ok(p)) => {
                            let key = match m.key_view::<str>() {
                                Some(key) => Some(String::from(key.unwrap())),
                                None => None,
                            };
                            pipeline
                                .run(Msg::new(key, String::from(p)))
                                .expect("Error during handling message")
                        }
                        _ => (),
                    };
                    // Now that the message is completely processed, add it's position to the offset
                    // store. The actual offset will be committed every 5 seconds.
                    if let Err(e) = self.consumer.store_offset(&m) {
                        warn!("Error while storing offset: {}", e);
                    }
                }
            }
        }
    }
}
