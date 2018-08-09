use futures::prelude::*;
use input::{Input as InputT, INPUT_ERR, INPUT_OK};
use pipeline::{Msg, Pipeline};
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext};
use rdkafka::error::KafkaResult;
use rdkafka::Message;
use rdkafka_sys;
use serde_json;
use std::collections::HashMap;

pub struct Input {
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

fn dflt_options() -> HashMap<String, String> {
    HashMap::new()
}

#[derive(Deserialize, Debug)]
struct Config {
    group_id: String,
    topics: Vec<String>,
    brokers: Vec<String>,
    #[serde(default = "dflt_options")]
    rdkafka_options: HashMap<String, String>,
}
// Define a new type for convenience
pub type LoggingConsumer = StreamConsumer<LoggingConsumerContext>;
impl Input {
    pub fn new(opts: &str) -> Self {
        match serde_json::from_str(opts) {
            Ok(config @ Config { .. }) => {
                info!(
                    "Starting Kafka input: GroupID: {}, Topic: {:?}, Brokers: {:?}",
                    config.group_id, config.topics, config.brokers
                );
                let context = LoggingConsumerContext;
                let consumer: LoggingConsumer = config.rdkafka_options.iter().fold(&mut ClientConfig::new(), |c: &mut ClientConfig, (k, v)| c.set(k, v))
                    .set("group.id", &config.group_id)
                    .set("bootstrap.servers", &config.brokers.join(","))
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
                let topics: Vec<&str> = config.topics.iter().map(|topic| topic.as_str()).collect();
                consumer
                    .subscribe(&topics)
                    .expect("Can't subscribe to specified topic");
                Input { consumer }
            }
            e => {
                panic!("Invalid options for Kafka input, use `{{\"group_id\": \"<group.id>\", \"topics\": [\"<topic>\"], \"brokers\": [\"<broker>\", \"<broker>\", ...]}}`\n{:?} ({})", e, opts)
            }
        }
    }
}

impl InputT for Input {
    fn enter_loop(&self, pipeline: &mut Pipeline) {
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
                    if let Some(Ok(p)) = m.payload_view::<str>() {
                        let key = match m.key_view::<str>() {
                            Some(key) => Some(key.unwrap()),
                            None => None,
                        };
                        if let Err(e) = pipeline.run(&Msg::new(key, p)) {
                            error!("Error during handling message: {:?}", e)
                        };
                    }
                    // Now that the message is completely processed, add it's position to the offset
                    // store. The actual offset will be committed every 5 seconds.
                    if let Err(e) = self.consumer.store_offset(&m) {
                        INPUT_ERR.inc();
                        warn!("Error while storing offset: {}", e);
                    } else {
                        INPUT_OK.inc();
                    }
                }
            }
        }
    }
}
