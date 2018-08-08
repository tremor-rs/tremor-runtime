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
use std::sync::mpsc::{channel, Receiver};
use std::thread;

pub struct Input {
    rx: Receiver<(Option<String>, String)>,
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

fn dflt_threads() -> u32 {
    1
}

#[derive(Deserialize, Debug, Clone)]
struct Config {
    group_id: String,
    topics: Vec<String>,
    brokers: Vec<String>,
    #[serde(default = "dflt_threads")]
    threads: u32,
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
                let (tx, rx) = channel();
                for _ in 0..config.threads {
                    let tx = tx.clone();
                    let config = config.clone();
                    thread::spawn(move || {
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
                        for message in consumer.start().wait() {
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
                                            Some(key) => Some(String::from(key.unwrap())),
                                            None => None,
                                        };
                                        let _ = tx.send((key, String::from(p)));
                                    }
                                    // Now that the message is completely processed, add it's position to the offset
                                    // store. The actual offset will be committed every 5 seconds.
                                    if let Err(e) = consumer.store_offset(&m) {
                                        INPUT_ERR.inc();
                                        warn!("Error while storing offset: {}", e);
                                    } else {
                                        INPUT_OK.inc();
                                    }
                                }
                            }
                        }

                    });
                }
                Input { rx }
            }
            e => {
                panic!("Invalid options for Kafka input, use `{{\"group_id\": \"<group.id>\", \"topics\": [\"<topic>\"], \"brokers\": [\"<broker>\", \"<broker>\", ...]}}`\n{:?} ({})", e, opts)
            }
        }
    }
}

impl InputT for Input {
    fn enter_loop(&self, pipeline: &mut Pipeline) {
        for (key, payload) in self.rx.iter() {
            if let Err(e) = match key {
                None => pipeline.run(&Msg::new(None, payload.as_str())),
                Some(s) => pipeline.run(&Msg::new(Some(s.clone().as_str()), payload.as_str())),
            } {
                error!("Error during handling message: {:?}", e)
            };
        }
    }
}
