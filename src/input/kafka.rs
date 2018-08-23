use futures::prelude::*;
use input::{Input as InputT, INPUT_ERR, INPUT_OK};
use pipeline::Msg;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext};
use rdkafka::error::KafkaResult;
use rdkafka::Message;
use rdkafka_sys;
use serde_json;
use std::collections::HashMap;
use std::thread;

use std::sync::mpsc;

pub struct Input {
    config: Config,
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

fn dflt_threads() -> usize {
    1
}
#[derive(Deserialize, Debug, Clone)]
struct Config {
    group_id: String,
    topics: Vec<String>,
    brokers: Vec<String>,
    #[serde(default = "dflt_options")]
    rdkafka_options: HashMap<String, String>,
    #[serde(default = "dflt_threads")]
    threads: usize,
}
// Define a new type for convenience
pub type LoggingConsumer = StreamConsumer<LoggingConsumerContext>;
impl Input {
    pub fn new(opts: &str) -> Self {
        match serde_json::from_str(opts) {
            Ok(config @ Config { .. }) => {
                Input { config }
            }
            e => {
                panic!("Invalid options for Kafka input, use `{{\"group_id\": \"<group.id>\", \"topics\": [\"<topic>\"], \"brokers\": [\"<broker>\", \"<broker>\", ...]}}`\n{:?} ({})", e, opts)
            }
        }
    }
}

impl InputT for Input {
    fn enter_loop(&self, pipelines: Vec<mpsc::SyncSender<Msg>>) {
        let mut idx = 0;
        let mut t: Option<thread::JoinHandle<()>> = None;
        for _tid in 0..self.config.threads {
            let config = self.config.clone();
            let pipelines = pipelines.clone();
            let h = thread::spawn(move || {
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
                        .set("enable.auto.offset.store", "true")
                        .set_log_level(RDKafkaLogLevel::Debug)
                        .create_with_context(context)
                        .expect("Consumer creation failed");
                let topics: Vec<&str> = config.topics.iter().map(|topic| topic.as_str()).collect();
                consumer
                    .subscribe(&topics)
                    .expect("Can't subscribe to specified topic");
                let len = pipelines.len();
                for message in consumer.start().wait() {
                    idx = (idx + 1) % len;
                    match message {
                        Err(_e) => {
                            INPUT_ERR.inc();
                            warn!("Input error");
                        }
                        Ok(Err(_m)) => {
                            INPUT_ERR.inc();
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
                                let payload = String::from(p);
                                let mut sent = false;
                                if len > 0 {
                                    for i in 0..len {
                                        idx = (idx + i) % len;
                                        if pipelines[idx]
                                            .try_send(Msg::new(key.clone(), payload.clone()))
                                            .is_ok()
                                        {
                                            sent = true;
                                            break;
                                        }
                                    }
                                }
                                if !sent {
                                    if let Err(e) = pipelines[idx].send(Msg::new(key, payload)) {
                                        error!("Error during handling message: {:?}", e)
                                    };
                                }
                            }
                            INPUT_OK.inc();
                        }
                    }
                }
            });
            t = Some(h);
        }
        if let Some(h) = t {
            let _ = h.join();
        }
    }
}
