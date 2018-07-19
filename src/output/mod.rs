//! This module handles outputs

use error::TSError;
use futures::Future;
use grouping::MaybeMessage;
use prometheus::Counter;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::collections::HashMap;
use std::time::{Duration, Instant};

lazy_static! {
    /*
     * Number of errors read received from the input
     */
    static ref OPTOUT_DROPED: Counter =
        register_counter!(opts!("ts_output_droped", "Messages dropped.")).unwrap();
    /*
     * Number of successes read received from the input
     */
    static ref OUTPUT_DELIVERED: Counter =
        register_counter!(opts!("ts_output_delivered", "Messages delivered.")).unwrap();
}

// Constructor function that given the name of the output will return the correct
// output connector.
pub fn new(name: &str, opts: &str) -> Outputs {
    match name {
        "kafka" => Outputs::Kafka(KafkaOutput::new(opts)),
        "stdout" => Outputs::Stdout(StdoutOutput::new(opts)),
        "debug" => Outputs::Debug(DebugOutput::new(opts)),

        _ => panic!("Unknown output: {} use kafka, stdout or debug", name),
    }
}

/// Enum of all output connectors we have implemented.
/// New connectors need to be added here.
pub enum Outputs {
    Kafka(KafkaOutput),
    Stdout(StdoutOutput),
    Debug(DebugOutput),
}

/// Implements the Output trait for the enum.
/// this needs to ba adopted for each implementation.
impl Output for Outputs {
    fn send<'a>(&mut self, msg: MaybeMessage<'a>) -> Result<(), TSError> {
        match self {
            Outputs::Kafka(o) => o.send(msg),
            Outputs::Stdout(o) => o.send(msg),
            Outputs::Debug(o) => o.send(msg),
        }
    }
}

/// The output trait, it defines the required functions for any output.
pub trait Output {
    /// Sends a message, blocks until sending is done and returns an empty Ok() or
    /// an Error.
    fn send<'a>(&mut self, msg: MaybeMessage<'a>) -> Result<(), TSError>;
}

/// The output trait, it defines the required functions for any output.
pub trait OutputImpl {
    fn new(opts: &str) -> Self;
}

/// An output that write to stdout
pub struct StdoutOutput {
    pfx: String,
}

impl OutputImpl for StdoutOutput {
    fn new(opts: &str) -> Self {
        StdoutOutput {
            pfx: String::from(opts),
        }
    }
}
impl Output for StdoutOutput {
    fn send<'a>(&mut self, msg: MaybeMessage<'a>) -> Result<(), TSError> {
        if !msg.drop {
            match msg.key {
                None => println!("{}{}", self.pfx, msg.msg.raw),
                Some(key) => println!("{}{} {}", self.pfx, key, msg.msg.raw),
            }
        }

        Ok(())
    }
}

/// Kafka output connector
pub struct KafkaOutput {
    producer: FutureProducer,
    topic: String,
}

impl OutputImpl for KafkaOutput {
    /// Creates a new output connector, `brokers` is a coma seperated list of
    /// brokers to connect to. `topic` is the topic to send to.

    fn new(opts: &str) -> Self {
        let opts: Vec<&str> = opts.split('|').collect();
        match opts.as_slice() {
            [topic, brokers] => {
                let producer = ClientConfig::new()
                    .set("bootstrap.servers", brokers)
                    .set("queue.buffering.max.ms", "0")  // Do not buffer
                    .create()
                    .expect("Producer creation failed");
                KafkaOutput {
                    producer: producer,
                    topic: String::from(*topic),
                }
            }
            _ => panic!("Invalid options for Kafka output, use <topioc>|<producers>"),
        }
    }
}
impl Output for KafkaOutput {
    fn send<'a>(&mut self, msg: MaybeMessage<'a>) -> Result<(), TSError> {
        if !msg.drop {
            OUTPUT_DELIVERED.inc();
            let mut record = FutureRecord::to(self.topic.as_str());
            record = record.payload(msg.msg.raw);
            let r = if let Some(k) = msg.key {
                record = record.key(k);
                self.producer.send(record, 1000).wait()
            } else {
                self.producer.send(record, 1000).wait()
            };
            if r.is_ok() {
                Ok(())
            } else {
                Err(TSError::new("Send failed"))
            }
        } else {
            OPTOUT_DROPED.inc();
            Ok(())
        }
    }
}

struct DebugBucket {
    pass: u64,
    drop: u64,
}
pub struct DebugOutput {
    last: Instant,
    update_time: Duration,
    buckets: HashMap<String, DebugBucket>,
    drop: u64,
    pass: u64,
}

impl DebugOutput {
    pub fn new(_opts: &str) -> Self {
        DebugOutput {
            last: Instant::now(),
            update_time: Duration::from_secs(1),
            buckets: HashMap::new(),
            pass: 0,
            drop: 0,
        }
    }
}
impl Output for DebugOutput {
    fn send<'m>(&mut self, msg: MaybeMessage<'m>) -> Result<(), TSError> {
        if self.last.elapsed() > self.update_time {
            self.last = Instant::now();
            println!("");
            println!(
                "|{:20}| {:7}| {:7}| {:7}|",
                "classification", "total", "pass", "drop"
            );
            println!(
                "|{:20}| {:7}| {:7}| {:7}|",
                "TOTAL",
                self.pass + self.drop,
                self.pass,
                self.drop
            );
            self.pass = 0;
            self.drop = 0;
            for (class, data) in self.buckets.iter() {
                println!(
                    "|{:20}| {:7}| {:7}| {:7}|",
                    class,
                    data.pass + data.drop,
                    data.pass,
                    data.drop
                );
            }
            println!("");
            self.buckets.clear();
        }
        let entry = self.buckets
            .entry(String::from(msg.classification))
            .or_insert(DebugBucket { pass: 0, drop: 0 });
        if msg.drop {
            entry.drop += 1;
            self.drop += 1;
        } else {
            entry.pass += 1;
            self.pass += 1;
        };
        Ok(())
    }
}
