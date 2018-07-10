//! This module handles outputs

use error::TSError;
use futures::Future;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

// Constructor function that given the name of the output will return the correct
// output connector.
pub fn new(name: &str, opts: &str) -> Outputs {
    match name {
        "kafka" => Outputs::Kafka(KafkaOutput::new(opts)),
        "stdout" => Outputs::Stdout(StdoutOutput::new(opts)),

        _ => panic!("Unknown output: {}", name),
    }
}

/// Enum of all output connectors we have implemented.
/// New connectors need to be added here.
pub enum Outputs {
    Kafka(KafkaOutput),
    Stdout(StdoutOutput),
}

/// Implements the Output trait for the enum.
/// this needs to ba adopted for each implementation.
impl Output for Outputs {
    fn send(self: &Self, key: Option<&str>, payload: &str) -> Result<(), TSError> {
        match self {
            Outputs::Kafka(o) => o.send(key, payload),
            Outputs::Stdout(o) => o.send(key, payload),
        }
    }
}

/// The output trait, it defines the required functions for any output.
pub trait Output {
    /// Sends a message, blocks until sending is done and returns an empty Ok() or
    /// an Error.
    fn send(self: &Self, key: Option<&str>, payload: &str) -> Result<(), TSError>;
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
    fn send(&self, key: Option<&str>, payload: &str) -> Result<(), TSError> {
        match key {
            None => println!("{}{}", self.pfx, payload),
            Some(key) => println!("{}{} {}", self.pfx, key, payload),
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
    fn send(self: &Self, key: Option<&str>, payload: &str) -> Result<(), TSError> {
        let mut record = FutureRecord::to(self.topic.as_str());
        record = record.payload(payload);
        let r = if let Some(k) = key {
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
    }
}
