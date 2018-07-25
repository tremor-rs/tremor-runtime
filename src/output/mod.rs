//! This module handles outputs

use elastic;
use elastic::client::requests::BulkRequest;
use elastic::client::responses::BulkResponse;
use elastic::client::{AsyncSender, Client};
use elastic::prelude::AsyncClientBuilder;
use error::TSError;
use futures::Future;
use grouping::MaybeMessage;
use prometheus::Counter;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std;
use std::collections::HashMap;
use std::mem;
use std::time::{Duration, Instant};
use std::vec::Vec;
use tokio_core::reactor::Core;

lazy_static! {
    /*
     * Number of errors read received from the input
     */
    static ref OUTPUT_DROPPED: Counter =
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
    Elastic(ElasticOutput),
    Stdout(StdoutOutput),
    Debug(DebugOutput),
}

/// Implements the Output trait for the enum.
/// this needs to ba adopted for each implementation.
impl Output for Outputs {
    fn send(&mut self, msg: MaybeMessage) -> Result<(), TSError> {
        match self {
            Outputs::Kafka(o) => o.send(msg),
            Outputs::Elastic(o) => o.send(msg),
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

/// An output that write to stdout
pub struct StdoutOutput {
    pfx: String,
}

impl StdoutOutput {
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

impl KafkaOutput {
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
            OUTPUT_DROPPED.inc();
            Ok(())
        }
    }
}

pub struct ElasticOutput {
    core: Core,
    client: Client<AsyncSender>,
    index: String,
    flushTxTimeoutMillis: u32,
    // queue: Vec<&'a MaybeMessage<'a>>,
    qidx: usize,
    qlimit: usize,
    payload: String,
}

impl ElasticOutput {
    /// Creates a new output connector, `brokers` is a coma seperated list of
    /// brokers to connect to. `topic` is the topic to send to.
    fn new(opts: &str) -> Self {
        let opts: Vec<&str> = opts.split('|').collect();
        match opts.as_slice() {
            [_endpoint,index,batchSize,batchTimeout] => {
                let mut core = Core::new().unwrap();
                
                let client= AsyncClientBuilder::new().build(&core.handle()).unwrap();
                let index = index.clone();
                let flushBatchSize: usize = batchSize.parse::<u16>().unwrap() as usize;
                let flushTxTimeoutMillis: u32 = batchTimeout.parse::<u32>().unwrap();
                ElasticOutput {
                    core: core,
                    client: client,
                    index: index.to_string(),
                    flushTxTimeoutMillis: flushTxTimeoutMillis,
                    qidx: 0,
                    qlimit: flushBatchSize-1,
                    payload: String::new(),
                }
            }
            _ => panic!("Invalid options for Elastic output, use <endpoint>|<index>|<batchSize>|<batchTimeout>"),
        }
    }

    fn flush(self: &mut Self, payload: &'static str) -> Result<(), TSError> {
        let res_future = self.client
            .request(BulkRequest::new(payload.clone()))
            .send()
            .and_then(|res| res.into_response::<BulkResponse>());

        let bulk_future = res_future.and_then(|bulk| {
            for op in bulk {
                match op {
                    Ok(op) => println!("ok: {:?}", op),
                    Err(op) => println!("err: {:?}", op),
                }
            }

            Ok(())
        });

        self.core.run(bulk_future)?;

        Ok(())
    }
}

impl std::convert::From<elastic::Error> for TSError {
    fn from(_from: elastic::Error) -> TSError {
        TSError::new("Elastic spastic is not fantastic")
    }
}

impl Output for ElasticOutput {
    fn send(&mut self, msg: MaybeMessage) -> Result<(), TSError> {
        if !msg.drop {
            if self.qidx < self.qlimit {
                self.payload.push_str(
                    format!(
                        "{{\"index\": \"{}\",\"type\": \"{}\",\"_id\": {}}}",
                        self.index.as_str(),
                        "_doc",
                        self.qidx
                    ).as_str(),
                );
                self.payload.push('\n');
                self.payload.push_str(msg.msg.raw);
                self.payload.push('\n');
                self.qidx += 1;
            } else {
                // TODO technically this waits until a message event occurs which
                // would overflow the bounded queue. This is not ideal as it biases
                // in favour of frequently delivered messages not infrequent or
                // aperiodic sources where arbitrary ( until the next batch limit is
                // reached ) delays may result in queued data not being delivered in
                // a timely and responsive manner.
                //
                // We let this hang wet ( unresolved ) in the POC for expedience and simplicity
                //
                // At this point the queue is at capacity, so we drain the queue to elastic
                // search via its bulk index API
                let payload = self.payload.clone();
                let payload_str = unsafe { mem::transmute(payload.as_str()) };
                self.flush(payload_str);
                self.payload.clear();
                self.payload.push_str(
                    format!(
                        "{{\"index\": \"{}\",\"type\": \"{}\",\"_id\": {}}}",
                        self.index.as_str(),
                        "_doc",
                        self.qidx
                    ).as_str(),
                );
                self.payload.push('\n');
                self.payload.push_str(msg.msg.raw);
                self.payload.push('\n');
                self.qidx = 1;
            }

            OUTPUT_DELIVERED.inc();
        } else {
            OUTPUT_DROPPED.inc();
        }
        Ok(())
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
