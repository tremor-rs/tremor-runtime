use error::TSError;
use futures::Future;
use output::{OUTPUT_DELIVERED, OUTPUT_DROPPED};
use pipeline::{Event, Step};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

/// Kafka output connector
pub struct Output {
    producer: FutureProducer,
    topic: String,
}

impl Output {
    /// Creates a new output connector, `brokers` is a coma seperated list of
    /// brokers to connect to. `topic` is the topic to send to.

    pub fn new(opts: &str) -> Self {
        let opts: Vec<&str> = opts.split('|').collect();
        match opts.as_slice() {
            [topic, brokers] => {
                let producer = ClientConfig::new()
                    .set("bootstrap.servers", brokers)
                    .set("queue.buffering.max.ms", "0")  // Do not buffer
                    .create()
                    .expect("Producer creation failed");
                Output {
                    producer,
                    topic: String::from(*topic),
                }
            }
            _ => panic!("Invalid options for Kafka output, use <topioc>|<producers>"),
        }
    }
}

impl Step for Output {
    fn apply(&mut self, event: Event) -> Result<Event, TSError> {
        if !event.drop {
            OUTPUT_DELIVERED.inc();
            let out_event = event.clone();
            let mut record = FutureRecord::to(self.topic.as_str());
            record = record.payload(event.raw.as_str());
            let r = if let Some(k) = event.key {
                self.producer.send(record.key(k.as_str()), 1000).wait()
            } else {
                self.producer.send(record, 1000).wait()
            };

            if r.is_ok() {
                Ok(out_event)
            } else {
                Err(TSError::new("Send failed"))
            }
        } else {
            OUTPUT_DROPPED.inc();
            Ok(event)
        }
    }
}
