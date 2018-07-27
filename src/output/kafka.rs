use error::TSError;
use futures::Future;
use grouping::MaybeMessage;
use output::utils::{Output as OutputT, OUTPUT_DELIVERED, OUTPUT_DROPPED};
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
                    producer: producer,
                    topic: String::from(*topic),
                }
            }
            _ => panic!("Invalid options for Kafka output, use <topioc>|<producers>"),
        }
    }
}

impl OutputT for Output {
    fn send<'a>(&mut self, msg: MaybeMessage<'a>) -> Result<Option<f64>, TSError> {
        if !msg.drop {
            OUTPUT_DELIVERED.inc();
            let mut record = FutureRecord::to(self.topic.as_str());
            record = record.payload(msg.msg.raw);
            if let Some(k) = msg.key {
                record = record.key(k);
            };
            let r = self.producer.send(record, 1000).wait();

            if r.is_ok() {
                Ok(None)
            } else {
                Err(TSError::new("Send failed"))
            }
        } else {
            OUTPUT_DROPPED.inc();
            Ok(None)
        }
    }
}
