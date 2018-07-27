use elastic;
use elastic::client::requests::BulkRequest;
use elastic::client::responses::BulkResponse;
use elastic::client::{AsyncSender, Client};
use elastic::prelude::AsyncClientBuilder;
use error::TSError;
use futures::Future;
use grouping::MaybeMessage;
use output::utils::{Output as OutputT, OUTPUT_DELIVERED, OUTPUT_DROPPED};
use std::convert::From;
use std::mem;
use std::time::Instant;
use std::vec::Vec;
use tokio_core::reactor::Core;

pub struct Output {
    core: Core,
    client: Client<AsyncSender>,
    index: String,
    flush_tx_timeout_millis: u32,
    // queue: Vec<&'a MaybeMessage<'a>>,
    qidx: usize,
    qlimit: usize,
    payload: String,
}

impl Output {
    /// Creates a new output connector, `brokers` is a coma seperated list of
    /// brokers to connect to. `topic` is the topic to send to.
    pub fn new(opts: &str) -> Self {
        let opts: Vec<&str> = opts.split('|').collect();
        match opts.as_slice() {
            [endpoint, index, batch_size, batch_timeout] => {
                let mut core = Core::new().unwrap();

                let client = AsyncClientBuilder::new().base_url(*endpoint)
.build(&core.handle()).unwrap();
                let index = index.clone();
                let flush_batch_size: usize = batch_size.parse::<u16>().unwrap() as usize;
                let flush_tx_timeout_millis: u32 = batch_timeout.parse::<u32>().unwrap();
                println!("batch size: {}", flush_batch_size);
                Output {
                    core: core,
                    client: client,
                    index: index.to_string(),
                    flush_tx_timeout_millis: flush_tx_timeout_millis,
                    qidx: 0,
                    qlimit: flush_batch_size - 1,
                    payload: String::new(),
                }
            }
            _ => panic!("Invalid options for Elastic output, use <endpoint>|<index>|<batchSize>|<batchTimeout>"),
        }
    }

    fn flush(self: &mut Self, payload: &str) -> Result<(), TSError> {
        let start = Instant::now();
        let res_future = self.client
            .request(BulkRequest::new(payload.to_owned()))
            .send()
            .and_then(|res| res.into_response::<BulkResponse>());

        let bulk_future = res_future.and_then(|bulk| {
            for op in bulk {
                match op {
                    Ok(op) => println!("ok: {:?}", op),
                    Err(op) => println!("err: {:?}", op),
                }
            }
            let d = start.elapsed();
            let d = (d.as_secs() * 1_000) as f64 + (d.subsec_nanos() / 1_000_000) as f64;

            Ok(d)
        });

        let r = self.core.run(bulk_future)?;
        println!("r: {}", r);

        Ok(())
    }
}

impl From<elastic::Error> for TSError {
    fn from(_from: elastic::Error) -> TSError {
        TSError::new("Elastic spastic is not fantastic")
    }
}

impl OutputT for Output {
    fn send(&mut self, msg: MaybeMessage) -> Result<Option<f64>, TSError> {
        if !msg.drop {
            if self.qidx < self.qlimit {
                println!("msg: {}", self.qidx);
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
                self.flush(payload.as_str())?;
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
        Ok(None)
    }
}
