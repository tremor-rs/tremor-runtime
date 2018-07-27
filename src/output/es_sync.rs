use elastic;
use elastic::client::prelude::BulkErrorsResponse;
use elastic::client::requests::BulkRequest;
use elastic::client::{Client, SyncSender};
use elastic::prelude::SyncClientBuilder;
use error::TSError;
use grouping::MaybeMessage;
use output::utils::{Output as OutputT, OUTPUT_DELIVERED, OUTPUT_DROPPED};
use prometheus::Histogram;
use std::convert::From;
use std::f64;
use std::time::Instant;
use std::vec::Vec;

lazy_static! {
    /*
     * Histogram of the duration it takes between getting a message and
     * sending (or dropping) it.
     */
    static ref SEND_HISTOGRAM: Histogram = register_histogram!(
        "ts_es_latency",
        "Latency for logstash output.",
        vec![
            0.0005, 0.001, 0.0025,
             0.005, 0.01, 0.025,
             0.05, 0.1, 0.25,
             0.5, 1.0, 2.5,
             5.0, 10.0, 25.0,
             50.0, 100.0, 250.0,
             500.0, 1000.0, 2500.0,
             f64::INFINITY]
    ).unwrap();

}
pub struct Output {
    client: Client<SyncSender>,
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
                let client = SyncClientBuilder::new().base_url(*endpoint).build().unwrap();
                let index = index.clone();
                let flush_batch_size: usize = batch_size.parse::<u16>().unwrap() as usize;
                let flush_tx_timeout_millis: u32 = batch_timeout.parse::<u32>().unwrap();
                Output {
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

    fn flush(self: &mut Self, payload: &str) -> Result<Option<f64>, TSError> {
        let start = Instant::now();
        let timer = SEND_HISTOGRAM.start_timer();
        let req = BulkRequest::for_index_ty(self.index.to_owned(), "_doc", payload.to_owned());
        self.client
            .request(req)
            .send()?
            .into_response::<BulkErrorsResponse>()?;
        timer.observe_duration();
        let d = start.elapsed();
        let d = (d.as_secs() * 1_000) as f64 + (d.subsec_nanos() / 1_000_000) as f64;
        Ok(Some(d))
    }
}

impl From<elastic::Error> for TSError {
    fn from(from: elastic::Error) -> TSError {
        TSError::new(format!("{}", from).as_str())
    }
}

impl OutputT for Output {
    fn send(&mut self, msg: MaybeMessage) -> Result<Option<f64>, TSError> {
        if !msg.drop {
            if self.qidx < self.qlimit {
                //{ "create" : { "_index" : "test", "_type" : "_doc", "_id" : "3" } }
                self.payload.push_str(
                    json!({
                        "index":
                        {
                            "_index": self.index,
                            "_type": "_doc"
                        }}).to_string()
                        .as_str(),
                );
                self.payload.push('\n');
                self.payload.push_str(msg.msg.raw);
                self.payload.push('\n');
                self.qidx += 1;
                Ok(None)
            } else {
                let c = self.qidx;
                self.qidx = 1;
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
                let r = self.flush(payload.as_str());
                match r {
                    Ok(_) => OUTPUT_DELIVERED.inc_by(c as i64),
                    Err(_) => OUTPUT_DROPPED.inc_by(c as i64),
                }

                self.payload.clear();
                self.payload.push_str(
                    json!({
                        "index":
                        {
                            "_index": self.index,
                            "_type": "_doc"
                        }}).to_string()
                        .as_str(),
                );
                self.payload.push('\n');
                self.payload.push_str(msg.msg.raw);
                self.payload.push('\n');
                r
            }
        } else {
            OUTPUT_DROPPED.inc();
            Ok(None)
        }
    }
}
