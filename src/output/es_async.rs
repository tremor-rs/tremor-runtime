/// Elastic search output with incremental backoff.
///
/// The algoritm is as follows:
///

/// Inputs:
///   * batch_size - number of messages in each batch.
///   * timeout - timeout for a write before we back off.
///   * concurrency - number of paralell batches.
///   * backoffs - array of backoffs to match.
///
/// Variables:
///   * backoff - additional delay after timed out send.
///
/// Pseudo variables:
///   * batch - collection of messages.
///   * queue - a queue of fugure sends.
///
/// Pseudocode:
/// ```
/// for m in messages {
///   if now() - last_done > backoff {
///     batch.add(m)
///     if batch.size >= batch_size {
///       if queue.size < concurrency {
///         queue.push(batch.send())
///       } else {
///         future = queue.first
///         if future.is_done {
///           queue.pop()
///           if future.execution_time < timeout {
///             backoff = 0;
///           } else {
///             backoff = grow_backoff(backoff) // backoff increase logic
///           }
///           last_done = now();
///           queue.push(batch.send())
///         } else {
///           batch.drop();
///         }
///       }
///     }
///   }
/// }
/// ```
use elastic::client::prelude::BulkErrorsResponse;
use elastic::client::requests::BulkRequest;
use elastic::client::{Client, SyncSender};
use elastic::prelude::SyncClientBuilder;
use error::TSError;
use output::{OUTPUT_DELIVERED, OUTPUT_DROPPED, OUTPUT_SKIPED};
use pipeline::{Event, Step};
use prometheus::{Gauge, Histogram};
use serde_json::{self, Value};
//use std::collections::HashMap;
use std::collections::VecDeque;
use std::convert::From;
use std::f64;
use std::sync::mpsc::{channel, Receiver};
use std::time::Duration;
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};
use threadpool::ThreadPool;

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
    static ref BACKOFF_GAUGE: Gauge = register_gauge!(opts!(
        "ts_es_backoff_ms",
        "Current backoff in millis."
    )).unwrap();

}

fn default_threads() -> usize {
    5
}

fn default_concurrency() -> usize {
    5
}

fn default_backoff() -> Vec<u64> {
    vec![50, 100, 250, 500, 1000, 5000, 100000, 500000]
}

//[endpoint, index, batch_size, batch_timeout]
#[derive(Deserialize, Debug)]
struct Config {
    endpoint: String,
    index: String,
    batch_size: usize,
    batch_timeout: f64,
    #[serde(default = "default_backoff")]
    backoff_rules: Vec<u64>,
    #[serde(default = "default_threads")]
    threads: usize,
    #[serde(default = "default_concurrency")]
    concurrency: usize,
}

impl Config {
    pub fn next_backoff(&self, last_backoff: u64) -> u64 {
        for backoff in self.backoff_rules.iter() {
            if *backoff > last_backoff {
                return *backoff;
            }
        }
        last_backoff
    }
}

struct AsyncSink<T> {
    queue: VecDeque<Receiver<Result<T, TSError>>>,
    capacity: usize,
    size: usize,
}

enum SinkEnqueueError {
    AtCapacity,
}

enum SinkDequeueError {
    Empty,
    NotReady,
}

impl<T> AsyncSink<T> {
    pub fn new(capacity: usize) -> Self {
        AsyncSink {
            queue: VecDeque::with_capacity(capacity),
            capacity: capacity,
            size: 0,
        }
    }
    pub fn enqueue(&mut self, value: Receiver<Result<T, TSError>>) -> Result<(), SinkEnqueueError> {
        if self.size >= self.capacity {
            println!("size({}) >= capacity({})", self.size, self.capacity);
            Err(SinkEnqueueError::AtCapacity)
        } else {
            self.size += 1;
            Ok(self.queue.push_back(value))
        }
    }
    pub fn dequeue(&mut self) -> Result<Result<T, TSError>, SinkDequeueError> {
        match self.queue.pop_front() {
            None => Err(SinkDequeueError::Empty),
            Some(rx) => match rx.try_recv() {
                Err(_) => {
                    self.queue.push_front(rx);
                    Err(SinkDequeueError::NotReady)
                }
                Ok(result) => {
                    self.size -= 1;
                    Ok(result)
                }
            },
        }
    }
    pub fn has_capacity(&self) -> bool {
        self.size < self.capacity
    }
}

impl From<SinkEnqueueError> for TSError {
    fn from(e: SinkEnqueueError) -> TSError {
        match e {
            SinkEnqueueError::AtCapacity => TSError::new("Queue overflow"),
        }
    }
}
pub struct Output {
    client: Client<SyncSender>,
    backoff: u64,
    queue: AsyncSink<f64>,
    qidx: usize,
    payload: String,
    last_flush: Instant,
    pool: ThreadPool,
    config: Config,
}

impl Output {
    /// Creates a new output connector, `brokers` is a coma seperated list of
    /// brokers to connect to. `topic` is the topic to send to.
    pub fn new(opts: &str) -> Self {
        match serde_json::from_str(opts) {
            Ok(conf @ Config{..}) => {
                let client = SyncClientBuilder::new().base_url(conf.endpoint.clone()).build().unwrap();
                let pool = ThreadPool::new(conf.threads);
                let queue = AsyncSink::new(conf.concurrency);
                Output {
                    config: conf,
                    backoff: 0,
                    pool: pool,
                    client: client,
                    qidx: 0,
                    payload: String::new(),
                    last_flush: Instant::now(),
                    queue: queue
                }
            }
            _ => panic!("Invalid options for Elastic output, use <endpoint>|<index>|<batchSize>|<batchTimeout>"),
        }
    }

    fn send_future(&self) -> Receiver<Result<f64, TSError>> {
        let payload = self.payload.clone();
        let index = self.config.index.clone();
        let client = self.client.clone();
        let c = self.qidx;
        let (tx, rx) = channel();
        self.pool.execute(move || {
            let r = flush(client, index, payload.as_str());
            match r.clone() {
                Ok(_) => OUTPUT_DELIVERED.inc_by(c as i64),
                Err(e) => {
                    println!("Error: {:?}", e);
                    OUTPUT_DROPPED.inc_by(c as i64);
                }
            };
            let _ = tx.send(r);
        });
        rx
    }
    fn inc_backoff(&mut self) {
        self.backoff = self.config.next_backoff(self.backoff);
        BACKOFF_GAUGE.set(self.backoff as f64);
    }

    fn reset_backoff(&mut self) {
        self.backoff = 0;
        BACKOFF_GAUGE.set(self.backoff as f64);
    }
}

fn flush(client: Client<SyncSender>, index: String, payload: &str) -> Result<f64, TSError> {
    let start = Instant::now();
    let timer = SEND_HISTOGRAM.start_timer();
    let req = BulkRequest::for_index_ty(index.to_owned(), "_doc", payload.to_owned());
    client
        .request(req)
        .send()?
        .into_response::<BulkErrorsResponse>()?;
    timer.observe_duration();
    let d = start.elapsed();
    let d = duration_to_millis(d) as f64;
    Ok(d)
}

fn duration_to_millis(at: Duration) -> u64 {
    (at.as_secs() as u64 * 1_000) + (at.subsec_nanos() as u64 / 1_000_000)
}

fn update_send_time(event: Event) -> Result<String, serde_json::Error> {
    match event.parsed {
        Value::Object(mut m) => {
            let start = SystemTime::now();
            let since_the_epoch = start
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards");
            let tremor_map: serde_json::Map<String, Value> = [
                (
                    String::from("send_time"),
                    Value::Number(serde_json::Number::from(duration_to_millis(
                        since_the_epoch,
                    ))),
                ),
                (
                    String::from("classification"),
                    Value::String(event.classification),
                ),
            ].iter()
                .cloned()
                .collect();
            m.insert(String::from("_tremor"), Value::Object(tremor_map));
            serde_json::to_string(&Value::Object(m))
        }
        _ => serde_json::to_string(&event.parsed),
    }
}

impl Step for Output {
    fn apply(&mut self, event: Event) -> Result<Event, TSError> {
        let d = duration_to_millis(self.last_flush.elapsed());
        // We only add the message if it is not already dropped and
        // we are not in backoff time.
        if !event.drop && d >= self.backoff {
            self.payload.push_str(
                json!({
                    "index":
                    {
                        "_index": self.config.index,
                        "_type": "_doc"
                    }}).to_string()
                    .as_str(),
            );
            self.payload.push('\n');
            let out_event = event.clone();
            self.payload
                .push_str(update_send_time(event).unwrap().as_str());
            self.payload.push('\n');
            self.qidx += 1;

            if self.config.batch_size > self.qidx {
                Ok(out_event)
            } else {
                let r = match self.queue.dequeue() {
                    Err(SinkDequeueError::NotReady) => {
                        if self.queue.has_capacity() {
                            let rx = self.send_future();
                            self.queue.enqueue(rx)?;
                        } else {
                            OUTPUT_DROPPED.inc_by(self.qidx as i64);
                        };
                        Ok(out_event)
                    }
                    Err(SinkDequeueError::Empty) => {
                        let rx = self.send_future();
                        self.queue.enqueue(rx)?;
                        Ok(out_event)
                    }
                    Ok(result) => {
                        let rx = self.send_future();
                        self.queue.enqueue(rx)?;
                        match result {
                            Ok(rtt) if rtt > self.config.batch_timeout as f64 => {
                                self.inc_backoff();
                                let mut event = Event::from(out_event);
                                event.feedback = Some(rtt);
                                Ok(event)
                            }
                            Err(e) => {
                                self.inc_backoff();
                                Err(e)
                            }
                            Ok(rtt) => {
                                self.reset_backoff();
                                let mut event = Event::from(out_event);
                                event.feedback = Some(rtt);
                                Ok(event)
                            }
                        }
                    }
                };
                self.payload.clear();
                self.qidx = 0;
                r
            }
        } else {
            OUTPUT_SKIPED.inc();
            Ok(event)
        }
    }
}

// We don't do this in a test module since we need to access private functions.
#[test]
fn backoff_test() {
    let c = Config {
        endpoint: String::from(""),
        index: String::from(""),
        batch_size: 10,
        batch_timeout: 10.0,
        backoff_rules: vec![10, 20, 30, 40],
        threads: 5,
        concurrency: 5,
    };
    assert_eq!(c.next_backoff(0), 10);
    assert_eq!(c.next_backoff(5), 10);
    assert_eq!(c.next_backoff(10), 20);
}
