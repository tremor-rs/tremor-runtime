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
/// ```pseudo,no_run
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
use async_sink::{AsyncSink, SinkDequeueError};
use error::TSError;
use output::{self, OUTPUT_DELIVERED, OUTPUT_DROPPED, OUTPUT_ERROR, OUTPUT_SKIPPED};
use pipeline::{Event, Step};
use prometheus::{Gauge, HistogramVec};
use reqwest;
use serde_json;
use std::f64;
use std::sync::mpsc::{channel, Receiver};
use std::time::Instant;
use threadpool::ThreadPool;
use utils::duration_to_millis;

lazy_static! {
    // Histogram of the duration it takes between getting a message and
    // sending (or dropping) it.
    static ref SEND_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "ts_influx_latency",
        "Latency for influx output.",
        &["dest"],
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
        "ts_influx_backoff_ms",
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
    vec![50, 100, 250, 500, 1000, 5000, 10000]
}

//[endpoints, index, batch_size, batch_timeout]
#[derive(Deserialize, Debug)]
struct Config {
    endpoints: Vec<String>,
    batch_size: usize,
    batch_timeout: f64,
    #[serde(default = "default_backoff")]
    backoff_rules: Vec<u64>,
    #[serde(default = "default_threads")]
    threads: usize,
    #[serde(default = "default_concurrency")]
    concurrency: usize,
    database: String,
}

impl Config {
    pub fn next_backoff(&self, last_backoff: u64) -> u64 {
        for backoff in &self.backoff_rules {
            if *backoff > last_backoff {
                return *backoff;
            }
        }
        last_backoff
    }
}

#[derive(Clone)]
struct Destination {
    client: reqwest::Client,
    url: String,
}

// Influx output connector
pub struct Output {
    client_idx: usize,
    clients: Vec<Destination>,
    backoff: u64,
    queue: AsyncSink<f64>,
    qidx: usize,
    payload: String,
    last_flush: Instant,
    pool: ThreadPool,
    config: Config,
}

impl Output {
    pub fn new(opts: &str) -> Self {
        match serde_json::from_str(opts) {
            Ok(config @ Config{..}) => {
                let clients = config.endpoints.iter().map(|client| Destination{
                    client: reqwest::Client::new(),
                    url: format!("{}/write?db={}", client, config.database)
                }).collect();
                let pool = ThreadPool::new(config.threads);
                let queue = AsyncSink::new(config.concurrency);
                Output {
                    client_idx: 0,
                    config,
                    backoff: 0,
                    pool,
                    clients,
                    qidx: 0,
                    payload: String::new(),
                    last_flush: Instant::now(),
                    queue
                }
            }
            _ => panic!("Invalid options for InfluxDB output, use `{{\"endpoints\":[\"<url>\"[, ...]], \"batch_size\":<size of each batch>, \"batch_timeout\": <maximum allowed timeout per batch>, \"database\": \"<database>\", [ \"threads\": <number of threads used to serve asyncornous writes>, \"concurrency\": <maximum number of batches in flight at any time>, \"backoff_rules\": [<1st timeout in ms>, <second timeout in ms>, ...]]}}`"),
        }
    }

    fn send_future(&mut self, step: &'static str, drop: bool) -> Receiver<Result<f64, TSError>> {
        self.client_idx = (self.client_idx + 1) % self.clients.len();
        let payload = self.payload.clone();
        let destination = self.clients[self.client_idx].clone();
        let c = self.qidx;
        let (tx, rx) = channel();
        self.pool.execute(move || {
            let dst = destination.url.as_str();
            let r = flush(&destination.client, dst, payload.as_str());
            match r.clone() {
                Ok(_) => if drop {
                    OUTPUT_SKIPPED.with_label_values(&[step, dst]).inc();
                } else {
                    OUTPUT_DELIVERED
                        .with_label_values(&[step, dst])
                        .inc_by(c as i64);
                },
                Err(e) => {
                    println!("Error: {:?}", e);
                    OUTPUT_ERROR
                        .with_label_values(&[step, dst])
                        .inc_by(c as i64);
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

fn flush(client: &reqwest::Client, url: &str, payload: &str) -> Result<f64, TSError> {
    let start = Instant::now();
    let timer = SEND_HISTOGRAM.with_label_values(&[url]).start_timer();
    client.post(url).body(payload.to_owned()).send()?;
    timer.observe_duration();
    let d = start.elapsed();
    let d = duration_to_millis(d) as f64;
    Ok(d)
}

impl Step for Output {
    fn shutdown(&mut self) {
        self.queue.empty();
        let destination = self.clients[self.client_idx].clone();
        let dst = destination.url.as_str();
        let _r = flush(&destination.client, dst, self.payload.as_str());
    }

    fn apply(&mut self, event: Event) -> Result<Event, TSError> {
        let d = duration_to_millis(self.last_flush.elapsed());
        // We only add the message if it is not already dropped and
        // we are not in backoff time.
        let output_step = output::step(&event);
        if d <= self.backoff {
            OUTPUT_DROPPED
                .with_label_values(&[output_step, "<backoff>"])
                .inc();
            let mut event = event;
            event.drop = true;
            Ok(event)
        } else {
            let mut out_event = event.clone();
            let drop = event.drop;
            self.payload.push_str(event.raw.as_str());
            self.payload.push('\n');
            self.qidx += 1;

            if self.config.batch_size > self.qidx {
                Ok(out_event)
            } else {
                let r = match self.queue.dequeue() {
                    Err(SinkDequeueError::NotReady) => {
                        if self.queue.has_capacity() {
                            let rx = self.send_future(output_step, drop);
                            self.queue.enqueue(rx)?;
                        } else {
                            OUTPUT_DROPPED
                                .with_label_values(&[output_step, "<overload>"])
                                .inc_by(self.qidx as i64);
                            out_event.drop = true;
                        };
                        Ok(out_event)
                    }
                    Err(SinkDequeueError::Empty) => {
                        let rx = self.send_future(output_step, drop);
                        self.queue.enqueue(rx)?;
                        Ok(out_event)
                    }
                    Ok(result) => {
                        let rx = self.send_future(output_step, drop);
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
        }
    }
}

// We don't do this in a test module since we need to access private functions.
#[test]
fn backoff_test() {
    let c = Config {
        endpoints: vec![String::from("")],
        batch_size: 10,
        batch_timeout: 10.0,
        backoff_rules: vec![10, 20, 30, 40],
        threads: 5,
        concurrency: 5,
        database: String::from("db"),
    };
    assert_eq!(c.next_backoff(0), 10);
    assert_eq!(c.next_backoff(5), 10);
    assert_eq!(c.next_backoff(10), 20);
}
