// Copyright 2021, The Tremor Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(clippy::doc_markdown)]
//! :::info
//!
//! This connector is not intended for production use, but for testing the Tremor runtime itself. To enable it pass `--debug-connectors` to tremor.
//!
//! :::
//!
//! The `bench` connector enables controlled micro-benchmarking of tremor-based
//! applications. Benchmarks and micro-benchmarking are an important part of the
//! performance engineering practices of the tremor authors.
//!
//! We maintain and publish [benchmark results](https://www.tremor.rs/benchmarks/) that are
//! published every time code is commited to the main [tremor runtime](https://github.com/tremor-rs/tremor-runtime) git repository
//! using bare metal infrastructure provided by the [CNCF](https://www.cncf.io) on [Equinix Metal](https://metal.equinix.com/)
//! where we host a simple [continuous benchmarking service](https://github.com/tremor-rs/tremor-benchmark) designed for this
//! purpose.
//!
//! ## Configuration
//!
//! | Config Option         | Description                                                                                                                                                                                     | Possible Values  | Required / Optional | Default Value |
//! |-----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|---------------------|---------------|
//! | `path`                | The source file to read data from, can be xz compressed                                                                                                                                         | file path        | required            |               |
//! | `interval`            | The interval between single events in nanoseconds. Set to `0` if you want events emitted as fast as possible.                                                                                   | positive integer | optional            | `0`           |
//! | `chunk_size`          | if provided, the `source` file data will be split into chunks of the given size, instead of split into lines.                                                                                   | positive integer | optional            |               |
//! | `iters`               | Number of iterations through the whole `source` data to stop after. If not provided (and `stop_after_secs` is also not provided), the connector will iterate over the `source` data infinitely. | positive integer | optional            |               |
//! | `base64`              | If set to `true`, the `source` data will be `base64` decoded.                                                                                                                                   | boolean          | optional            | `false`       |
//! | `is_transactional`    | If set to `true`, events will be emitted as transactional (requiring ack/fail contraflow messages).                                                                                             | boolean          | optional            | `false`       |
//! | `structured`          | If set to `true` the benchmark report is output as JSON, if set to `false` it is printed in human-readable form.                                                                                | boolean          | optional            | `false`       |
//! | `stop_after_secs`     | Number of seconds after which the benchmark should be stopped.                                                                                                                                  | positive integer | optional            |               |
//! | `significant_figures` | Digits of precision for latency HDR histogram results.                                                                                                                                          | positive integer | optional            | `2`           |
//! | `warmup_secs`         | Number of seconds to warm up. Events during this time are not accounted for in the latency measurements.                                                                                        | positive integer | optional            | `0`           |
//!
//! ### Example
//!
//! ```tremor title="example.troy"
//! use std::time::nanos;
//! define connector bench from bench
//! with
//!   codec = "json",                       # Decode each line as a JSON document
//!   config = {
//!     "source": "in.json",                # Take the source data from `in.json` and turn each line into an event
//!     "interval": nanos::from_millis(1),  # Wait for 1ms between each event
//!     "iters": 1,                         # Iterate only once through the data in `in.json`
//!   }
//! end;
//! ```
//!
//! ## Operation
//!
//! The `bench` connector consists of two parts. The *source* part for generating synthetical loads of events and the *sink* part, measuring how many events it got and the latency of each event.
//!
//! The *source* part will load an emulated source of events from a (possibly `xz` compressed) file and load them into memory.
//! It is required to send the emitted events to the *sink* part via the `in` port of the same connector eventually.
//! The pipelines and connectors in between can be considered the system that is subject to the benchmark.
//!
//! The *source* part is replaying its contents for the duration of the test, or until the number of configured test iterations has been
//! exceeded; whichever happens first. Once the test has been stopped a high dynamic range [HDR Histogram](http://hdrhistogram.org/)
//! is produced and printed to stdout. The histogram can be loaded into the web based [histogram plotting tool](http://hdrhistogram.github.io/HdrHistogram/plotFiles.html) for analysis.
//!
//! Once the latency histogram and throughput measures have been emitted, the tremor runtime process is halted.
//!
//! ## How do I write a benchmark?
//!
//! The most important part of writing a benchmark with the `bench` connector is that the *source* part
//! needs to be the source of events. Usually the *source* part emits events as fast as it possibly can, in order to see how much the whole system is actually able to handle inm the best case.
//!
//! The *sink* part needs to receive the events eventually, otherwise the benchmark does not measure anything. In that case the `bench` connector can be used as a load generator.
//!
//! A complete benchmark will define the `bench` connector as in the
//! configuration example above with a system under test defined in a deployment file. A full
//! example is provided for illustration.
//!
//! ```tremor title="config.troy"
//! define flow main
//! flow
//!   use tremor::connectors;
//!
//!   define connector bench from bench
//!   with
//!     codec = "json",
//!     config = {
//!       "source": "in.json",
//!       "stop_after_secs": 10,
//!       "warmup_secs": 2
//!     }
//!   end;
//!   create connector bench;
//!
//!   define pipeline bench_me
//!   pipeline
//!     # this is just a dummy pipeline.
//!     # What we actually benchmark here is how much throughput the vanilla tremor runtime
//!     # without any application logic can achieve.
//!     select event from in into out;
//!   end;
//!   create pipeline bench_me;
//!
//!   # send synthetical load of events to the pipeline
//!   connect /connector/bench to /pipeline/bench_me;
//!   # send events to the bench connector for measuring and reporting
//!   connect /pipeline/bench_me to /connector/bench;
//! end;
//!
//! deploy flow main;
//! ```
//!
//! This is a test of the benchmark connector itself that is exercised as part of our CI system, it can be run
//! manually as follows:
//!
//! ```bash
//! $ git clone https://github.com/tremor-rs/tremor-runtime
//! $ cd tremor-runtime
//! $ cargo build --all --release # grab a coffee, this takes a while
//! $ cd tremor-cli/tests/integration/blaster # Piu piu!
//! $ export TREMOR_PATH=/path/to/tremor-runtime/tremor-script/lib
//! $ tremor test bench .
//!   Running `target/debug/tremor test bench -v temp/bench`
//!   Benchmark: Running bench
//!     Tags:  bench
//!
//!        | Value Percentile TotalCount 1/(1-Percentile)
//!        |
//!        | 8575 0.00000          1           1.00
//!        | 15679 0.25000     249319           1.33
//!        | 18559 0.50000     507133           2.00
//!        | 20223 0.62500     626440           2.67
//!        | 25087 0.75000     746698           4.00
//!        | 237567 0.81250     808828           5.33
//!        | 770047 0.87500     871282           8.00
//!        | 1146879 0.90625     902347          10.67
//!        | 1703935 0.93750     933194          16.00
//!        | 2097151 0.95312     948800          21.33
//!        | 2506751 0.96875     964503          32.00
//!        | 2670591 0.97656     972719          42.67
//!        | 2818047 0.98438     979971          64.00
//!        | 2981887 0.98828     983951          85.33
//!        | 3309567 0.99219     987641         128.00
//!        | 3571711 0.99414     989559         170.67
//!        | 3964927 0.99609     991540         256.00
//!        | 4259839 0.99707     992494         341.33
//!        | 4751359 0.99805     993481         512.00
//!        | 5079039 0.99854     993936         682.67
//!        | 5439487 0.99902     994468        1024.00
//!        | 5537791 0.99927     994776        1365.33
//!        | 5668863 0.99951     994894        2048.00
//!        | 6029311 0.99963     995017        2730.67
//!        | 6225919 0.99976     995133        4096.00
//!        | 6619135 0.99982     995194        5461.33
//!        | 6914047 0.99988     995349        8192.00
//!        | 6914047 0.99991     995349       10922.67
//!        | 6914047 0.99994     995349       16384.00
//!        | 6914047 0.99995     995349       21845.33
//!        | 6914047 0.99997     995349       32768.00
//!        | 6946815 0.99998     995376       43690.67
//!        | 6946815 1.00000     995376            inf
//!        | #[Mean       =    285758.86, StdDeviation   =    701828.62]
//!        | #[Max        =      6946815, Total count    =       995376]
//!        | #[Buckets    =           30, SubBuckets     =         3968]
//!        |
//!        |
//!        | Throughput   (data): 0.5 MB/s
//!        | Throughput (events): 99.5k events/s
//!
//!   Elapsed: 12s 40ms
//! ```
//!
//! ## Constraints
//!
//! It is an error to attempt to run a benchmark ( any deployment using the `bench` connector )
//! via the regular server execution command in the `tremor` command line interface
//!
//! ```bash
//! ➜  blaster git:(main) ✗ tremor server run config.troy
//! tremor version: 0.12
//! tremor instance: tremor
//! rd_kafka version: 0x000002ff, 1.8.2
//! allocator: snmalloc
//! [2022-04-12T14:38:20Z ERROR tremor_runtime::system] Error starting deployment of flow main: Unknown connector type bench
//! Error: An error occurred while loading the file `config.troy`: Error deploying Flow main: Unknown connector type bench
//! [2022-04-12T14:38:20Z ERROR tremor::server] Error: An error occurred while loading the file `config.troy`: Error deploying Flow main: Unknown connector type bench
//! We are SHUTTING DOWN due to errors during initialization!
//! [2022-04-12T14:38:20Z ERROR tremor::server] We are SHUTTING DOWN due to errors during initialization!
//! ```
//!
//! In order to run the the `tremor server run` with the bench connector, add the `--debug-connectors` flag.

use crate::{
    connectors::prelude::*,
    system::{KillSwitch, ShutdownMode},
};
use hdrhistogram::Histogram;
use std::{
    cmp::min,
    fmt::Display,
    io::{stdout, BufRead as StdBufRead, BufReader, Read, Write},
    time::Duration,
};
use tremor_common::{base64, file, time::nanotime};
use xz2::read::XzDecoder;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// source file to read data from, it will be iterated over repeatedly,
    /// can be xz compressed
    pub(crate) path: String,

    /// Interval in nanoseconds for coordinated emission testing
    pub interval: Option<u64>,

    /// if this is set, this doesnt split things into lines, but into chunks
    pub chunk_size: Option<usize>,
    /// Number of iterations through the whole source to stop after
    pub iters: Option<usize>,
    #[serde(default = "default_false")]
    pub base64: bool,

    #[serde(default = "default_false")]
    pub is_transactional: bool,

    #[serde(default = "default_false")]
    pub structured: bool,

    /// Number of seconds to collect data before the system is stopped.
    #[serde(default = "Default::default")]
    pub stop_after_secs: u64,
    /// Significant figures for the histogram
    #[serde(default = "default_significant_figures")]
    pub significant_figures: u8,
    /// Number of seconds to warmup, events during this time are not
    /// accounted for in the latency measurements
    #[serde(default = "Default::default")]
    pub warmup_secs: u64,
}

fn default_significant_figures() -> u8 {
    2
}

impl tremor_config::Impl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

fn decode<T: AsRef<[u8]>>(base64: bool, data: T) -> Result<Vec<u8>> {
    if base64 {
        Ok(base64::decode(data)?)
    } else {
        let d: &[u8] = data.as_ref();
        Ok(d.to_vec())
    }
}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    async fn build_cfg(
        &self,
        id: &alias::Connector,
        _: &ConnectorConfig,
        config: &Value,
    ) -> Result<Box<dyn Connector>> {
        let config: Config = Config::new(config)?;
        let mut source_data_file = file::open(&config.path)?;
        let mut data = vec![];
        let ext = file::extension(&config.path);
        if ext == Some("xz") {
            XzDecoder::new(source_data_file).read_to_end(&mut data)?;
        } else {
            source_data_file.read_to_end(&mut data)?;
        };
        let origin_uri = EventOriginUri {
            scheme: "tremor-blaster".to_string(),
            host: hostname(),
            port: None,
            path: vec![config.path.clone()],
        };
        let elements: Vec<Vec<u8>> = if let Some(chunk_size) = config.chunk_size {
            // split into sized chunks
            data.chunks(chunk_size)
                .map(|e| decode(config.base64, e))
                .collect::<Result<_>>()?
        } else {
            // split into lines
            BufReader::new(data.as_slice())
                .lines()
                .map(|e| decode(config.base64, e?))
                .collect::<Result<_>>()?
        };
        let num_elements = elements.len();
        let stop_after_events = config.iters.map(|i| i * num_elements);
        let stop_after_secs = if config.stop_after_secs == 0 {
            None
        } else {
            Some(config.stop_after_secs + config.warmup_secs)
        };
        let stop_after = StopAfter {
            events: stop_after_events,
            seconds: stop_after_secs,
        };
        if stop_after.events.is_none() && stop_after.seconds.is_none() {
            warn!("[Connector::{id}] No stop condition is specified. This connector will emit events infinitely.");
        }

        Ok(Box::new(Bench {
            config,
            acc: Acc { elements, count: 0 },
            origin_uri,
            stop_after,
        }))
    }

    fn connector_type(&self) -> ConnectorType {
        "bench".into()
    }
}

#[derive(Clone, Default)]
struct Acc {
    elements: Vec<Vec<u8>>,
    count: usize,
}
impl Acc {
    fn next(&mut self) -> Vec<u8> {
        // actually safe because we only get element from a slot < elements.len()
        let next = unsafe {
            self.elements
                .get_unchecked(self.count % self.elements.len())
                .clone()
        };
        self.count += 1;
        next
    }
}

#[derive(Clone)]
pub(crate) struct Bench {
    config: Config,
    acc: Acc,
    origin_uri: EventOriginUri,
    stop_after: StopAfter,
}

#[async_trait::async_trait]
impl Connector for Bench {
    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = Blaster {
            acc: self.acc.clone(),
            origin_uri: self.origin_uri.clone(),
            is_transactional: self.config.is_transactional,
            stop_after: self.stop_after,
            stop_at: None,
            interval_ns: self.config.interval.map(Duration::from_nanos),
            finished: false,
        };
        Ok(Some(builder.spawn(source, ctx)))
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = Blackhole::new(&self.config, self.stop_after, ctx.killswitch());
        Ok(Some(builder.spawn(sink, ctx)))
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Optional("json")
    }
}

struct Blaster {
    acc: Acc,
    origin_uri: EventOriginUri,
    is_transactional: bool,
    stop_after: StopAfter,
    stop_at: Option<u64>,
    interval_ns: Option<Duration>,
    finished: bool,
}

#[async_trait::async_trait]
impl Source for Blaster {
    async fn on_start(&mut self, _ctx: &SourceContext) -> anyhow::Result<()> {
        self.stop_at = self
            .stop_after
            .seconds
            .map(|secs| nanotime() + (secs * 1_000_000_000));
        Ok(())
    }

    #[allow(clippy::cast_possible_truncation)]
    async fn pull_data(
        &mut self,
        _pull_id: &mut u64,
        _ctx: &SourceContext,
    ) -> anyhow::Result<SourceReply> {
        if self.finished {
            return Ok(SourceReply::Finished);
        }
        if let Some(interval) = self.interval_ns {
            tokio::time::sleep(interval).await;
        }
        let res = if self.stop_at.iter().any(|stop_at| nanotime() >= *stop_at)
            || self
                .stop_after
                .events
                .iter()
                .any(|stop_after_events| self.acc.count >= *stop_after_events)
        {
            self.finished = true;
            SourceReply::EndStream {
                origin_uri: self.origin_uri.clone(),
                stream: DEFAULT_STREAM_ID,
                meta: None,
            }
        } else {
            let data = self.acc.next();
            SourceReply::Data {
                origin_uri: self.origin_uri.clone(),
                data,
                meta: None,
                stream: Some(DEFAULT_STREAM_ID),
                port: None,
                codec_overwrite: None,
            }
        };
        Ok(res)
    }

    fn is_transactional(&self) -> bool {
        self.is_transactional
    }

    fn asynchronous(&self) -> bool {
        false
    }
}

#[derive(Clone, Copy, Debug)]
struct StopAfter {
    events: Option<usize>,
    seconds: Option<u64>,
}

/// A null offramp that records histograms
struct Blackhole {
    // config: Config,
    stop_after: StopAfter,
    stop_at: Option<u64>,
    warmup: u64,
    delivered: Histogram<u64>,
    run_secs: f64,
    bytes: usize,
    count: usize,
    structured: bool,
    buf: Vec<u8>,
    kill_switch: KillSwitch,
    finished: bool,
}

impl Blackhole {
    #[allow(clippy::cast_precision_loss, clippy::unwrap_used)]
    fn new(c: &Config, stop_after: StopAfter, kill_switch: KillSwitch) -> Self {
        let now_ns = nanotime();
        let sigfig = min(c.significant_figures, 5);
        // ALLOW: this is a debug connector and the values are validated ahead of time
        let delivered = Histogram::new_with_bounds(1, 100_000_000_000, sigfig).unwrap();
        Blackhole {
            // config: config.clone(),
            run_secs: c.stop_after_secs as f64,
            stop_after,
            stop_at: stop_after
                .seconds
                .map(|stop_after_secs| now_ns + (stop_after_secs * 1_000_000_000)),
            warmup: now_ns + (c.warmup_secs * 1_000_000_000),
            structured: c.structured,
            delivered,
            bytes: 0,
            count: 0,
            buf: Vec::with_capacity(1024),
            kill_switch,
            finished: false,
        }
    }
}

#[async_trait::async_trait]
impl Sink for Blackhole {
    fn auto_ack(&self) -> bool {
        true
    }
    async fn on_event(
        &mut self,
        _input: &str,
        event: tremor_pipeline::Event,
        ctx: &SinkContext,
        event_serializer: &mut EventSerializer,
        _start: u64,
    ) -> anyhow::Result<SinkReply> {
        if !self.finished {
            let now_ns = nanotime();

            for (value, meta) in event.value_meta_iter() {
                if now_ns > self.warmup {
                    let delta_ns = now_ns - event.ingest_ns;
                    if let Ok(bufs) = event_serializer
                        .serialize(value, meta, event.ingest_ns)
                        .await
                    {
                        self.bytes += bufs.iter().map(Vec::len).sum::<usize>();
                    } else {
                        error!("{ctx} failed to encode");
                    };
                    self.count += 1;
                    self.buf.clear();
                    self.delivered.record(delta_ns)?;
                }
            }

            if self.stop_at.iter().any(|stop_at| now_ns >= *stop_at)
                || self
                    .stop_after
                    .events
                    .iter()
                    .any(|stop_after_events| self.count >= *stop_after_events)
            {
                self.finish(ctx)?;
            };
        }

        Ok(SinkReply::default())
    }

    async fn on_signal(
        &mut self,
        _signal: Event,
        ctx: &SinkContext,
        _serializer: &mut EventSerializer,
    ) -> anyhow::Result<SinkReply> {
        let now_ns = nanotime();
        if self.stop_at.iter().any(|stop_at| now_ns >= *stop_at) {
            self.finish(ctx)?;
        }
        Ok(SinkReply::default())
    }
}

impl Blackhole {
    fn finish(&mut self, ctx: &SinkContext) -> Result<()> {
        // avoid printing twice
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        if self.structured {
            let v = self.to_value(2);
            v.write(&mut stdout())?;
        } else {
            self.write_text(stdout(), 5, 2)?;
        }
        let kill_switch = self.kill_switch.clone();
        let stop_ctx = ctx.clone();
        info!("{ctx} Bench done");

        // this should stop the whole server process
        // we spawn this out into another task, so we don't block the sink loop handling control plane messages
        tokio::task::spawn(async move {
            info!("{stop_ctx} Exiting...");
            stop_ctx.swallow_err(
                kill_switch.stop(ShutdownMode::Forceful).await,
                "Error stopping the world",
            );
        });
        Ok(())
    }

    #[allow(clippy::cast_precision_loss)] // This is for reporting only we're fine with some precision loss
    fn write_text<W: Write>(
        &self,
        mut writer: W,
        quantile_precision: usize,
        ticks_per_half: u32,
    ) -> Result<()> {
        fn write_extra_data<T1: Display, T2: Display, W: Write>(
            writer: &mut W,
            label1: &str,
            data1: T1,
            label2: &str,
            data2: T2,
        ) -> std::io::Result<()> {
            writer.write_all(
                format!("#[{label1:10} = {data1:12.2}, {label2:14} = {data2:12.2}]\n",).as_ref(),
            )
        }
        writer.write_all(
            format!(
                "{:>10} {:>quantile_precision$} {:>10} {:>14}\n\n",
                "Value",
                "Percentile",
                "TotalCount",
                "1/(1-Percentile)",
                quantile_precision = quantile_precision + 2 // + 2 from leading "0." for numbers
            )
            .as_ref(),
        )?;
        let mut sum = 0;
        for v in self.delivered.iter_quantiles(ticks_per_half) {
            sum += v.count_since_last_iteration();
            if v.quantile_iterated_to() < 1.0 {
                writer.write_all(
                    format!(
                        "{:12} {:1.*} {:10} {:14.2}\n",
                        v.value_iterated_to(),
                        quantile_precision,
                        //                        v.quantile(),
                        //                        quantile_precision,
                        v.quantile_iterated_to(),
                        sum,
                        1_f64 / (1_f64 - v.quantile_iterated_to()),
                    )
                    .as_ref(),
                )?;
            } else {
                writer.write_all(
                    format!(
                        "{:12} {:1.*} {:10} {:>14}\n",
                        v.value_iterated_to(),
                        quantile_precision,
                        //                        v.quantile(),
                        //                        quantile_precision,
                        v.quantile_iterated_to(),
                        sum,
                        "inf"
                    )
                    .as_ref(),
                )?;
            }
        }
        write_extra_data(
            &mut writer,
            "Mean",
            self.delivered.mean(),
            "StdDeviation",
            self.delivered.stdev(),
        )?;
        write_extra_data(
            &mut writer,
            "Max",
            self.delivered.max(),
            "Total count",
            self.delivered.len(),
        )?;
        write_extra_data(
            &mut writer,
            "Buckets",
            self.delivered.buckets(),
            "SubBuckets",
            self.delivered.distinct_values(),
        )?;
        println!(
            "\n\nThroughput   (data): {:.1} MB/s\nThroughput (events): {:.1}k events/s",
            (self.bytes as f64 / self.run_secs) / (1024.0 * 1024.0),
            (self.count as f64 / self.run_secs) / 1000.0
        );

        Ok(())
    }

    #[allow(clippy::cast_precision_loss)] // This is for reporting only we're fine with some precision loss
    fn to_value(&self, ticks_per_half: u32) -> Value<'static> {
        let quantiles: Value = self
            .delivered
            .iter_quantiles(ticks_per_half)
            .map(|v| {
                literal!({
                    "value": v.value_iterated_to(),
                    "quantile": v.quantile(),
                    "quantile_to": v.quantile_iterated_to()

                })
            })
            .collect();
        literal!({
            "mean": self.delivered.mean(),
            "stdev": self.delivered.stdev(),
            "max": self.delivered.max(),
            "count": self.delivered.len(),
            "buckets": self.delivered.buckets(),
            "subbuckets": self.delivered.distinct_values(),
            "bytes": self.bytes,
            "throughput": {
                "events_per_second": (self.count as f64 / self.run_secs),
                "bytes_per_second": (self.bytes as f64 / self.run_secs)
            },
            "quantiles": quantiles
        })
    }
}
