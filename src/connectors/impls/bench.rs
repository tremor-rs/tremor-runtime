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

// #![cfg_attr(coverage, no_coverage)] // This is for benchmarking and testing

use crate::connectors::prelude::*;
use crate::system::{KillSwitch, ShutdownMode};
use hdrhistogram::Histogram;
use std::io::{stdout, Write};
use std::{
    cmp::min,
    fmt::Display,
    io::{BufRead as StdBufRead, BufReader, Read},
    time::Duration,
};
use tremor_common::{file, time::nanotime};
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

impl ConfigImpl for Config {}

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
        id: &Alias,
        _: &ConnectorConfig,
        config: &Value,
        kill_switch: &KillSwitch,
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
            kill_switch: kill_switch.clone(),
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
    kill_switch: KillSwitch,
}

#[async_trait::async_trait]
impl Connector for Bench {
    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let s = Blaster {
            acc: self.acc.clone(),
            origin_uri: self.origin_uri.clone(),
            is_transactional: self.config.is_transactional,
            stop_after: self.stop_after,
            stop_at: None,
            interval_ns: self.config.interval.map(Duration::from_nanos),
            finished: false,
        };
        builder.spawn(s, source_context).map(Some)
    }

    async fn create_sink(
        &mut self,
        sink_context: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        builder
            .spawn(
                Blackhole::new(&self.config, self.stop_after, self.kill_switch.clone()),
                sink_context,
            )
            .map(Some)
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
    async fn on_start(&mut self, _ctx: &SourceContext) -> Result<()> {
        self.stop_at = self
            .stop_after
            .seconds
            .map(|secs| nanotime() + (secs * 1_000_000_000));
        Ok(())
    }

    #[allow(clippy::cast_possible_truncation)]
    async fn pull_data(&mut self, _pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        if self.finished {
            return Ok(SourceReply::Finished);
        }
        if let Some(interval) = self.interval_ns {
            async_std::task::sleep(interval).await;
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
    ) -> Result<SinkReply> {
        if !self.finished {
            let now_ns = nanotime();

            for value in event.value_iter() {
                if now_ns > self.warmup {
                    let delta_ns = now_ns - event.ingest_ns;
                    if let Ok(bufs) = event_serializer.serialize(value, event.ingest_ns) {
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
    ) -> Result<SinkReply> {
        let now_ns = nanotime();
        if self.stop_at.iter().any(|stop_at| now_ns >= *stop_at) {
            self.finish(ctx)?;
        }
        Ok(SinkReply::default())
    }
}

impl Blackhole {
    fn finish(&mut self, ctx: &SinkContext) -> Result<()> {
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
        async_std::task::spawn(async move {
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
                format!(
                    "#[{:10} = {:12.2}, {:14} = {:12.2}]\n",
                    label1, data1, label2, data2
                )
                .as_ref(),
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
