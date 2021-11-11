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

use crate::connectors::prelude::*;
use hdrhistogram::Histogram;
use std::{
    fmt::Display,
    io::{BufRead as StdBufRead, BufReader, Read},
    time::Duration,
};
use std::{
    io::{stdout, Write},
    process,
};
use tremor_common::{file, time::nanotime};
use xz2::read::XzDecoder;

// FIXME: enable structured writing
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// source file to read data from, it will be iterated over repeatedly,
    /// can be xz compressed
    pub source: String,
    /// Interval in nanoseconds for coordinated emission testing
    pub interval: Option<u64>,

    /// if this is set, this doesnt split things into lines, but into chunks
    pub chunk_size: Option<usize>,
    /// Number of iterations through the whole source to stop after
    pub iters: Option<usize>,
    #[serde(default = "Default::default")]
    pub base64: bool,

    #[serde(default = "Default::default")]
    pub is_transactional: bool,

    #[serde(default = "Default::default")]
    pub structured: bool,

    /// Number of seconds to collect data before the system is stopped.
    pub stop_after_secs: u64,
    /// Significant figures for the histogram
    pub significant_figures: u64,
    /// Number of seconds to warmup, events during this time are not
    /// accounted for in the latency measurements
    pub warmup_secs: u64,
}

impl ConfigImpl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    async fn from_config(
        &self,
        id: &TremorUrl,
        config: &Option<OpConfig>,
    ) -> Result<Box<dyn Connector>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let mut source_data_file = file::open(&config.source)?;
            let mut data = vec![];
            let ext = file::extension(&config.source);
            if ext == Some("xz") {
                XzDecoder::new(source_data_file).read_to_end(&mut data)?;
            } else {
                source_data_file.read_to_end(&mut data)?;
            };
            let origin_uri = EventOriginUri {
                scheme: "tremor-blaster".to_string(),
                host: hostname(),
                port: None,
                path: vec![config.source.clone()],
            };
            let elements: Vec<Vec<u8>> = if let Some(chunk_size) = config.chunk_size {
                // split into sized chunks
                data.chunks(chunk_size)
                    .map(|e| -> Result<Vec<u8>> {
                        if config.base64 {
                            Ok(base64::decode(e)?)
                        } else {
                            Ok(e.to_vec())
                        }
                    })
                    .collect::<Result<_>>()?
            } else {
                // split into lines
                BufReader::new(data.as_slice())
                    .lines()
                    .map(|e| -> Result<Vec<u8>> {
                        if config.base64 {
                            Ok(base64::decode(&e?.as_bytes())?)
                        } else {
                            Ok(e?.as_bytes().to_vec())
                        }
                    })
                    .collect::<Result<_>>()?
            };
            Ok(Box::new(Bench {
                config,
                data,
                acc: Acc {
                    elements,
                    count: 0,
                    iterations: 0,
                },
                origin_uri,
                onramp_id: id.clone(),
            }))
        } else {
            Err("Missing config for blaster onramp".into())
        }
    }
}

#[derive(Clone, Default)]
struct Acc {
    elements: Vec<Vec<u8>>,
    count: usize,
    iterations: usize,
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
        self.iterations = self.count / self.elements.len();
        next
    }
}

#[derive(Clone)]
pub struct Bench {
    pub config: Config,
    onramp_id: TremorUrl,
    data: Vec<u8>,
    acc: Acc,
    origin_uri: EventOriginUri,
}

#[async_trait::async_trait]
impl Connector for Bench {
    async fn connect(&mut self, _ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        Ok(true)
    }

    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let s = Blaster {
            acc: self.acc.clone(),
            origin_uri: self.origin_uri.clone(),
            is_transactional: self.config.is_transactional,
            iterations: self.config.iters,
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
            .spawn(Blackhole::from_config(&self.config), sink_context)
            .map(Some)
    }

    fn default_codec(&self) -> &str {
        "null"
    }
}

struct Blaster {
    acc: Acc,
    origin_uri: EventOriginUri,
    is_transactional: bool,
    iterations: Option<usize>,
    interval_ns: Option<Duration>,
    finished: bool,
}

#[async_trait::async_trait]
impl Source for Blaster {
    async fn pull_data(&mut self, _pull_id: u64, _ctx: &SourceContext) -> Result<SourceReply> {
        // TODO better sleep perhaps
        if let Some(interval) = self.interval_ns {
            async_std::task::sleep(interval).await;
        }
        if Some(self.acc.iterations) == self.iterations {
            self.finished = true;
            return Ok(SourceReply::EndStream(DEFAULT_STREAM_ID));
        };
        if self.finished {
            Ok(SourceReply::Empty(100))
        } else {
            Ok(SourceReply::Data {
                origin_uri: self.origin_uri.clone(),
                data: self.acc.next(),
                meta: None,
                stream: DEFAULT_STREAM_ID,
                port: None,
            })
        }
    }

    fn is_transactional(&self) -> bool {
        self.is_transactional
    }
}

/// A null offramp that records histograms
struct Blackhole {
    // config: Config,
    stop_after: u64,
    warmup: u64,
    has_stop_limit: bool,
    delivered: Histogram<u64>,
    run_secs: f64,
    bytes: usize,
    count: u64,
    structured: bool,
    buf: Vec<u8>,
}

impl Blackhole {
    fn from_config(config: &Config) -> Self {
        let now_ns = nanotime();

        Blackhole {
            // config: config.clone(),
            run_secs: config.stop_after_secs as f64,
            stop_after: now_ns + ((config.stop_after_secs + config.warmup_secs) * 1_000_000_000),
            warmup: now_ns + (config.warmup_secs * 1_000_000_000),
            has_stop_limit: config.stop_after_secs != 0,
            structured: config.structured,
            delivered: Histogram::new_with_bounds(
                1,
                100_000_000_000,
                config.significant_figures as u8,
            )
            .unwrap(),
            bytes: 0,
            count: 0,
            buf: Vec::with_capacity(1024),
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
        _ctx: &SinkContext,
        event_serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        let now_ns = nanotime();
        if self.has_stop_limit && now_ns > self.stop_after {
            if self.structured {
                let v = self.to_value(2)?;
                v.write(&mut stdout())?;
            } else {
                self.write_text(stdout(), 5, 2)?;
            }
            // ALLOW: This is on purpose, we use blackhole for benchmarking, so we want it to terminate the process when done
            process::exit(0);
        };
        for value in event.value_iter() {
            if now_ns > self.warmup {
                let delta_ns = now_ns - event.ingest_ns;
                // FIXME: use the buffer
                if let Ok(bufs) = event_serializer.serialize(value, event.ingest_ns) {
                    self.bytes += bufs.iter().map(Vec::len).sum::<usize>();
                } else {
                    error!("failed to encode");
                };
                self.count += 1;
                self.buf.clear();
                self.delivered.record(delta_ns)?;
            }
        }
        Ok(SinkReply::default())
    }
}

impl Blackhole {
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

    fn to_value(&self, ticks_per_half: u32) -> Result<Value<'static>> {
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
        Ok(literal!({
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

        }))
    }
}
