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
use crate::system::{ShutdownMode, World};
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

use crate::errors::Error;
use abi_stable::{
    prefix_type::PrefixTypeTrait,
    rstr, rvec, sabi_extern_fn,
    std_types::{
        ROption::{self, RNone, RSome},
        RResult::{RErr, ROk},
        RStr, RString, RVec,
    },
    type_level::downcasting::TD_Opaque,
};
use async_ffi::{BorrowingFfiFuture, FfiFuture, FutureExt};
use std::future;
use tremor_common::{pdk::RError, ttry};

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

/// Note that since it's a built-in plugin, `#[export_root_module]` can't be
/// used or it would conflict with other plugins.
pub fn instantiate_root_module() -> ConnectorPluginRef {
    ConnectorPlugin {
        connector_type,
        from_config,
    }
    .leak_into_prefix()
}

#[sabi_extern_fn]
fn connector_type() -> ConnectorType {
    "bench".into()
}

#[sabi_extern_fn]
pub fn from_config<'a>(
    id: RStr<'a>,
    _: &ConnectorConfig,
    config: &'a Value,
    world: ROption<World>,
) -> BorrowingFfiFuture<'a, RResult<BoxedRawConnector>> {
    async move {
            let config: Config = ttry!(Config::new(config));
            let mut source_data_file = ttry!(file::open(&config.source));
            let mut data = vec![];
            let ext = file::extension(&config.source);
            if ext == Some("xz") {
                ttry!(XzDecoder::new(source_data_file)
                    .read_to_end(&mut data)
                    .map_err(RError::new));
            } else {
                ttry!(source_data_file.read_to_end(&mut data).map_err(RError::new));
            };
            let origin_uri = EventOriginUri {
                scheme: RString::from("tremor-blaster"),
                host: RString::from(hostname()),
                port: RNone,
                path: rvec![RString::from(config.source.clone())],
            };
            let elements: Vec<Vec<u8>> = if let Some(chunk_size) = config.chunk_size {
                // split into sized chunks
                ttry!(data
                    .chunks(chunk_size)
                    .map(|e| -> Result<Vec<u8>> {
                        if config.base64 {
                            Ok(Vec::from(base64::decode(e)?))
                        } else {
                            Ok(Vec::from(e))
                        }
                    })
                    .collect::<Result<_>>())
            } else {
                // split into lines
                ttry!(BufReader::new(data.as_slice())
                    .lines()
                    .map(|e| -> Result<Vec<u8>> {
                        if config.base64 {
                            Ok(Vec::from(base64::decode(&e?.as_bytes())?))
                        } else {
                            Ok(Vec::from(e?.as_bytes()))
                        }
                    })
                    .collect::<Result<_>>())
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

            ROk(BoxedRawConnector::from_value(Bench {
                config,
                acc: Acc { elements, count: 0 },
                origin_uri,
                stop_after,
                world: world.unwrap().clone(),
            }, TD_Opaque))
    }
    .into_ffi()
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
pub struct Bench {
    config: Config,
    acc: Acc,
    origin_uri: EventOriginUri,
    stop_after: StopAfter,
    world: World,
}

impl RawConnector for Bench {
    fn create_source(
        &mut self,
        _source_context: SourceContext,
        _qsize: usize,
    ) -> BorrowingFfiFuture<'_, RResult<ROption<BoxedRawSource>>> {
        let s = Blaster {
            acc: self.acc.clone(),
            origin_uri: self.origin_uri.clone(),
            is_transactional: self.config.is_transactional,
            stop_after: self.stop_after,
            stop_at: None,
            interval_ns: self.config.interval.map(Duration::from_nanos),
            finished: false,
        };
        // We don't need to be able to downcast the connector back to the original
        // type, so we just pass it as an opaque type.
        let s = BoxedRawSource::from_value(s, TD_Opaque);
        future::ready(ROk(RSome(s))).into_ffi()
    }

    fn create_sink(
        &mut self,
        _sink_context: SinkContext,
        _qsize: usize,
        _reply_tx: BoxedContraflowSender,
    ) -> BorrowingFfiFuture<'_, RResult<ROption<BoxedRawSink>>> {
        let s = Blackhole::new(&self.config, self.stop_after, self.world.clone());
        // We don't need to be able to downcast the connector back to the original
        // type, so we just pass it as an opaque type.
        let s = BoxedRawSink::from_value(s, TD_Opaque);
        future::ready(ROk(RSome(s))).into_ffi()
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Optional(rstr!("json"))
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

impl RawSource for Blaster {
    fn on_start<'a>(&'a mut self, _ctx: &'a SourceContext) -> BorrowingFfiFuture<'a, RResult<()>> {
        self.stop_at = self
            .stop_after
            .seconds
            .map(|secs| nanotime() + (secs * 1_000_000_000));
        future::ready(ROk(())).into_ffi()
    }

    fn pull_data<'a>(
        &'a mut self,
        _pull_id: &'a mut u64,
        _ctx: &'a SourceContext,
    ) -> BorrowingFfiFuture<'a, RResult<SourceReply>> {
        async move {
            if self.finished {
                return ROk(SourceReply::Finished);
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
                    meta: RNone,
                }
            } else {
                let data = self.acc.next();
                SourceReply::Data {
                    origin_uri: self.origin_uri.clone(),
                    // TODO: this conversion can be avoided
                    data: data.into(),
                    meta: RNone,
                    stream: RSome(DEFAULT_STREAM_ID),
                    port: RNone,
                    codec_overwrite: RNone,
                }
            };
            ROk(res)
        }
        .into_ffi()
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
    world: World,
    finished: bool,
}

impl Blackhole {
    #[allow(clippy::cast_precision_loss, clippy::unwrap_used)]
    fn new(c: &Config, stop_after: StopAfter, world: World) -> Self {
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
            world,
            finished: false,
        }
    }
}

impl RawSink for Blackhole {
    fn auto_ack(&self) -> bool {
        true
    }
    fn on_event<'a>(
        &'a mut self,
        _input: RStr<'a>,
        event: Event,
        ctx: &'a SinkContext,
        event_serializer: &'a mut MutEventSerializer,
        _start: u64,
    ) -> BorrowingFfiFuture<'a, RResult<SinkReply>> {
        async move {
            if !self.finished {
                let now_ns = nanotime();

                for value in event.value_iter() {
                    if now_ns > self.warmup {
                        let delta_ns = now_ns - event.ingest_ns;
                        if let ROk(bufs) = event_serializer.serialize(value, event.ingest_ns) {
                            self.bytes += bufs.iter().map(RVec::len).sum::<usize>();
                        } else {
                            error!("failed to encode");
                        };
                        self.count += 1;
                        self.buf.clear();
                        ttry!(self.delivered.record(delta_ns).map_err(Error::from));
                    }
                }

                if self.stop_at.iter().any(|stop_at| now_ns >= *stop_at)
                    || self
                        .stop_after
                        .events
                        .iter()
                        .any(|stop_after_events| self.count >= *stop_after_events)
                {
                    self.finished = true;
                    if self.structured {
                        let v = self.to_value(2);
                        ttry!(v.write(&mut stdout()).map_err(Error::from));
                    } else {
                        ttry!(self.write_text(stdout(), 5, 2));
                    }
                    let world = self.world.clone();
                    let stop_ctx = ctx.clone();
                    info!("Bench done");

                    // this should stop the whole server process
                    // we spawn this out into another task, so we don't block the sink loop handling control plane messages
                    async_std::task::spawn(async move {
                        info!("{stop_ctx} Exiting...");
                        stop_ctx.swallow_err(
                            world.stop(ShutdownMode::Forceful).await,
                            "Error stopping the world",
                        );
                    });
                };
            }

            ROk(SinkReply::default())
        }
        .into_ffi()
    }
}

impl Blackhole {
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
