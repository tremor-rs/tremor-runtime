use error::TSError;

use hdrhistogram::serialization::Serializer;
use hdrhistogram::serialization::V2Serializer;
use hdrhistogram::Histogram;
use output;
use output::{OUTPUT_DELIVERED, OUTPUT_SKIPPED};
use pipeline::{Event, Step};
use serde_json;
use std::io::stdout;
use std::process;
use std::str;
use utils;

/// A null output that records histograms
pub struct Output {
    config: Config,
    delivered: Histogram<u64>,
    dropped: Histogram<u64>,
}

#[derive(Deserialize, Debug, Clone)]
struct Config {
    stop_after_secs: u64,
    significant_figures: u64,
}

impl Output {
    pub fn new(opts: &str) -> Self {
        match serde_json::from_str(opts) {
            Ok(config @ Config { .. }) => Output {
                config: config.clone(),
                delivered: Histogram::new_with_bounds(1, 1000000000, config.significant_figures as u8)
                    .unwrap(),
                dropped: Histogram::new_with_bounds(1, 1000000000, config.significant_figures as u8).unwrap(),
            },
            e => panic!(
                "Invalid options for Blackhole output, use `{{\"stop_after_secs\": <secs>, \"significant_figures\": <digits>}}`\n{:?} ({})",
                e, opts
            ),
        }
    }
}

impl Step for Output {
    fn apply(&mut self, event: Event) -> Result<Event, TSError> {
        let step = output::step(&event);
        let now_ns = utils::nanotime();
        let delta_epoch_ns = now_ns - event.app_epoch_ns;
        let delta_event_ns = now_ns - event.ingest_time_ns;
        let should_warmup = event.bench_warmup;
        let has_stop_limit = self.config.stop_after_secs != 0;
        let stop_after_ns = self.config.stop_after_secs * 1_000_000_000;

        if has_stop_limit && delta_epoch_ns > stop_after_ns {
            self.shutdown();
            process::exit(0);
        }

        if event.drop {
            if !should_warmup {
                self.dropped
                    .record(delta_event_ns)
                    .expect("HDR Histogram error");
            }
            OUTPUT_SKIPPED.with_label_values(&[step, "blackhole"]).inc();
            Ok(event)
        } else {
            if !should_warmup {
                self.delivered
                    .record(delta_event_ns)
                    .expect("HDR Histogram error");
            }
            OUTPUT_DELIVERED
                .with_label_values(&[step, "blackhole"])
                .inc();
            Ok(event)
        }
    }

    fn shutdown(&mut self) {
        let mut buf = Vec::new();
        let mut serializer = V2Serializer::new();

        serializer.serialize(&self.delivered, &mut buf).unwrap();
        utils::quantiles(buf.as_slice(), stdout(), 5, 2).expect("Failed to serialize histogram");

        buf.clear();
        serializer.serialize(&self.dropped, &mut buf).unwrap();
        utils::quantiles(buf.as_slice(), stdout(), 2, 2).expect("Failed to serialize histogram");
    }
}
