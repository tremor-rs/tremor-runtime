// Copyright 2018, Wayfair GmbH
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

//! # Blackhole benchmarking offramp
//!
//! Offramp used for benchmarking to generate latency histograms
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.

use errors::*;
use hdrhistogram::serialization::Serializer;
use hdrhistogram::serialization::V2Serializer;
use hdrhistogram::Histogram;
use pipeline::prelude::*;
use serde_yaml;
use std::io::stdout;
use std::process;
use std::str;
use utils;

/// A null offramp that records histograms
#[derive(Debug, Clone)]
pub struct Offramp {
    config: Config,
    stop_after: u64,
    warmup: u64,
    has_stop_limit: bool,
    delivered: Histogram<u64>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// Number of seconds to collect data before the system is stopped.
    pub stop_after_secs: u64,
    /// Significant figures for the histogram
    pub significant_figures: u64,
    /// Number of seconds to warmup, events during this time are not
    /// accounted for in the latency measurements
    pub warmup_secs: u64,
}

impl Offramp {
    pub fn new(opts: &ConfValue) -> Result<Self> {
        let config: Config = serde_yaml::from_value(opts.clone())?;
        let now_ns = utils::nanotime();
        Ok(Offramp {
            config: config.clone(),
            stop_after: now_ns + (config.stop_after_secs + config.warmup_secs) * 1_000_000_000,
            warmup: now_ns + config.warmup_secs * 1_000_000_000,
            has_stop_limit: config.stop_after_secs != 0,
            delivered: Histogram::new_with_bounds(
                1,
                1_000_000_000,
                config.significant_figures as u8,
            ).unwrap(),
        })
    }
}

impl Opable for Offramp {
    // TODO
    fn exec(&mut self, event: EventData) -> EventResult {
        let now_ns = utils::nanotime();
        let delta_ns = now_ns - event.ingest_ns;

        if self.has_stop_limit && now_ns > self.stop_after {
            self.shutdown();
            process::exit(0);
        }

        if now_ns > self.warmup {
            self.delivered
                .record(delta_ns)
                .expect("HDR Histogram error");
        }
        EventResult::Return(event.make_return(Ok(None)))
    }
    fn shutdown(&mut self) {
        let mut buf = Vec::new();
        let mut serializer = V2Serializer::new();

        serializer.serialize(&self.delivered, &mut buf).unwrap();
        utils::quantiles(buf.as_slice(), stdout(), 5, 2).expect("Failed to serialize histogram");
    }

    opable_types!(ValueType::Raw, ValueType::Raw);
}

/*
*/
