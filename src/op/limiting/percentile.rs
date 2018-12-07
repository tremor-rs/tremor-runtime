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

//! # Percentile based sampling limiter
//!
//! If `adjustment` is set this limiter will move the sampling
//! percentage to keep the returned value from downstream operations
//! between upper and lower limit.
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.
//!
//! ## Outputs
//!
//! The 1st additional output is used to route data that was decided to
//! be discarded.

use crate::dflt;
use crate::errors::*;
use crate::pipeline::prelude::*;
use prometheus::Gauge; // w/ instance
use rand::prelude::*;
use serde_yaml;

lazy_static! {
    static ref PERCENTILE_GAUGE: Gauge =
        prom_gauge!("ts_limiting_percentile", "Current limiting percentile.");
}

/// A Limiter algorithm that just lets trough a percentage of messages
#[derive(Deserialize)]
pub struct Config {
    /// Percentage of events passed through (initial if adjustment is set)
    /// `1.0` means 100% of events will be passed
    pub percentile: f64,
    /// If adjustment is set and the upper limit is exceeded the percentage
    /// of events passed will be lowered (default: infinity)
    #[serde(default = "dflt::d_inf")]
    pub upper_limit: f64,
    /// If adjustment is set and the return is below the lower limit the
    /// percentage of events passed will be raised (default: 0)
    #[serde(default = "dflt::d_0")]
    pub lower_limit: f64,
    /// Optional adjustment to be used to keep returns between the upper
    /// and lower limit
    pub adjustment: Option<f64>,
}

pub struct Limiter {
    config: Config,
    percentile: f64,
}

impl Limiter {
    pub fn create(opts: ConfValue) -> Result<Self> {
        let config: Config = serde_yaml::from_value(opts)?;
        PERCENTILE_GAUGE.set(config.percentile);
        Ok(Self {
            percentile: config.percentile,
            config,
        })
    }
}

fn max(f1: f64, f2: f64) -> f64 {
    if f1 >= f2 {
        f1
    } else {
        f2
    }
}

fn min(f1: f64, f2: f64) -> f64 {
    if f1 <= f2 {
        f1
    } else {
        f2
    }
}

impl Opable for Limiter {
    fn exec(&mut self, event: EventData) -> EventResult {
        let mut rng = thread_rng();
        if rng.gen::<f64>() < self.percentile {
            EventResult::Next(event)
        } else {
            EventResult::NextID(3, event)
        }
    }
    fn result(&mut self, result: EventReturn) -> EventReturn {
        match (&result, self.config.adjustment) {
            (Ok(Some(f)), Some(adjustment)) if f > &self.config.upper_limit => {
                self.percentile = max(adjustment, self.percentile - adjustment);
                PERCENTILE_GAUGE.set(self.percentile);
                debug!("v {} ({})", self.percentile, f);
            }
            (Ok(Some(f)), Some(adjustment)) if f < &self.config.lower_limit => {
                self.percentile = min(1.0, self.percentile + adjustment);
                PERCENTILE_GAUGE.set(self.percentile);
                debug!("^ {} ({})", self.percentile, f);
            }
            _ => (),
        }
        result
    }
    opable_types!(ValueType::Same, ValueType::Same);
}

#[cfg(test)]
mod tests {

    use super::super::Limiter;
    use crate::pipeline::prelude::*;
    use crate::utils::*;
    use serde_yaml::Mapping;
    use std::iter::FromIterator;

    #[test]
    fn keep_all() {
        let conf = ConfValue::Mapping(Mapping::from_iter(
            hashmap! {
                vs("percentile") => vf(1.0),
            }
            .into_iter(),
        ));

        let e = EventData::new(0, 0, None, EventValue::Raw(vec![]));
        let mut c = Limiter::create("percentile", conf).unwrap();

        let r = c.exec(e);

        assert_matches!(r, EventResult::Next(_));
    }

    #[test]
    fn keep_none() {
        let conf = ConfValue::Mapping(Mapping::from_iter(
            hashmap! {
                vs("percentile") => vf(0.0),
            }
            .into_iter(),
        ));

        let e = EventData::new(0, 0, None, EventValue::Raw(vec![]));
        let mut c = Limiter::create("percentile", conf).unwrap();

        let r = c.exec(e);

        assert_matches!(r, EventResult::NextID(3, _));
    }
}
