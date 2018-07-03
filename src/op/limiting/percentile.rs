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

use dflt;
use errors::*;
use pipeline::prelude::*;
use prometheus::Gauge; // w/ instance
use rand::prelude::*;
use serde_yaml;

lazy_static! {
    static ref PERCENTILE_GAUGE: Gauge =
        prom_gauge!("ts_limiting_percentile", "Current limiting percentile.");
}

/// A Limitier algorith that just lets trough a percentage of messages
#[derive(Deserialize)]
pub struct Limiter {
    percentile: f64,
    #[serde(default = "dflt::d_0")]
    upper_limit: f64,
    #[serde(default = "dflt::d_inf")]
    lower_limit: f64,
    #[serde(default = "dflt::d_0")]
    adjustment: f64,
}

impl Limiter {
    pub fn new(opts: ConfValue) -> Result<Self> {
        let l: Limiter = serde_yaml::from_value(opts)?;
        PERCENTILE_GAUGE.set(l.percentile);
        Ok(l)
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
        match result {
            Ok(Some(f)) if f > self.upper_limit => {
                self.percentile = max(self.adjustment, self.percentile - self.adjustment);
                PERCENTILE_GAUGE.set(self.percentile);
                debug!("v {} ({})", self.percentile, f);
            }
            Ok(Some(f)) if f < self.lower_limit => {
                self.percentile = min(1.0, self.percentile + self.adjustment);
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
    use pipeline::prelude::*;
    use serde_yaml::Mapping;
    use std::iter::FromIterator;
    use utils::*;

    #[test]
    fn keep_all() {
        let conf = ConfValue::Mapping(Mapping::from_iter(
            hashmap!{
                vs("percentile") => vf(1.0),
            }.into_iter(),
        ));

        let e = EventData::new(0, 0, None, EventValue::Raw(vec![]));
        let mut c = Limiter::new("percentile", conf).unwrap();

        let r = c.exec(e);

        assert_matches!(r, EventResult::Next(_));
    }

    #[test]
    fn keep_none() {
        let conf = ConfValue::Mapping(Mapping::from_iter(
            hashmap!{
                vs("percentile") => vf(0.0),
            }.into_iter(),
        ));

        let e = EventData::new(0, 0, None, EventValue::Raw(vec![]));
        let mut c = Limiter::new("percentile", conf).unwrap();

        let r = c.exec(e);

        assert_matches!(r, EventResult::NextID(3, _));
    }
}
