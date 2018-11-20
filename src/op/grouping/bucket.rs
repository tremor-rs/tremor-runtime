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

//! # Dimension based grouping with sliding windows
//!
//! A This grouper is configred with a number of buckets and their alotted
//! throughput on a time basis.
//!
//! Messages are not combined by key and the alottment is applied in a sliding
//! window fashion with a window granularity (a default of 10ms).
//!
//! There is no 'magical' default bucket but one can be configured if desired
//! along with a default rule.
//!
//! ## Configuration
//!
//! See [Config](struct.Config.html) for details.
//!
//! ## Outputs
//!
//! The 1st additional output is used to route data that was decided to
//! be discarded to.
//!
//! # Example
//!
//! ```yaml
//! - grouper::bucket:
//!     class1: # 1k messages per seconds on second granularity
//!       rate: 1000
//!     class2: # 500 messages per seconds on 10 second granularity
//!       rate: 5000
//!       time_range: 10000
//! ```
//!
//! ## Pseudocode
//!
//! ```pseudocode
//! for e in events {
//!   if let bucket = buckets.get(e.classification) {
//!      dimension = bucket.keys.map(|key| e.data.get(key));
//!      if let window bucket.windows(dimension) {
//!        if window.inc() { pass } else { drop }
//!      } else {
//!        bucket.windows[dimension] = Window::new(bucket.limit)
//!        if bucket.windows[dimension].inc() { pass } else { drop }
//!      }
//!   } else {
//!     return drop
//!   }
//! }
//! ```
use dflt;
use error::TSError;
use errors::*;
use pipeline::prelude::*;
use serde_json;
use serde_yaml;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::iter::Iterator;
use window::TimeWindow;

static DROP_OUTPUT_ID: usize = 3; // 1 is std, 2 is err, 3 is drop

/// Single bucket specification
#[derive(Deserialize, Debug)]
pub struct Rate {
    /// the maximum number of events per time range
    pub rate: u64,
    /// time range in milliseconds, (default: 1000 - 1 second)
    #[serde(default = "dflt::d_1000")]
    pub time_range: u64,
    /// numbers of window in the time_range (default: 100)
    #[serde(default = "dflt::d_100")]
    pub windows: usize,
}

#[derive(Deserialize, Debug)]
pub struct Config {
    /// the buckets to limit against, based on the `classification`
    /// metadata variable
    pub buckets: HashMap<String, Rate>,
}

/// Window based grouping
#[derive(Debug)]
pub struct Grouper {
    buckets: HashMap<String, Bucket>,
}

#[derive(Debug)]
struct Bucket {
    class: String,
    config: Rate,
    groups: HashMap<String, TimeWindow>,
}

impl Grouper {
    /// The grouper is configured with the following syntax:
    ///
    /// * rule: `<name>:<throughput per second>`
    /// * config: `<time range in ms>;<number of windows>;<rule>|<rule>|...`
    ///
    /// So the config `important:10000|unimportant:100|default:10`
    /// would create 3 buckets:
    /// * `important` that gets 10k msgs/s
    /// * `unimportant` that gets 100 msgs/s
    /// * `default` thet gets 10 msgs/s    pub fn new(opts: &str) -> Self {
    pub fn new(opts: &ConfValue) -> Result<Self> {
        let config: Config = serde_yaml::from_value(opts.clone())?;
        let mut buckets = HashMap::new();
        for (name, spec) in config.buckets {
            let bkt = Bucket {
                class: name.clone(),
                config: spec,
                groups: HashMap::new(),
            };
            buckets.insert(name, bkt);
        }
        Ok(Grouper { buckets })
    }
}

impl Opable for Grouper {
    fn exec(&mut self, event: EventData) -> EventResult {
        if let Some(MetaValue::String(class)) = event.var_clone(&"classification") {
            match self.buckets.get_mut(&class) {
                Some(Bucket {
                    class: _class,
                    config,
                    groups,
                }) => {
                    let dimensions = match event.var_clone(&"dimensions") {
                        Some(MetaValue::String(dimension)) => vec![dimension],
                        Some(MetaValue::VecS(dimensions)) => dimensions,
                        _ => vec![],
                    };
                    let dimensions: Vec<serde_json::Value> = dimensions
                        .iter()
                        .map(|d| serde_json::Value::String(d.clone()))
                        .collect();
                    let dimensions = serde_json::to_string(&serde_json::Value::Array(dimensions));
                    let dimensions = match dimensions {
                        Ok(d) => d,
                        Err(e) => {
                            return EventResult::Error(
                                event,
                                Some(TSError::new(&format!(
                                    "Failed to create dimensions: {}.",
                                    e
                                ))),
                            )
                        }
                    };
                    let window = match groups.entry(dimensions) {
                        Entry::Occupied(o) => o.into_mut(),
                        Entry::Vacant(v) => v.insert(TimeWindow::new(
                            config.windows,
                            config.time_range / (config.windows as u64),
                            config.rate,
                        )),
                    };
                    if window.inc_t(event.ingest_ns).is_ok() {
                        EventResult::Next(event)
                    } else {
                        EventResult::NextID(DROP_OUTPUT_ID, event)
                    }
                }
                None => EventResult::NextID(DROP_OUTPUT_ID, event),
            }
        } else {
            EventResult::NextID(DROP_OUTPUT_ID, event)
        }
    }
    opable_types!(ValueType::Same, ValueType::Same);
}

#[cfg(test)]
mod tests {
    use op::grouping::Grouper;
    use pipeline::prelude::*;
    use serde_yaml::{Mapping, Value};
    use std::collections::HashMap;
    use std::iter::FromIterator;
    use utils::*;

    fn event(vars: HashMap<String, MetaValue>, now: u64) -> EventData {
        EventData::new_with_vars(0, now, None, EventValue::Raw(vec![]), vars)
    }

    fn conf() -> Value {
        let buckets = Value::Mapping(Mapping::from_iter(
            hashmap!{
            vs("a") => Value::Mapping(Mapping::from_iter(
                hashmap!{
                    vs("rate") => vi(1)
                }.into_iter()))}.into_iter(),
        ));
        let conf = hashmap!{
            vs("buckets") => buckets,
        };
        Value::Mapping(Mapping::from_iter(conf.into_iter()))
    }
    #[test]
    fn grouping_test_pass() {
        let e = event(hashmap!{"classification".to_string() => "a".into()}, 0);

        let mut g = Grouper::new("bucket", &conf()).unwrap();
        let r = g.exec(e);
        assert_matches!(r, EventResult::Next(_));
    }

    #[test]
    fn grouping_test_fail() {
        let e = event(hashmap!{"classification".to_string() => "b".into()}, 0);

        let mut g = Grouper::new("bucket", &conf()).unwrap();
        let r = g.exec(e);
        assert_matches!(r, EventResult::NextID(3, _));
    }
    #[test]
    fn grouping_test_timed() {
        let e = event(hashmap!{"classification".to_string() => "a".into()}, 0);
        // the second message arrives 100ms later
        let e1 = event(
            hashmap!{"classification".to_string() => "a".into()},
            ms!(100),
        );
        let e2 = event(
            hashmap!{"classification".to_string() => "a".into()},
            s!(1) + ms!(200),
        );

        let mut g = Grouper::new("bucket", &conf()).unwrap();
        let r = g.exec(e);
        assert_matches!(r, EventResult::Next(_));

        let r = g.exec(e1);
        assert_matches!(r, EventResult::NextID(3, _));

        let r = g.exec(e2);
        assert_matches!(r, EventResult::Next(_));
    }

}
