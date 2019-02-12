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
//! grouper is configured with a number of buckets and their alloted
//! throughput on a time basis.
//!
//! Messages are not combined by key and the allotment is applied in a sliding
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
use crate::dflt;
use crate::errors::*;
use crate::pipeline::prelude::*;
use lru::LruCache;
use serde_json;
use serde_yaml;
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
    /// the cardinality of the dimension of the bucket to support (default: 1000)
    #[serde(default = "dflt::d_1000")]
    pub cardinality: u64,
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

// #[derive(Debug)]
struct Bucket {
    class: String,
    config: Rate,
    groups: LruCache<String, TimeWindow>,
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
    /// * `default` thet gets 10 msgs/s    pub fn create(opts: &str) -> Self {
    pub fn create(opts: &ConfValue) -> Result<Self> {
        let config: Config = serde_yaml::from_value(opts.clone())?;
        let mut buckets = HashMap::new();
        for (name, spec) in config.buckets {
            let Rate {
                cardinality: c,
                rate: _r,
                time_range: _t,
                windows: _w,
            } = spec;
            if c < 1 {
                return Err("Can't have a cardinality lower then 1.".into());
            }
            let bkt = Bucket {
                class: name.clone(),
                config: spec,
                groups: LruCache::new(c as usize),
            };
            buckets.insert(name, bkt);
        }
        Ok(Grouper { buckets })
    }
}

impl std::fmt::Debug for Bucket {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Bucket: {} with config: {:?} - the lru_cache cannot be printed",
            self.class, self.config
        )
    }
}

impl Opable for Grouper {
    fn on_event(&mut self, event: EventData) -> EventResult {
        if let Some(serde_json::Value::String(class)) = event.var_clone(&"classification") {
            match self.buckets.get_mut(&class) {
                Some(Bucket {
                    class: _class,
                    config,
                    groups,
                }) => {
                    let dimensions = match event.var_clone(&"dimensions") {
                        Some(serde_json::Value::String(dimension)) => vec![dimension],
                        Some(serde_json::Value::Array(dimensions)) => dimensions
                            .iter()
                            .filter_map(|e| match e {
                                serde_json::Value::String(s) => Some(s.clone()),
                                _ => None,
                            })
                            .collect(),
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
                            let e = format!("Failed to create dimensions: {}.", e);
                            return error_result!(event, e);
                        }
                    };
                    let window = match groups.get_mut(&dimensions) {
                        None => {
                            groups.put(
                                dimensions.clone(),
                                TimeWindow::new(
                                    config.windows,
                                    config.time_range / (config.windows as u64),
                                    config.rate,
                                ),
                            );
                            groups.get_mut(&dimensions).unwrap()
                        }
                        Some(m) => m,
                    };
                    if window.inc_t(event.ingest_ns).is_ok() {
                        next!(event)
                    } else {
                        next_id!(DROP_OUTPUT_ID, event)
                    }
                }
                None => next_id!(DROP_OUTPUT_ID, event),
            }
        } else {
            next_id!(DROP_OUTPUT_ID, event)
        }
    }
    opable_types!(ValueType::Same, ValueType::Same);
}

#[cfg(test)]
mod tests {
    use crate::op::grouping::Grouper;
    use crate::pipeline::prelude::*;
    use crate::utils::*;
    use serde_yaml::{Mapping, Value};
    use std::collections::HashMap;
    use std::iter::FromIterator;

    fn event(vars: HashMap<String, MetaValue>, now: u64) -> EventData {
        EventData::new_with_vars(0, now, None, EventValue::Raw(vec![]), vars)
    }

    fn conf(card: u64) -> Value {
        let buckets = Value::Mapping(Mapping::from_iter(
            hashmap! {
            vs("a") => Value::Mapping(Mapping::from_iter(
                hashmap!{
                    vs("rate") => vi(1),
                    vs("cardinality") => vi(card)
                }.into_iter()))}
            .into_iter(),
        ));
        let conf = hashmap! {
            vs("buckets") => buckets,
        };
        Value::Mapping(Mapping::from_iter(conf.into_iter()))
    }
    #[test]
    fn grouping_test_pass() {
        let e = event(hashmap! {"classification".to_string() => "a".into()}, 0);

        let mut g = Grouper::create("bucket", &conf(1000)).unwrap();
        let r = g.on_event(e);
        assert_matches!(r, EventResult::NextID(1, _));
    }

    #[test]
    fn grouping_test_fail() {
        let e = event(hashmap! {"classification".to_string() => "b".into()}, 0);

        let mut g = Grouper::create("bucket", &conf(1000)).unwrap();
        let r = g.on_event(e);
        assert_matches!(r, EventResult::NextID(3, _));
    }
    #[test]
    fn grouping_test_timed() {
        let e = event(hashmap! {"classification".to_string() => "a".into()}, 0);
        // the second message arrives 100ms later
        let e1 = event(
            hashmap! {"classification".to_string() => "a".into()},
            ms!(100),
        );
        let e2 = event(
            hashmap! {"classification".to_string() => "a".into()},
            s!(1) + ms!(200),
        );

        let mut g = Grouper::create("bucket", &conf(1000)).unwrap();
        let r = g.on_event(e);
        assert_matches!(r, EventResult::NextID(1, _));

        let r = g.on_event(e1);
        assert_matches!(r, EventResult::NextID(3, _));

        let r = g.on_event(e2);
        assert_matches!(r, EventResult::NextID(1, _));
    }

    #[test]
    fn lru_invalid() {
        assert!(Grouper::create("bucket", &conf(0)).is_err());
    }

    #[test]
    fn lru() {
        let ha =
            hashmap! {"classification".into() => "a".into(), "dimensions".into() => "a".into()};
        let hb =
            hashmap! {"classification".into() => "a".into(), "dimensions".into() => "b".into()};
        let e = event(ha.clone(), 0);
        let eb = event(hb.clone(), 0);
        // the second message arrives 100ms later
        let e1 = event(ha.clone(), ms!(100));
        let e2 = event(ha.clone(), s!(1) + ms!(200));

        let mut g = Grouper::create("bucket", &conf(1)).unwrap();
        let r = g.on_event(e);
        assert_matches!(r, EventResult::NextID(1, _));

        // hb should evict a from the LRU cache
        let r = g.on_event(eb);
        assert_matches!(r, EventResult::NextID(1, _));

        //That way we are allowe to ingest a again
        let r = g.on_event(e1);
        assert_matches!(r, EventResult::NextID(1, _));

        let r = g.on_event(e2);
        assert_matches!(r, EventResult::NextID(1, _));
    }

}
