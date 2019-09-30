// Copyright 2018-2019, Wayfair GmbH
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

use crate::errors::*;
use crate::{Event, Operator};
use halfbrown::HashMap;
use lru::LruCache;
use simd_json::{json, BorrowedValue as Value, ValueTrait};
use std::borrow::{Borrow, Cow};
use window::TimeWindow;

op!(BucketGrouperFactory(node) {
    if node.config.is_none() {
        Ok(Box::new(BucketGrouper {
            buckets: HashMap::new(),
            _id: node.id.clone(),
        }))
    } else {
        Err(ErrorKind::ExtraOpConfig(node.id.clone()).into())
    }
});
/// Single bucket specification
#[derive(Debug)]
pub struct Rate {
    /// the maximum number of events per time range
    pub rate: u64,
    /// time range in milliseconds, (default: 1000 - 1 second)
    pub time_range: u64,
    /// numbers of window in the time_range (default: 100)
    pub windows: usize,
}

impl Rate {
    pub fn from_meta<'any>(meta: &Value<'any>) -> Option<Self> {
        let rate = meta.get("rate")?.as_u64()?;

        let time_range = meta
            .get("time_range")
            .and_then(Value::as_u64)
            .unwrap_or(1000);
        let windows = meta.get("windows").and_then(Value::as_u64).unwrap_or(100) as usize;
        Some(Rate {
            rate,
            time_range,
            windows,
        })
    }
}

pub struct Bucket {
    cache: LruCache<String, TimeWindow>,
    pass: u64,
    overflow: u64,
}
impl Bucket {
    fn new(cardinality: usize) -> Self {
        Self {
            cache: LruCache::new(cardinality),
            pass: 0,
            overflow: 0,
        }
    }
}

pub struct BucketGrouper {
    pub _id: String,
    pub buckets: HashMap<String, Bucket>,
}

impl std::fmt::Debug for BucketGrouper {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Bucketgrouper")
    }
}

impl Operator for BucketGrouper {
    fn on_event(&mut self, _port: &str, event: Event) -> Result<Vec<(String, Event)>> {
        let meta = &event.data.suffix().meta;
        if let Some(class) = meta.get("class").and_then(Value::as_string) {
            let groups = match self.buckets.get_mut(class.borrow() as &str) {
                Some(g) => g,
                None => {
                    let cardinality = meta
                        .get("cardinality")
                        .and_then(Value::as_u64)
                        .unwrap_or(1000) as usize;
                    self.buckets
                        .insert(class.to_string(), Bucket::new(cardinality));
                    if let Some(g) = self.buckets.get_mut(class.borrow() as &str) {
                        g
                    } else {
                        unreachable!()
                    }
                }
            };
            let d = if let Some(d) = meta.get("dimensions") {
                d
            } else {
                &Value::Null
            };
            let dimensions = serde_json::to_string(d)?;
            let window = match groups.cache.get_mut(&dimensions) {
                None => {
                    let rate = if let Some(rate) = Rate::from_meta(&meta) {
                        rate
                    } else {
                        return Ok(vec![("error".to_string(), event)]);
                    };
                    groups.cache.put(
                        dimensions.clone(),
                        TimeWindow::new(
                            rate.windows,
                            rate.time_range / (rate.windows as u64),
                            rate.rate,
                        ),
                    );
                    if let Some(g) = groups.cache.get_mut(&dimensions) {
                        g
                    } else {
                        unreachable!()
                    }
                }
                Some(m) => m,
            };
            if window.inc_t(event.ingest_ns).is_ok() {
                groups.pass += 1;
                Ok(vec![("out".to_string(), event)])
            } else {
                groups.overflow += 1;
                Ok(vec![("overflow".to_string(), event)])
            }
        } else {
            Ok(vec![("error".to_string(), event)])
        }
    }

    fn metrics(
        &self,
        mut tags: HashMap<Cow<'static, str>, Cow<'static, str>>,
        timestamp: u64,
    ) -> Result<Vec<Value<'static>>> {
        let mut res = Vec::with_capacity(self.buckets.len() * 2);
        for (class, b) in &self.buckets {
            // FIXME: .unwrap() this can be done w/o conversion
            tags.insert("class".into(), class.clone().into());

            tags.insert("action".into(), "pass".into());
            // TODO: this is ugly
            res.push(
                json!({
                    "measurement": "bucketing",
                    "tags": tags,
                    "fields": {
                        "count": b.pass
                    },
                    "timestamp": timestamp
                })
                .into(),
            );
            tags.insert("action".into(), "overflow".into());
            // TODO: this is ugly
            res.push(
                json!({
                    "measurement": "bucketing",
                    "tags": tags,
                    "fields": {
                        "count": b.overflow
                    },
                    "timestamp": timestamp
                })
                .into(),
            );
        }
        Ok(res)
    }
}
