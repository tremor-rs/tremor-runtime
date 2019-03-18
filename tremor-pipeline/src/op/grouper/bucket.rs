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
use hashbrown::HashMap;
use lru::LruCache;
use serde_json;
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
    pub fn from_meta(meta: &HashMap<String, serde_json::Value>) -> Option<Self> {
        match meta.get("rate") {
            Some(serde_json::Value::Number(rate)) if rate.is_u64() => {
                let rate = rate.as_u64().unwrap();
                let time_range = match meta.get("time_range") {
                    Some(serde_json::Value::Number(time_range)) if time_range.is_u64() => {
                        time_range.as_u64().unwrap()
                    }
                    _ => 1000,
                };
                let windows: usize = match meta.get("windows") {
                    Some(serde_json::Value::Number(windows)) if windows.is_u64() => {
                        windows.as_u64().unwrap() as usize
                    }
                    _ => 100,
                };
                Some(Rate {
                    rate,
                    time_range,
                    windows,
                })
            }
            _ => None,
        }
    }
}

type Bucket = LruCache<String, TimeWindow>;

pub struct BucketGrouper {
    _id: String,
    buckets: HashMap<String, Bucket>,
}

impl std::fmt::Debug for BucketGrouper {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Bucketgrouper")
    }
}

impl Operator for BucketGrouper {
    fn on_event(&mut self, _port: &str, event: Event) -> Result<Vec<(String, Event)>> {
        if let Some(serde_json::Value::String(class)) = event.meta.get("class") {
            let groups = match self.buckets.get_mut(class) {
                Some(g) => g,
                None => {
                    let cardinality = match event.meta.get("cardinality") {
                        Some(serde_json::Value::Number(cardinality)) if cardinality.is_u64() => {
                            cardinality.as_u64().unwrap() as usize
                        }
                        _ => 1000,
                    };
                    self.buckets
                        .insert(class.clone(), LruCache::new(cardinality));
                    self.buckets.get_mut(class).unwrap()
                }
            };
            let d = if let Some(d) = event.meta.get("dimensions") {
                d
            } else {
                &serde_json::Value::Null
            };
            let dimensions = serde_json::to_string(d)?;
            let window = match groups.get_mut(&dimensions) {
                None => {
                    let rate = if let Some(rate) = Rate::from_meta(&event.meta) {
                        rate
                    } else {
                        return Ok(vec![("error".to_string(), event)]);
                    };
                    groups.put(
                        dimensions.clone(),
                        TimeWindow::new(
                            rate.windows,
                            rate.time_range / (rate.windows as u64),
                            rate.rate,
                        ),
                    );
                    groups.get_mut(&dimensions).unwrap()
                }
                Some(m) => m,
            };
            if window.inc_t(event.ingest_ns).is_ok() {
                Ok(vec![("out".to_string(), event)])
            } else {
                Ok(vec![("overflow".to_string(), event)])
            }
        } else {
            Ok(vec![("error".to_string(), event)])
        }
    }
}
