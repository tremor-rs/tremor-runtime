// Copyright 2020-2021, The Tremor Team
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

//! Bucket will perform a sliding window rate limiting based on event metadata. Limits are applied for every `$class`. In a `$class` each `$dimensions` is allowed to pass `$rate` messages per second.
//!
//! This operator does not support configuration.
//!
//! This operator preserves event metadata.
//!
//! **Metadata Variables**:
//!
//! - `$class` - The class of an event. (String)
//! - `$rate` - Allowed events per second per class/dimension (Number)
//! - (Optional) `$dimensions` - The dimensions of the event. (Any)
//! - (Optional)`$cardinality` - the maximum number of dimensions kept track of at the same time (Number, default: `1000`)
//!
//! **Outputs**:
//!
//! - `out`
//! - `error` - Unprocessable events for example if `$class` or `$rate` are not set.
//! - `overflow` - Events that exceed the rate defined for them
//!
//! **Example**:
//!
//! ```tremor
//! define operator group from grouper::bucket;
//! ```
//!
//! **Metrics**:
//!
//! The bucket operator generates additional metrics. For each class the following two statistics are generated (as an example):
//!
//! ```json
//! {"measurement":"bucketing",
//!  "tags":{
//!    "action":"pass",
//!    "class":"test",
//!    "direction":"output",
//!    "node":"bucketing",
//!    "pipeline":"main",
//!    "port":"out"
//!  },
//!  "fields":{"count":93},
//!  "timestamp":1553012903452340000
//! }
//! {"measurement":"bucketing",
//!  "tags":{
//!    "action":"overflow",
//!    "class":"test",
//!    "direction":"output",
//!    "node":"bucketing",
//!    "pipeline":"main",
//!    "port":"out"
//!  },
//!  "fields":{"count":127},
//!  "timestamp":1553012903452340000
//! }
//! ```
//!
//! This tells us the following, up until this measurement was published in the class `test`:
//!
//! - (`pass`) Passed 93 events
//! - (`overflow`) Marked 127 events as overflow due to not fitting in the limit

use std::num::NonZeroUsize;

use crate::errors::{ErrorKind, Result};
use crate::metrics::value_count;
use crate::op::prelude::*;
use crate::{Event, Operator};
use beef::Cow;
use halfbrown::HashMap;
use lru::LruCache;
use tremor_script::prelude::*;
use window::TimeWindow;

const BUCKETING: Cow<'static, str> = Cow::const_str("bucketing");
const CLASS: Cow<'static, str> = Cow::const_str("class");
const ACTION: Cow<'static, str> = Cow::const_str("action");
const PASS: Cow<'static, str> = Cow::const_str("pass");

op!(BucketGrouperFactory(_uid, node) {
    if node.config.is_none() {
        Ok(Box::new(Grouper {
            buckets: HashMap::new(),
        }))
    } else {
        Err(ErrorKind::ExtraOpConfig(node.id.clone()).into())
    }
});
/// Single bucket specification
#[derive(Debug)]
struct Rate {
    /// the maximum number of events per time range
    rate: u64,
    /// time range in milliseconds, (default: 1000 - 1 second)
    time_range: u64,
    /// numbers of window in the time_range (default: 100)
    windows: usize,
}

impl Rate {
    pub fn from_meta(meta: &Value) -> Option<Self> {
        let rate = meta.get("rate")?.as_u64()?;

        let time_range = meta.get_u64("time_range").unwrap_or(1000);
        let windows = meta.get_usize("windows").unwrap_or(100);
        Some(Self {
            rate,
            time_range,
            windows,
        })
    }
}

struct Bucket {
    cache: LruCache<String, TimeWindow>,
    pass: u64,
    overflow: u64,
}
impl Bucket {
    fn new(cardinality: usize) -> Self {
        Self {
            cache: LruCache::new(
                NonZeroUsize::new(cardinality)
                    // ALLOW: 1000 is not 0, so we are good here
                    .unwrap_or(NonZeroUsize::new(1000).expect("1000 is not 0")),
            ),
            pass: 0,
            overflow: 0,
        }
    }
}

pub(crate) struct Grouper {
    buckets: HashMap<String, Bucket>,
}

// #[cfg_attr(coverage, no_coverage)]
impl std::fmt::Debug for Grouper {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Bucketgrouper")
    }
}

impl Operator for Grouper {
    #[allow(clippy::manual_let_else)] // clippy bug
    fn on_event(
        &mut self,
        _node_id: u64,
        _uid: OperatorUId,
        _port: &Port<'static>,
        _state: &mut Value<'static>,
        event: Event,
    ) -> Result<EventAndInsights> {
        let meta = event.data.suffix().meta();
        if let Some(class) = meta.get_str("class") {
            let (_, groups) = self
                .buckets
                .raw_entry_mut()
                .from_key(class)
                .or_insert_with(|| {
                    let cardinality = meta.get_usize("cardinality").unwrap_or(1000);
                    (class.to_string(), Bucket::new(cardinality))
                });

            let d = meta.get("dimensions").unwrap_or(&NULL);
            let dimensions = d.encode();
            let window = if let Some(window) = groups.cache.get_mut(&dimensions) {
                window
            } else {
                let Some(rate) = Rate::from_meta(meta) else {
                        return Ok(vec![(ERR, event)].into());
                };
                groups.cache.put(
                    dimensions.clone(),
                    TimeWindow::new(
                        rate.windows,
                        rate.time_range / (rate.windows as u64),
                        rate.rate,
                    ),
                );
                let Some(g) = groups.cache.get_mut(&dimensions) else {
                    //ALLOW: we just put this entry in. The Entry API https://github.com/jeromefroe/lru-rs/issues/30 would solve this
                    unreachable!()
                };
                g
            };
            if window.inc_t(event.ingest_ns).is_ok() {
                groups.pass += 1;
                Ok(event.into())
            } else {
                groups.overflow += 1;
                Ok(vec![(OVERFLOW, event)].into())
            }
        } else {
            Ok(vec![(ERR, event)].into())
        }
    }

    fn metrics(
        &self,
        tags: &HashMap<Cow<'static, str>, Value<'static>>,
        timestamp: u64,
    ) -> Result<Vec<Value<'static>>> {
        let mut res = Vec::with_capacity(self.buckets.len() * 2);
        if !self.buckets.is_empty() {
            let mut tags = tags.clone();
            for (class, b) in &self.buckets {
                tags.insert(CLASS, class.clone().into());
                tags.insert(ACTION, PASS.into());
                // Count good cases
                res.push(value_count(BUCKETING, tags.clone(), b.pass, timestamp));
                // Count bad cases
                tags.insert(ACTION, OVERFLOW.into());
                res.push(value_count(BUCKETING, tags.clone(), b.overflow, timestamp));
            }
        }
        Ok(res)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tremor_common::uids::UId;
    use tremor_value::Value;

    #[test]
    fn bucket() -> Result<()> {
        let operator_id = OperatorUId::new(0);
        let mut op = Grouper {
            buckets: HashMap::new(),
        };
        let event1 = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1,
            data: Value::from("snot").into(),
            ..Event::default()
        };

        let mut state = Value::null();

        let mut r = op
            .on_event(0, operator_id, &Port::In, &mut state, event1.clone())
            .expect("could not run pipeline");

        let (port, e) = r.events.pop().ok_or("no data")?;
        assert!(r.events.is_empty());
        assert_eq!(port, "err");
        assert_eq!(e, event1);

        // let meta =
        let event2 = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 1,
            data: (Value::from("snot"), literal!({"class": "test", "rate": 2})).into(),
            ..Event::default()
        };

        let mut r = op
            .on_event(0, operator_id, &Port::In, &mut state, event2.clone())
            .expect("could not run pipeline");

        let (port, e) = r.events.pop().ok_or("no data")?;
        assert!(r.events.is_empty());
        assert_eq!(port, "out");
        assert_eq!(e, event2);

        let mut r = op
            .on_event(0, operator_id, &Port::In, &mut state, event2.clone())
            .expect("could not run pipeline");

        let (port, e) = r.events.pop().ok_or("no data")?;
        assert!(r.events.is_empty());
        assert_eq!(port, "out");
        assert_eq!(e, event2);

        let mut r = op
            .on_event(0, operator_id, &Port::In, &mut state, event2.clone())
            .expect("could not run pipeline");

        let (port, e) = r.events.pop().ok_or("no data")?;
        assert!(r.events.is_empty());
        assert_eq!(port, "overflow");
        assert_eq!(e, event2);

        let event3 = Event {
            id: (1, 1, 1).into(),
            ingest_ns: 10_000_000_002,
            data: (Value::from("snot"), literal!({"class": "test", "rate": 2})).into(),
            ..Event::default()
        };

        let mut r = op
            .on_event(0, operator_id, &Port::In, &mut state, event3.clone())
            .expect("could not run pipeline");

        let (port, e) = r.events.pop().ok_or("no data")?;
        assert!(r.events.is_empty());
        assert_eq!(port, "out");
        assert_eq!(e, event3);

        let mut m = op.metrics(&HashMap::new(), 0)?;
        let overflow = m.pop().ok_or("no data")?;
        let pass = m.pop().ok_or("no data")?;
        assert!(m.is_empty());
        assert_eq!(overflow["tags"]["action"], "overflow");
        assert_eq!(overflow["fields"]["count"], 1);
        assert_eq!(pass["tags"]["action"], "pass");
        assert_eq!(pass["fields"]["count"], 3);
        Ok(())
    }
}
