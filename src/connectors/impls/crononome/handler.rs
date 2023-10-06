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

use crate::connectors::prelude::*;
use chrono::{DateTime, Utc};
use cron::Schedule;
use serde_yaml::Value as YamlValue;
use std::cmp::Reverse;
use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd};
use std::collections::BinaryHeap;
use std::convert::TryFrom;
use std::fmt;
use std::str::FromStr;
use std::{clone::Clone, time::Duration};
use tremor_script::prelude::*;

#[derive(Deserialize, Clone)]
pub(crate) struct CronEntry {
    pub(crate) name: String,
    pub(crate) expr: String,
    pub(crate) payload: Option<YamlValue>,
}

#[derive(Clone)]
pub(crate) struct CronEntryInt {
    pub(crate) name: String,
    pub(crate) expr: String,
    pub(crate) sched: Schedule,
    pub(crate) payload: Option<Value<'static>>,
}

impl TryFrom<CronEntry> for CronEntryInt {
    type Error = crate::errors::Error;
    fn try_from(entry: CronEntry) -> Result<Self> {
        let payload = if let Some(yaml_payload) = entry.payload {
            // We use this to translate a yaml value (payload in) to tremor value (payload out)
            let mut payload = simd_json::to_vec(&yaml_payload)?;
            let tremor_payload = tremor_value::parse_to_value(&mut payload)?;
            Some(tremor_payload.into_static())
        } else {
            None
        };
        Ok(Self {
            sched: Schedule::from_str(entry.expr.as_str())?,
            name: entry.name,
            expr: entry.expr,
            payload,
        })
    }
}
impl TryFrom<Value<'static>> for CronEntryInt {
    type Error = crate::errors::Error;
    fn try_from(entry: Value) -> Result<Self> {
        let entry: CronEntry = tremor_value::structurize(entry)?;
        Self::try_from(entry)
    }
}

impl std::fmt::Debug for CronEntryInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} - {}", self.name, self.expr)
    }
}

impl std::fmt::Debug for CronEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} - {}", self.name, self.expr)
    }
}

impl PartialEq for CronEntry {
    fn eq(&self, other: &Self) -> bool {
        self.name.eq(&other.name) && self.expr.eq(&other.expr)
    }
}

#[derive(Debug, Clone)]
struct TemporalItem<I> {
    at: DateTime<Utc>,
    what: I,
}

impl<I> PartialEq for TemporalItem<I> {
    fn eq(&self, other: &Self) -> bool {
        self.at.eq(&other.at)
    }
}

impl<I> PartialOrd for TemporalItem<I> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.at.cmp(&other.at))
    }
}

impl<I> Eq for TemporalItem<I> {
    fn assert_receiver_is_total_eq(&self) {
        self.at.assert_receiver_is_total_eq();
    }
}

impl<I> Ord for TemporalItem<I> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.at.cmp(&other.at)
    }
    fn max(self, other: Self) -> Self
    where
        Self: Sized,
    {
        if self.at > other.at {
            self
        } else {
            other
        }
    }
    fn min(self, other: Self) -> Self
    where
        Self: Sized,
    {
        if other.at > self.at {
            self
        } else {
            other
        }
    }
}

#[derive(Debug, Clone)]
struct TemporalPriorityQueue<I> {
    q: BinaryHeap<Reverse<TemporalItem<I>>>,
}

impl<I> TemporalPriorityQueue<I> {
    pub(crate) fn default() -> Self {
        Self {
            q: BinaryHeap::new(),
        }
    }

    pub(crate) fn enqueue(&mut self, at: TemporalItem<I>) {
        self.q.push(Reverse(at));
    }

    #[cfg(not(feature = "tarpaulin-exclude"))]
    #[cfg(test)]
    pub(crate) fn pop(&mut self) -> Option<TemporalItem<I>> {
        let Reverse(x) = self.q.peek()?;
        let now = Utc::now().timestamp();
        let event = x.at.timestamp();
        if event <= now {
            self.q.pop().map(|Reverse(x)| x)
        } else {
            None
        }
    }

    pub(crate) async fn wait_for_pop(&mut self) -> Option<TemporalItem<I>> {
        let Reverse(x) = self.q.peek()?;
        let now = Utc::now().timestamp();
        let event = x.at.timestamp();
        #[allow(clippy::cast_sign_loss)]
        tokio::time::sleep(Duration::from_secs(
            (event as u64).saturating_sub(now as u64),
        ))
        .await;
        self.q.pop().map(|Reverse(x)| x)
    }

    #[cfg(not(feature = "tarpaulin-exclude"))]
    #[cfg(test)]
    pub(crate) fn drain(&mut self) -> Vec<TemporalItem<I>> {
        let now = Utc::now().timestamp();
        let mut sched: Vec<TemporalItem<I>> = vec![];
        // for next in self.q.iter() {
        loop {
            let next = self.q.peek();
            //            let next = self.q.peek();
            match next {
                Some(Reverse(x)) => {
                    let event = x.at.timestamp();
                    if event <= now {
                        match self.q.pop() {
                            Some(Reverse(x)) => {
                                sched.push(x);
                            }
                            None => continue, // should never occur in practice
                        }
                    } else {
                        break;
                    }
                }
                None => break,
            }
        }

        sched
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ChronomicQueue {
    tpq: TemporalPriorityQueue<CronEntryInt>,
}

impl ChronomicQueue {
    pub(crate) fn default() -> Self {
        Self {
            tpq: TemporalPriorityQueue::default(),
        }
    }

    pub(crate) fn enqueue(&mut self, entry: &CronEntryInt) {
        if let Some(at) = entry.sched.upcoming(chrono::Utc).next() {
            self.tpq.enqueue(TemporalItem {
                at,
                what: entry.clone(),
            });
        }
    }

    #[cfg(not(feature = "tarpaulin-exclude"))]
    #[cfg(test)]
    pub(crate) fn drain(&mut self) -> Vec<(String, Option<Value<'static>>)> {
        let due = self.tpq.drain();
        let mut trigger: Vec<(String, Option<Value<'static>>)> = vec![];
        for ti in &due {
            // Enqueue next scheduled event if any
            self.enqueue(&ti.what);
            trigger.push((ti.what.name.clone(), ti.what.payload.clone()));
        }
        trigger
    }
    #[cfg(not(feature = "tarpaulin-exclude"))]
    #[cfg(test)]
    pub(crate) fn next(&mut self) -> Option<(String, Option<Value<'static>>)> {
        self.tpq.pop().map(|ti| {
            self.enqueue(&ti.what);
            (ti.what.name, ti.what.payload)
        })
    }

    pub(crate) async fn wait_for_next(&mut self) -> Option<(String, Option<Value<'static>>)> {
        self.tpq.wait_for_pop().await.map(|ti| {
            self.enqueue(&ti.what);
            (ti.what.name, ti.what.payload)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryFrom;

    #[cfg(not(feature = "tarpaulin-exclude"))]
    use chrono::DateTime;
    #[cfg(not(feature = "tarpaulin-exclude"))]
    use std::time::Duration;

    #[test]
    fn test_deserialize_cron_entry() -> Result<()> {
        let mut s = b"{\"name\": \"test\", \"expr\": \"* 0 0 * * * *\"}".to_vec();
        let entry = CronEntryInt::try_from(simd_json::from_slice::<CronEntry>(s.as_mut_slice())?)?;
        assert_eq!("test", entry.name);
        assert_eq!("* 0 0 * * * *", entry.expr);
        Ok(())
    }

    #[test]
    fn test_deserialize_cron_entry_error() -> Result<()> {
        let mut s = b"{\"name\": \"test\", \"expr\": \"snot snot\"}".to_vec();
        let entry: CronEntry = simd_json::from_slice(s.as_mut_slice())?;
        assert_eq!("test", entry.name);
        assert!(CronEntryInt::try_from(entry).is_err());
        Ok(())
    }

    // This tight requirements for timing are extremely problematic in tests
    // and lead to frequent issues with the test being flaky or unrelaibale
    #[cfg(not(feature = "tarpaulin-exclude"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpq_fill_drain() -> Result<()> {
        use chrono::prelude::Utc;
        let mut tpq = TemporalPriorityQueue::default();
        let n1 = TemporalItem {
            at: DateTime::from(std::time::SystemTime::UNIX_EPOCH), // Epoch
            what: CronEntryInt::try_from(literal!({
                "name": "a",
                "expr": "* * * * * * 1970"
            }))?,
        };
        let n2 = TemporalItem {
            at: Utc::now(),
            what: CronEntryInt::try_from(literal!({
                "name": "b",
                "expr": "* * * * * * *"
            }))?,
        };

        tpq.enqueue(n1.clone());
        tpq.enqueue(n2.clone());
        let due = tpq.drain();
        assert_eq!(vec![n1.clone(), n2], due);

        tokio::time::sleep(Duration::from_millis(1000)).await;
        assert!(tpq.drain().is_empty());
        tokio::time::sleep(Duration::from_millis(1000)).await;
        assert!(tpq.drain().is_empty());

        Ok(())
    }

    // This tight requirements for timing are extremely problematic in tests
    // and lead to frequent issues with the test being flaky or unrelaibale
    #[cfg(not(feature = "tarpaulin-exclude"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpq_fill_pop() -> Result<()> {
        use chrono::prelude::Utc;
        let mut tpq = TemporalPriorityQueue::default();
        let n1 = TemporalItem {
            at: DateTime::from(std::time::SystemTime::UNIX_EPOCH), // Epoch
            what: CronEntryInt::try_from(literal!({
                "name": "a",
                "expr": "* * * * * * 1970"
            }))?,
        };
        let n2 = TemporalItem {
            at: Utc::now(),
            what: CronEntryInt::try_from(literal!({
                "name": "b",
                "expr": "* * * * * * *"
            }))?,
        };

        tpq.enqueue(n1.clone());
        tpq.enqueue(n2.clone());

        assert_eq!(Some(n1), tpq.pop());
        assert_eq!(Some(n2), tpq.pop());

        tokio::time::sleep(Duration::from_millis(1000)).await;
        assert!(tpq.pop().is_none());
        tokio::time::sleep(Duration::from_millis(1000)).await;
        assert!(tpq.pop().is_none());

        Ok(())
    }

    // This tight requirements for timing are extremely problematic in tests
    // and lead to frequent issues with the test being flaky or unrelaibale
    #[cfg(not(feature = "tarpaulin-exclude"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_cq_fill_drain_refill() -> Result<()> {
        let mut cq = ChronomicQueue::default();
        // Dates before Jan 1st 1970 are invalid
        let n1 = CronEntryInt::try_from(literal!({
            "name": "a",
            "expr": "* * * * * * 1970", // Dates before UNIX epoch start invalid
        }))?;
        let n2 = CronEntryInt::try_from(literal!({
            "name": "b",
            "expr": "* * * * * * *",
        }))?;
        let n3 = CronEntryInt::try_from(literal!({
            "name": "c",
            "expr": "* * * * * * 2038", // limit is 2038 due to the 2038 problem ( we'll be retired so letting this hang wait! )
        }))?;

        cq.enqueue(&n1);
        cq.enqueue(&n2);
        cq.enqueue(&n3);
        let due = cq.drain();
        assert_eq!(0, due.len());

        tokio::time::sleep(Duration::from_millis(1000)).await;
        assert_eq!(1, cq.drain().len());

        tokio::time::sleep(Duration::from_millis(1000)).await;
        assert_eq!(1, cq.drain().len());

        Ok(())
    }

    // This tight requirements for timing are extremely problematic in tests
    // and lead to frequent issues with the test being flaky or unrelaibale
    #[cfg(not(feature = "tarpaulin-exclude"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_cq_fill_pop_refill() -> Result<()> {
        let mut cq = ChronomicQueue::default();
        // Dates before Jan 1st 1970 are invalid
        let n1 = CronEntryInt::try_from(literal!({
            "name": "a",
            "expr": "* * * * * * 1970" // Dates before UNIX epoch start invalid
        }))?;
        let n2 = CronEntryInt::try_from(literal!({
            "name": "b".to_string(),
            "expr": "* * * * * * *".to_string()
        }))?;
        let n3 = CronEntryInt::try_from(literal!({
            "name": "c",
            "expr": "* * * * * * 2038" // limit is 2038 due to the 2038 problem ( we'll be retired so letting this hang wait! )
        }))?;

        cq.enqueue(&n1);
        cq.enqueue(&n2);
        cq.enqueue(&n3);
        assert!(cq.next().is_none());

        tokio::time::sleep(Duration::from_millis(1000)).await;
        assert!(cq.next().is_some());
        assert!(cq.next().is_none());
        tokio::time::sleep(Duration::from_millis(1000)).await;
        assert!(cq.next().is_some());

        Ok(())
    }

    #[test]
    fn test_cron_rs() {
        // NOTE Handy to check cron.rs limitations & constraints
        use cron::{Schedule, TimeUnitSpec};
        use std::collections::Bound::{Excluded, Included};
        use std::str::FromStr;

        let expression = "* * * * * * 2015-2044";
        let schedule = Schedule::from_str(expression).expect("Failed to parse expression.");

        // Membership
        assert_eq!(true, schedule.years().includes(2031));
        assert_eq!(false, schedule.years().includes(1969));

        // Number of years specified
        assert_eq!(30, schedule.years().count());

        // Iterator
        let mut years_iter = schedule.years().iter();
        assert_eq!(Some(2015), years_iter.next());
        assert_eq!(Some(2016), years_iter.next());
        // ...

        // Range Iterator
        let mut five_year_plan = schedule.years().range((Included(2017), Excluded(2017 + 5)));
        assert_eq!(Some(2017), five_year_plan.next());
        assert_eq!(Some(2018), five_year_plan.next());
        assert_eq!(Some(2019), five_year_plan.next());
        assert_eq!(Some(2020), five_year_plan.next());
        assert_eq!(Some(2021), five_year_plan.next());
        assert_eq!(None, five_year_plan.next());
    }
}
