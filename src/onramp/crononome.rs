// Copyright 2018-2020, Wayfair GmbH
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

use crate::onramp::prelude::*;
use crate::utils::nanotime;
use chrono::{DateTime, Utc};
use cron::Schedule;
use serde_yaml::Value;
use std::clone::Clone;
use std::cmp::Reverse;
use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd};
use std::collections::BinaryHeap;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

#[derive(Deserialize)]
pub struct CronEntry {
    pub name: String,
    pub expr: String,
    #[serde(skip)]
    pub sched: Option<Schedule>,
    pub payload: Option<Value>,
}

impl std::fmt::Debug for CronEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} - {}", self.name, self.expr)
    }
}

impl Clone for CronEntry {
    fn clone(&self) -> Self {
        let mut fresh = Self {
            name: self.name.clone(),
            expr: self.expr.clone(),
            sched: None,
            payload: self.payload.clone(),
        };
        fresh.parse().ok();
        fresh
    }
}

impl PartialEq for CronEntry {
    fn eq(&self, other: &Self) -> bool {
        self.name.eq(&other.name) && self.expr.eq(&other.expr)
    }
}

impl CronEntry {
    #[allow(dead_code)]
    fn parse(&mut self) -> std::result::Result<(), cron::error::Error> {
        match Schedule::from_str(self.expr.as_str()) {
            Ok(x) => {
                self.sched = Some(x);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

#[derive(Deserialize, Clone)]
pub struct Config {
    pub entries: Vec<CronEntry>,
}

impl ConfigImpl for Config {}

#[derive(Clone)]
pub struct Crononome {
    pub config: Config,
    origin_uri: tremor_pipeline::EventOriginUri,
    cq: ChronomicQueue,
    id: u64,
}

impl onramp::Impl for Crononome {
    fn from_config(config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            let origin_uri = tremor_pipeline::EventOriginUri {
                scheme: "tremor-crononome".to_string(),
                host: hostname(),
                port: None,
                path: vec![],
            };

            Ok(Box::new(Self {
                origin_uri,
                config,
                id: 0,
                cq: ChronomicQueue::default(),
            }))
        } else {
            Err("Missing config for crononome onramp".into())
        }
    }
}

#[derive(Debug, Clone)]
struct TemporalItem<I> {
    at: DateTime<Utc>,
    what: I,
}

impl<I: Clone> TemporalItem<I> {
    #[allow(dead_code)]
    pub fn value(&self) -> &I {
        &self.what
    }
}
impl<I> PartialEq for TemporalItem<I> {
    fn eq(&self, other: &Self) -> bool {
        self.at.eq(&other.at)
    }
}

impl<I> PartialOrd for TemporalItem<I> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.at.partial_cmp(&other.at)
    }
}

impl<I> Eq for TemporalItem<I> {
    fn assert_receiver_is_total_eq(&self) {
        self.at.assert_receiver_is_total_eq()
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

#[allow(dead_code)]
impl<I> TemporalPriorityQueue<I> {
    pub fn default() -> Self {
        Self {
            q: BinaryHeap::new(),
        }
    }

    pub fn enqueue(&mut self, at: TemporalItem<I>) {
        self.q.push(Reverse(at))
    }

    pub fn pop(&mut self) -> Option<TemporalItem<I>> {
        if let Some(Reverse(x)) = self.q.peek() {
            let now = Utc::now().timestamp();
            let event = x.at.timestamp();
            if event <= now {
                self.q.pop().map(|Reverse(x)| x)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn drain(&mut self) -> Vec<TemporalItem<I>> {
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
                            None => continue, // FIXME should never occur in practice
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
struct ChronomicQueue {
    tpq: TemporalPriorityQueue<CronEntry>,
}

#[allow(dead_code)]
impl ChronomicQueue {
    pub fn default() -> Self {
        Self {
            tpq: TemporalPriorityQueue::default(),
        }
    }

    pub fn enqueue(&mut self, entry: &CronEntry) {
        let next = match &entry.sched {
            Some(x) => x.upcoming(chrono::Utc).take(1).collect(),
            None => vec![],
        };

        if let Some(at) = next.get(0) {
            let mut entry2 = CronEntry {
                name: entry.name.clone(),
                expr: entry.expr.clone(),
                sched: None,
                payload: entry.payload.clone(),
            };
            let x = entry2.parse();
            // FIXME propagate any errors
            if x.is_ok() {
                self.tpq.enqueue(TemporalItem {
                    at: *at,
                    what: entry2,
                });
            }
        }
        // No future events for this entry => drop
    }

    pub fn drain(&mut self) -> Vec<(String, Option<Value>)> {
        let due = self.tpq.drain();
        let mut trigger: Vec<(String, Option<Value>)> = vec![];
        for ti in &due {
            // Enqueue next scheduled event if any
            self.enqueue(&ti.what);
            trigger.push((ti.what.name.clone(), ti.what.payload.clone()));
        }
        trigger
    }
    pub fn next(&mut self) -> Option<(String, Option<Value>)> {
        if let Some(ti) = self.tpq.pop() {
            self.enqueue(&ti.what);
            Some((ti.what.name, ti.what.payload))
        } else {
            None
        }
    }
}

#[async_trait::async_trait()]
impl Source for Crononome {
    async fn read(&mut self) -> Result<SourceReply> {
        let ingest_ns = nanotime();
        if let Some(trigger) = self.cq.next() {
            let data = simd_json::to_vec(
                &json!({"onramp": "crononome", "ingest_ns": ingest_ns, "id": self.id, "trigger": {
                        "name": trigger.0,
                        "payload": trigger.1
                    },
                }),
            )?;
            self.id += 1;
            Ok(SourceReply::Data {
                origin_uri: self.origin_uri.clone(),
                data,
                stream: 0,
            })
        } else {
            task::sleep(Duration::from_millis(100)).await;
            Ok(SourceReply::Empty)
        }
    }

    async fn init(&mut self) -> Result<SourceState> {
        for entry in &self.config.entries {
            self.cq.enqueue(&entry);
        }
        Ok(SourceState::Connected)
    }

    fn trigger_breaker(&mut self) {}
    fn restore_breaker(&mut self) {}
}

#[async_trait::async_trait]
impl Onramp for Crononome {
    async fn start(
        &mut self,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        let source = self.clone();

        for entry in &mut self.config.entries {
            if entry.parse().ok().is_none() {
                return Err(format!(
                    "Bad configuration in crononome - expression {} is illegal",
                    entry.name
                )
                .into());
            }
        }

        let (manager, tx) =
            SourceManager::new(source, preprocessors, codec, metrics_reporter).await?;
        thread::Builder::new()
            .name(format!("onramp-crononome-{}", "???"))
            .spawn(move || task::block_on(manager.run()))?;
        Ok(tx)
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{self, DateTime};
    use std::time::Duration;

    #[test]
    pub fn test_deserialize_cron_entry() -> Result<()> {
        let mut s = b"{\"name\": \"test\", \"expr\": \"* 0 0 * * * *\"}".to_vec();
        let mut entry: CronEntry = simd_json::from_slice(s.as_mut_slice())?;
        assert_eq!("test", entry.name);
        assert_eq!("* 0 0 * * * *", entry.expr);
        entry.parse().ok();
        assert!(entry.sched.is_some());
        Ok(())
    }

    #[test]
    pub fn test_deserialize_cron_entry_error() -> Result<()> {
        let mut s = b"{\"name\": \"test\", \"expr\": \"snot snot\"}".to_vec();
        let mut entry: CronEntry = simd_json::from_slice(s.as_mut_slice())?;
        assert_eq!("test", entry.name);
        assert!(entry.parse().ok().is_none());
        Ok(())
    }

    #[test]
    pub fn test_tpq_fill_drain() -> Result<()> {
        use chrono::prelude::Utc;
        let mut tpq = TemporalPriorityQueue::default();
        let mut n1 = TemporalItem {
            at: DateTime::from(std::time::SystemTime::UNIX_EPOCH), // Epoch
            what: CronEntry {
                name: "a".to_string(),
                expr: "* * * * * * 1970".to_string(),
                sched: None,
                payload: None,
            },
        };
        let mut n2 = TemporalItem {
            at: Utc::now(),
            what: CronEntry {
                name: "b".to_string(),
                expr: "* * * * * * *".to_string(),
                sched: None,
                payload: None,
            },
        };

        n1.what.parse().ok();
        n2.what.parse().ok();

        tpq.enqueue(n1.clone());
        tpq.enqueue(n2.clone());
        let due = tpq.drain();
        assert_eq!(vec![n1.clone(), n2], due);

        std::thread::sleep(Duration::from_millis(1000));
        assert!(tpq.drain().is_empty());
        std::thread::sleep(Duration::from_millis(1000));
        assert!(tpq.drain().is_empty());

        Ok(())
    }

    #[test]
    pub fn test_tpq_fill_pop() -> Result<()> {
        use chrono::prelude::Utc;
        let mut tpq = TemporalPriorityQueue::default();
        let mut n1 = TemporalItem {
            at: DateTime::from(std::time::SystemTime::UNIX_EPOCH), // Epoch
            what: CronEntry {
                name: "a".to_string(),
                expr: "* * * * * * 1970".to_string(),
                sched: None,
                payload: None,
            },
        };
        let mut n2 = TemporalItem {
            at: Utc::now(),
            what: CronEntry {
                name: "b".to_string(),
                expr: "* * * * * * *".to_string(),
                sched: None,
                payload: None,
            },
        };

        n1.what.parse().ok();
        n2.what.parse().ok();

        tpq.enqueue(n1.clone());
        tpq.enqueue(n2.clone());

        assert_eq!(Some(n1), tpq.pop());
        assert_eq!(Some(n2), tpq.pop());

        std::thread::sleep(Duration::from_millis(1000));
        assert!(tpq.pop().is_none());
        std::thread::sleep(Duration::from_millis(1000));
        assert!(tpq.pop().is_none());

        Ok(())
    }

    #[test]
    pub fn test_cq_fill_drain_refill() -> Result<()> {
        let mut cq = ChronomicQueue::default();
        // Dates before Jan 1st 1970 are invalid
        let mut n1 = CronEntry {
            name: "a".to_string(),
            expr: "* * * * * * 1970".to_string(), // Dates before UNIX epoch start invalid
            sched: None,
            payload: None,
        };
        let mut n2 = CronEntry {
            name: "b".to_string(),
            expr: "* * * * * * *".to_string(),
            sched: None,
            payload: None,
        };
        let mut n3 = CronEntry {
            name: "c".to_string(),
            expr: "* * * * * * 2038".to_string(), // limit is 2038 due to the 2038 problem ( we'll be retired so letting this hang wait! )
            sched: None,
            payload: None,
        };

        n1.parse().ok();
        n2.parse().ok();
        n3.parse().ok();

        cq.enqueue(&n1);
        cq.enqueue(&n2);
        cq.enqueue(&n3);
        let due = cq.drain();
        assert_eq!(0, due.len());

        std::thread::sleep(Duration::from_millis(1000));
        assert_eq!(1, cq.drain().len());

        std::thread::sleep(Duration::from_millis(1000));
        assert_eq!(1, cq.drain().len());

        Ok(())
    }

    #[test]
    pub fn test_cq_fill_pop_refill() -> Result<()> {
        let mut cq = ChronomicQueue::default();
        // Dates before Jan 1st 1970 are invalid
        let mut n1 = CronEntry {
            name: "a".to_string(),
            expr: "* * * * * * 1970".to_string(), // Dates before UNIX epoch start invalid
            sched: None,
            payload: None,
        };
        let mut n2 = CronEntry {
            name: "b".to_string(),
            expr: "* * * * * * *".to_string(),
            sched: None,
            payload: None,
        };
        let mut n3 = CronEntry {
            name: "c".to_string(),
            expr: "* * * * * * 2038".to_string(), // limit is 2038 due to the 2038 problem ( we'll be retired so letting this hang wait! )
            sched: None,
            payload: None,
        };

        n1.parse().ok();
        n2.parse().ok();
        n3.parse().ok();

        cq.enqueue(&n1);
        cq.enqueue(&n2);
        cq.enqueue(&n3);
        assert!(cq.next().is_none());

        std::thread::sleep(Duration::from_millis(1000));
        assert!(cq.next().is_some());
        assert!(cq.next().is_none());
        std::thread::sleep(Duration::from_millis(1000));
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
