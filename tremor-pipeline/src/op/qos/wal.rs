// Copyright 2020, The Tremor Team
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

use crate::op::prelude::*;
use byteorder::{BigEndian, ReadBytesExt};
use simd_json_derive::{Deserialize, Serialize};
use sled::IVec;
use std::io::Cursor;
use std::mem;
use std::ops::{Add, AddAssign};
use tremor_script::prelude::*;
const OUT: Cow<'static, str> = Cow::Borrowed("out");

#[derive(Clone, Copy, Default)]
struct Idx([u8; 8]);
impl std::fmt::Debug for Idx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Idx({})", u64::from(self))
    }
}

impl std::fmt::Display for Idx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", u64::from(self))
    }
}

impl AddAssign<u64> for Idx {
    fn add_assign(&mut self, other: u64) {
        let this: u64 = u64::from(&*self);
        self.0 = unsafe { mem::transmute((this + other).to_be()) };
    }
}

impl Add<u64> for Idx {
    type Output = Idx;
    fn add(self, rhs: u64) -> Self::Output {
        Idx::from(u64::from(self) + rhs)
    }
}

impl Add<u8> for Idx {
    type Output = Idx;
    fn add(self, rhs: u8) -> Self::Output {
        self + u64::from(rhs)
    }
}

impl Add<usize> for Idx {
    type Output = Idx;
    fn add(self, rhs: usize) -> Self::Output {
        Idx::from(u64::from(self) + rhs as u64)
    }
}

impl From<&Idx> for u64 {
    fn from(i: &Idx) -> u64 {
        let mut rdr = Cursor::new(&i.0);
        rdr.read_u64::<BigEndian>().unwrap_or(0)
    }
}

impl From<&mut Idx> for u64 {
    fn from(i: &mut Idx) -> u64 {
        let mut rdr = Cursor::new(&i.0);
        rdr.read_u64::<BigEndian>().unwrap_or(0)
    }
}

impl From<Idx> for u64 {
    fn from(i: Idx) -> u64 {
        let mut rdr = Cursor::new(&i.0);
        rdr.read_u64::<BigEndian>().unwrap_or(0)
    }
}

impl From<IVec> for Idx {
    fn from(v: IVec) -> Self {
        let mut rdr = Cursor::new(v);
        let res: u64 = rdr.read_u64::<BigEndian>().unwrap_or(0);
        Self(unsafe { mem::transmute(res.to_be()) })
    }
}
impl From<u64> for Idx {
    fn from(v: u64) -> Self {
        Self(unsafe { mem::transmute(v.to_be()) })
    }
}

impl Idx {
    fn set(&mut self, v: u64) {
        self.0 = unsafe { mem::transmute(v.to_be()) };
    }
    fn set_min(&mut self, v: u64) {
        if v < u64::from(*self) {
            self.0 = unsafe { mem::transmute(v.to_be()) };
        }
    }
}
impl AsRef<[u8]> for Idx {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<Idx> for IVec {
    fn from(i: Idx) -> Self {
        IVec::from(&i.0)
    }
}

#[derive(Debug, Clone, Deserialize, serde::Deserialize, serde::Serialize)]
pub struct Config {
    /// Maximum number of events to read per tick/event when filling
    /// up from the persistant storage
    pub read_count: usize,

    /// The directory to store data in, if no dir is provided this will use
    /// a temporary storage that won't persist over restarts
    pub dir: Option<String>,

    /// The maximum elements to store before breaking the circuit
    /// note this is an approximation we might store a few elements
    /// above that to allow circuit breakers to kick in
    pub max_elements: u64,

    /// Maximum number of bytes the WAL is alloed to take on disk,
    /// note this is a soft maximum and might be overshot slighty
    pub max_bytes: u64,

    /// Flush to disk on every write
    flush_on_evnt: Option<bool>,
}

impl ConfigImpl for Config {}

#[derive(Debug, Clone)]
// TODO add seed value and field name as config items
/// A Write Ahead Log that will persist data to disk and feed the following operators from this disk
/// cache. It allows to run onramps that do not provide any support for delivery guarantees with
/// offramps that do.
///
/// The wal operator will intercept and generate it's own circuit breaker events. You can think about it
/// as a firewall that will protect all operators before itself from issues beyond it. On the other hand
/// it will indiscriminately consume data from sources and operators before itself until it's own
/// circuit breaking conditions are met.
///
/// At the same time will it interact with tremors guaranteed delivery system, events are only removed
/// from disk once they're acknowledged. In case of delivery failure the WAL operator will replay the
/// failed events. On the same way the WAL operator will acknowledge events that it persists to disk.
///
/// The WAL operator should be used with caution, since every event that passes through it will be
/// written to the hard drive it has a significant performance impact.
pub struct WAL {
    /// Elements currently in the event storage
    cnt: u64,
    /// general DB
    wal: sled::Db,
    /// event storage
    events_tree: sled::Tree,
    /// state storage (written, etc)
    state_tree: sled::Tree,
    /// Next read index
    read: Idx,
    /// The last
    confirmed: Idx,
    /// The configuration
    config: Config,
    /// Are we currently in a broken CB state
    broken: bool,
    /// Did we signal because we're full
    full: bool,
    /// ID of this operator
    origin_uri: Option<EventOriginUri>,
}

op!(WalFactory(node) {
    if let Some(map) = &node.config {
        let config: Config = Config::new(map)?;

        let wal = if let Some(dir) = &config.dir {
            sled::open(&dir)?
        } else {
            sled::Config::default().temporary(true).open()?
        };
        let events_tree = wal.open_tree("events")?;
        let state_tree = wal.open_tree("state")?;

        #[allow(clippy::cast_possible_truncation)]
        let read = state_tree.get("read")?.map(Idx::from).unwrap_or_default();
        Ok(Box::new(WAL{
            cnt: events_tree.len() as u64,
            wal,
            read,
            confirmed: read,
            events_tree,
            state_tree,
            config,
            broken: true,
            full: false,
            origin_uri: Some(EventOriginUri {
                uid: 0,
                scheme: "tremor-wal".to_string(),
                host: "pipeline".to_string(),
                port: None,
                path: vec![node.id.to_string()],
            })
        }))
    } else {
        Err(ErrorKind::MissingOpConfig(node.id.to_string()).into())
    }
});

impl WAL {
    fn limit_reached(&self) -> Result<bool> {
        Ok(self.cnt >= self.config.max_elements
            || self.wal.size_on_disk()? >= self.config.max_bytes)
    }

    fn read_events(&mut self, _now: u64) -> Result<Vec<(Cow<'static, str>, Event)>> {
        // The maximum number of entries we read
        let mut events = Vec::with_capacity(self.config.read_count as usize);

        let mut num_read = 0usize;
        for e in self.events_tree.range(self.read..) {
            num_read += 1;
            if num_read > self.config.read_count {
                break;
            }
            let (idx, mut e) = e?;
            self.read = idx.into();
            self.read += 1;
            let e_slice: &mut [u8] = &mut e;
            let mut event = Event::from_slice(e_slice)?;
            event.transactional = true;
            events.push((OUT, event))
        }
        self.gc()?;
        Ok(events)
    }

    fn store_event(&mut self, uid: u64, mut event: Event) -> Result<()> {
        let id = self.wal.generate_id()?;
        let write: [u8; 8] = unsafe { mem::transmute(id.to_be()) };
        event.id.add_id(uid, id); // = Ids::new(uid, id);

        // Sieralize and write the event
        let event_buf = event.json_vec()?;
        self.events_tree.insert(write, event_buf.as_slice())?;
        if self.config.flush_on_evnt.unwrap_or_default() {
            self.events_tree.flush()?;
        }
        self.cnt += 1;
        Ok(())
    }

    fn gc(&mut self) -> Result<u64> {
        let mut i = 0;
        for e in self.events_tree.range(..self.confirmed) {
            i += 1;
            self.cnt -= 1;
            let (idx, _) = e?;
            self.events_tree.remove(idx)?;
        }
        Ok(i)
    }
}

fn maybe_parse_ivec(e: Option<IVec>) -> Option<Event> {
    let e_slice: &mut [u8] = &mut e?;
    Event::from_slice(e_slice).ok()
}

impl Operator for WAL {
    fn handles_contraflow(&self) -> bool {
        true
    }
    fn on_contraflow(&mut self, u_id: u64, insight: &mut Event) {
        match insight.cb {
            CBAction::None => {}
            CBAction::Open => self.broken = false,
            CBAction::Close => self.broken = true,
            CBAction::Ack => {
                let c_id = if let Some(c_id) = insight.id.get(u_id) {
                    c_id
                } else {
                    // This is not for us
                    return;
                };
                self.confirmed.set(c_id);
                if let Err(e) = self.state_tree.insert("read", self.confirmed) {
                    error!("Failed to persist confirm state: {}", e);
                }
                if let Some(e) = self
                    .events_tree
                    .get(self.confirmed)
                    .ok()
                    .and_then(maybe_parse_ivec)
                {
                    debug!("WAL confirm: {}", c_id);
                    insight.id.merge(&e.id);
                }
            }
            CBAction::Fail => {
                let f_id = if let Some(f_id) = insight.id.get(u_id) {
                    f_id
                } else {
                    // This is not for us
                    return;
                };
                self.read.set_min(f_id);

                if let Some(e) = self
                    .events_tree
                    .get(self.confirmed)
                    .ok()
                    .and_then(maybe_parse_ivec)
                {
                    insight.id.merge(&e.id);
                }

                let c = u64::from(self.confirmed);
                if f_id < c {
                    error!(
                        "trying to fail a message({}) that was already confirmed({})",
                        f_id, c
                    );
                    self.confirmed.set(f_id);
                    if let Err(e) = self.state_tree.insert("read", self.confirmed) {
                        error!("Failed to persist confirm state: {}", e);
                    }
                }
            }
        }
        insight.cb = CBAction::None;
    }

    fn handles_signal(&self) -> bool {
        true
    }
    fn on_signal(&mut self, _uid: u64, signal: &mut Event) -> Result<EventAndInsights> {
        let now = signal.ingest_ns;
        // Are we currently full
        let now_full = self.limit_reached()?;
        // If we just became full or we went from full to non full
        // update the CB status
        let insights = if self.full && !now_full {
            let mut e = Event::cb_restore(signal.ingest_ns);
            e.origin_uri = self.origin_uri.clone();
            vec![e]
        } else if !self.full && now_full {
            let mut e = Event::cb_trigger(signal.ingest_ns);
            e.origin_uri = self.origin_uri.clone();
            vec![e]
        } else {
            vec![]
        };
        self.full = now_full;
        let events = if self.broken {
            vec![]
        } else {
            self.read_events(now)?
        };
        Ok(EventAndInsights { insights, events })
    }

    fn on_event(
        &mut self,
        uid: u64,
        _port: &str,
        _state: &mut Value<'static>,
        event: Event,
    ) -> Result<EventAndInsights> {
        let id = event.id.clone();
        let now = event.ingest_ns;
        let transactional = event.transactional;

        self.store_event(uid, event)?;

        let insights = if transactional {
            vec![Event::cb_ack(now, id)]
        } else {
            vec![]
        };
        let events = if self.broken {
            Vec::new()
        } else {
            self.read_events(now)?
        };
        Ok(EventAndInsights { insights, events })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn rw() -> Result<()> {
        let c = Config {
            read_count: 100,
            dir: None,
            max_elements: 10,
            max_bytes: 1024 * 1024,
            flush_on_evnt: None,
        };
        let mut o = WalFactory::new().from_node(&NodeConfig::from_config("test", c)?)?;
        let mut v = Value::null();
        let e = Event::default();

        // The operator start in broken status

        // Send a first event
        let r = o.on_event(0, "in", &mut v, e.clone())?;
        // Since we are broken we should get nothing back
        assert_eq!(r.len(), 0);

        // Restore the CB
        let mut i = Event::cb_restore(0);
        o.on_contraflow(0, &mut i);

        // Send a second event
        let r = o.on_event(0, "in", &mut v, e.clone())?;
        // Since we are restored we now get 2 events (1 and 2)
        assert_eq!(r.len(), 2);

        // Send a fail event beck to 1, this tell the WAL that delivery of
        // 2 failed and they need to be delivered again
        let mut i = Event::default();
        i.id = 1.into();
        i.cb = CBAction::Fail;
        o.on_contraflow(0, &mut i);

        // Send a second event
        let r = o.on_event(0, "in", &mut v, e.clone())?;
        // since we failed before we should see 2 events, 3 and the retransmit
        // of 2
        assert_eq!(r.len(), 2);

        // Send a fail event beck to 0, this tell the WAL that delivery of
        // 1, 2, 3 failed and they need to be delivered again
        let mut i = Event::default();
        i.id = 0.into();

        i.cb = CBAction::Fail;
        o.on_contraflow(0, &mut i);

        // Send a second event
        let r = o.on_event(0, "in", &mut v, e.clone())?;
        // since we failed before we should see 4 events, 4 and the retransmit
        // of 1-3
        assert_eq!(r.len(), 4);

        Ok(())
    }

    #[test]
    // tests that the wal works fine
    // after a restart of the tremor server
    fn restart_wal_regression() -> Result<()> {
        let temp_dir = TempDirBuilder::new()
            .prefix("tremor-pipeline-wal")
            .tempdir()?;
        let read_count = 100;
        let c = Config {
            read_count,
            dir: Some(temp_dir.path().to_string_lossy().into_owned()),
            max_elements: 10,
            max_bytes: 1024 * 1024,
            flush_on_evnt: None,
        };

        let mut v = Value::null();
        let e = Event::default();

        {
            // create the operator - first time
            let mut o1 =
                WalFactory::new().from_node(&NodeConfig::from_config("wal-test-1", c.clone())?)?;

            // Restore the CB
            let mut i = Event::cb_restore(0);
            o1.on_contraflow(0, &mut i);

            // send a first event - not acked. so it lingers around in our WAL
            let r = o1.on_event(0, "in", &mut v, e.clone())?;
            assert_eq!(r.events.len(), 1);
            assert_eq!(r.insights.len(), 0);
        }

        {
            // create the operator - second time
            // simulating a tremor restart
            let mut o2 = WalFactory::new().from_node(&NodeConfig::from_config("wal-test-2", c)?)?;

            // Restore the CB
            let mut i = Event::cb_restore(1);
            o2.on_contraflow(0, &mut i);

            // send a first event - not acked. so it lingers around in our
            let r = o2.on_event(0, "in", &mut v, e.clone())?;
            assert_eq!(r.events.len(), 2);
            let id1 = &r.events[0].1.id;
            let id2 = &r.events[1].1.id;
            assert_eq!(id1.get(0).unwrap(), 0);
            // ensure we actually had a gap bigger than read count, which triggers the error condition
            assert!(id2.get(0).unwrap() - id1.get(0).unwrap() > read_count as u64);
            assert_eq!(r.insights.len(), 0);

            let r = o2.on_event(0, "in", &mut v, e.clone())?;
            assert_eq!(r.events.len(), 1);
        }

        Ok(())
    }
}
