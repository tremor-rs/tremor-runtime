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

use crate::op::prelude::*;
use byteorder::{BigEndian, ReadBytesExt};
use sled::Transactional;
use std::io::Cursor;
use std::mem;
use tremor_script::prelude::*;

const OUT: Cow<'static, str> = Cow::Borrowed("out");

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Maximum number of events to read per tick/event when filling
    /// up from the persistant storage
    pub read_count: usize,

    /// The directory to store data in
    pub dir: String,

    /// The maximum elements to store before breaking the circuit
    pub max_elements: usize,
}

impl ConfigImpl for Config {}

#[derive(Debug, Clone)]
// TODO add seed value and field name as config items
pub struct WAL {
    cnt: usize,
    wal: sled::Db,
    events_tree: sled::Tree,
    state_tree: sled::Tree,
    write: usize,
    config: Config,
    broken: bool,
}

op!(WalFactory(node) {
    if let Some(map) = &node.config {
        let config: Config = Config::new(map)?;

        let wal = sled::open(&config.dir)?;
        let events_tree = wal.open_tree("events")?;
        let state_tree = wal.open_tree("state")?;

        let write = state_tree.get("write")?.and_then(|v| {let mut rdr = Cursor::new(&v); rdr.read_u64::<BigEndian>().ok().map(|v| v as usize) } ).unwrap_or(0);
        dbg!(write);
        Ok(Box::new(WAL{
            cnt: events_tree.len(),
            wal,
            write,
            events_tree,
            state_tree,
            config,
            broken: false
        }))
    } else {
        Err(ErrorKind::MissingOpConfig(node.id.to_string()).into())
    }
});

impl WAL {
    fn read_events(&mut self) -> Result<Vec<(Cow<'static, str>, Event)>> {
        // The maximum number of entries we read
        let mut events = Vec::with_capacity(self.config.read_count);

        for _ in 0..self.config.read_count {
            if let Some((_, e)) = self.events_tree.pop_min()? {
                self.cnt -= 1;
                let e_slice: &[u8] = &e;
                let mut ev = Vec::from(e_slice);
                let event = simd_json::from_slice(&mut ev)?;
                events.push((OUT, event))
            } else {
                break;
            }
        }

        Ok(events)
    }
}
#[allow(unused_mut)]
impl Operator for WAL {
    fn handles_contraflow(&self) -> bool {
        true
    }
    fn on_contraflow(&mut self, insight: &mut Event) {
        if insight.cb == Some(CBAction::Restore) {
            self.broken = false;
        } else if insight.cb == Some(CBAction::Trigger) {
            self.broken = true;
        }
        insight.cb = None;
    }
    fn handles_signal(&self) -> bool {
        true
    }

    fn on_signal(&mut self, signal: &mut Event) -> Result<SignalResponse> {
        if self.config.max_elements < self.cnt {
            let e = Event {
                ingest_ns: signal.ingest_ns,
                cb: Some(CBAction::Trigger),
                ..std::default::Default::default()
            };
            dbg!("full", self.write);
            Ok((vec![], Some(e)))
        } else if self.broken {
            Ok((vec![], None))
        } else {
            Ok((self.read_events()?, None))
        }
    }

    fn on_event(
        &mut self,
        _port: &str,
        _state: &mut Value<'static>,
        event: Event,
    ) -> Result<Vec<(Cow<'static, str>, Event)>> {
        // Calculate the next Id to write to
        self.write += 1;
        let write_buf: [u8; 8] = unsafe { mem::transmute(self.write.to_be()) };

        // Sieralize and write the event
        let event_buf = simd_json::serde::to_vec(&event)?;
        (&self.events_tree, &self.state_tree).transaction(|(events_tree, state_tree)| {
            events_tree.insert(&write_buf, event_buf.as_slice())?;
            state_tree.insert("write", &write_buf)?;
            Ok(())
        })?;
        self.cnt += 1;

        if self.broken {
            Ok(vec![])
        } else {
            self.read_events()
        }
    }
}
