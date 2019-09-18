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

use super::Preprocessor;
use crate::errors::*;
use hashbrown::{hash_map::Entry, HashMap};
use rand::{self, RngCore};
use std::any::Any;

const FIVE_SEC: u64 = 5_000_000_000;

#[derive(Clone)]
pub struct GELF {
    buffer: HashMap<u64, GELFMsgs>,
    last_buffer: HashMap<u64, GELFMsgs>,
    last_swap: u64,
    cnt: usize,
    is_tcp: bool,
}

impl GELF {
    pub fn default() -> Self {
        GELF {
            buffer: HashMap::new(),
            last_buffer: HashMap::new(),
            last_swap: 0,
            cnt: 0,
            is_tcp: false,
        }
    }
    pub fn tcp() -> Self {
        GELF {
            buffer: HashMap::new(),
            last_buffer: HashMap::new(),
            last_swap: 0,
            cnt: 0,
            is_tcp: true,
        }
    }
}

#[derive(Clone, Default)]
struct GELFMsgs {
    count: u8,
    stored: u8,
    bytes: usize,
    segments: Vec<Option<Vec<u8>>>,
}

#[derive(Clone, Default)]
struct GELFSegment {
    id: u64,
    seq: u8,
    count: u8,
    data: Vec<u8>,
}

fn decode_gelf(bin: &[u8]) -> Result<GELFSegment> {
    // We got to do that for badly compressed / non standard conform
    // gelf messages
    match bin.get(0..2) {
        // WELF magic header - wayfair uncompressed gelf
        Some(&[0x1f, 0x3c]) => {
            // If we are less then 2 byte we can not be a proper Package
            if bin.len() < 2 {
                Err(ErrorKind::InvalidGELFHeader(bin.len(), None).into())
            } else {
                // FIXME: we would allow up to 255 chunks
                Ok(GELFSegment {
                    id: rand::rngs::OsRng.next_u64(),
                    seq: 0,
                    count: 1,
                    data: bin[2..].to_vec(),
                })
            }
        }
        // GELF magic header
        Some(&[0x1e, 0x0f]) => {
            // If we are less then 12 byte we can not be a proper Package
            if bin.len() < 12 {
                if bin.len() >= 2 {
                    Err(ErrorKind::InvalidGELFHeader(bin.len(), Some([bin[0], bin[1]])).into())
                } else {
                    Err(ErrorKind::InvalidGELFHeader(bin.len(), None).into())
                }
            } else {
                // FIXME: we would allow up to 255 chunks
                Ok(GELFSegment {
                    id: (u64::from(bin[2]) << 56)
                        + (u64::from(bin[3]) << 48)
                        + (u64::from(bin[4]) << 40)
                        + (u64::from(bin[5]) << 32)
                        + (u64::from(bin[6]) << 24)
                        + (u64::from(bin[7]) << 16)
                        + (u64::from(bin[8]) << 8)
                        + u64::from(bin[9]),
                    seq: bin[10],
                    count: bin[11],
                    data: bin[12..].to_vec(), // FIXME: can we skip that
                })
            }
        }
        Some(&[b'{', _]) => Ok(GELFSegment {
            id: 0,
            seq: 0,
            count: 1,
            data: bin.to_vec(),
        }),
        _ => Err(ErrorKind::InvalidGELFHeader(bin.len(), Some([bin[0], bin[1]])).into()),
    }
}

impl Preprocessor for GELF {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn process(&mut self, ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let msg = decode_gelf(data)?;
        if let Some(data) = self.enqueue(*ingest_ns, msg) {
            let _len = if self.is_tcp {
                data.len() - 1
            } else {
                data.len()
            };
            let len = if self.is_tcp {
                data.len() - 1
            } else {
                data.len()
            };
            Ok(vec![data[0..len].to_vec()])
        } else {
            Ok(vec![])
        }
    }
}

impl GELF {
    fn enqueue(&mut self, ingest_ns: u64, msg: GELFSegment) -> Option<Vec<u8>> {
        // By sepc all incomplete chunks need to be destroyed after 5 seconds
        if ingest_ns - self.last_swap > FIVE_SEC {
            // clear the last buffer and swap current and last.
            self.last_swap = ingest_ns;
            self.last_buffer.clear();
            std::mem::swap(&mut self.last_buffer, &mut self.buffer);
        }
        if msg.count == 1 {
            return Some(msg.data);
        }
        let idx = msg.seq as usize;
        let key = msg.id;
        match self.buffer.entry(key) {
            Entry::Vacant(v) => match self.last_buffer.entry(key) {
                Entry::Occupied(mut o) => {
                    let m = o.get_mut();
                    if let Some(None) = m.segments.get(idx) {
                        if let Some(d) = m.segments.get_mut(idx) {
                            m.bytes += msg.data.len();
                            m.stored += 1;
                            *d = Some(msg.data);
                        } else {
                            warn!(
                                "Discarding out of range chunk {}/{} for {} ({} bytes)",
                                idx,
                                m.count,
                                key,
                                msg.data.len()
                            );
                            return None;
                        };
                        if m.stored == m.count {
                            let m = o.remove();
                            assemble(key, m)
                        } else {
                            None
                        }
                    } else {
                        error!("Duplicate index {} for gelf message id {}", idx, o.key());
                        o.remove();
                        None
                    }
                }
                Entry::Vacant(_v_last) => {
                    let mut m = GELFMsgs {
                        bytes: msg.data.len(),
                        count: msg.count,
                        stored: 1,
                        segments: vec![None; msg.count as usize],
                    };
                    if let Some(d) = m.segments.get_mut(idx) {
                        *d = Some(msg.data);
                    } else {
                        warn!(
                            "Discarding out of range chunk {}/{} for {} ({} bytes)",
                            idx,
                            m.count,
                            key,
                            msg.data.len()
                        );
                        return None;
                    };
                    if m.stored == m.count {
                        assemble(key, m)
                    } else {
                        v.insert(m);
                        None
                    }
                }
            },
            Entry::Occupied(mut o) => {
                let m = o.get_mut();
                if let Some(None) = m.segments.get(idx) {
                    if let Some(d) = m.segments.get_mut(idx) {
                        m.bytes += msg.data.len();
                        m.stored += 1;
                        *d = Some(msg.data);
                    } else {
                        warn!(
                            "Discarding out of range chunk {}/{} for {} ({} bytes)",
                            idx,
                            m.count,
                            key,
                            msg.data.len()
                        );
                        return None;
                    };
                    if m.stored == m.count {
                        let m = o.remove();
                        assemble(key, m)
                    } else {
                        None
                    }
                } else {
                    error!("Duplicate index {} for gelf message id {}", idx, o.key());
                    o.remove();
                    None
                }
            }
        }
    }
}

fn assemble(key: u64, m: GELFMsgs) -> Option<Vec<u8>> {
    let mut result = Vec::with_capacity(m.bytes);
    for v in m.segments.into_iter() {
        if let Some(mut v) = v {
            result.append(&mut v)
        } else {
            error!("Missing segment in GELF chunks for {}", key);
            return None;
        }
    }
    Some(result)
}
