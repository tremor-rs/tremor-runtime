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

use super::Preprocessor;
use crate::errors::{ErrorKind, Result};
use hashbrown::{hash_map::Entry, HashMap};
use rand::{self, RngCore};

const FIVE_SEC: u64 = 5_000_000_000;

#[derive(Clone)]
pub struct Gelf {
    buffer: HashMap<u64, GelfMsgs>,
    last_buffer: HashMap<u64, GelfMsgs>,
    last_swap: u64,
    cnt: usize,
    is_tcp: bool,
}

impl Gelf {
    pub fn default() -> Self {
        Self {
            buffer: HashMap::new(),
            last_buffer: HashMap::new(),
            last_swap: 0,
            cnt: 0,
            is_tcp: false,
        }
    }
    pub fn tcp() -> Self {
        Self {
            buffer: HashMap::new(),
            last_buffer: HashMap::new(),
            last_swap: 0,
            cnt: 0,
            is_tcp: true,
        }
    }
}

#[derive(Clone, Default)]
struct GelfMsgs {
    count: u8,
    stored: u8,
    bytes: usize,
    segments: Vec<Option<Vec<u8>>>,
}

#[derive(Clone, Default)]
struct GelfSegment {
    id: u64,
    seq: u8,
    count: u8,
    data: Vec<u8>,
}

fn decode_gelf(bin: &[u8]) -> Result<GelfSegment> {
    // We got to do that for badly compressed / non standard conform
    // gelf messages
    match *bin {
        // WELF magic header - Wayfair uncompressed GELF
        [0x1f, 0x3c, ref rest @ ..] => {
            // we would allow up to 255 chunks
            Ok(GelfSegment {
                id: rand::rngs::OsRng.next_u64(),
                seq: 0,
                count: 1,
                data: rest.to_vec(),
            })
        }

        // GELF magic header
        [0x1e, 0x0f, b2, b3, b4, b5, b6, b7, b8, b9, seq, count, ref rest @ ..] => {
            // If we are less then 12 byte we can not be a proper Package
            // we would allow up to 255 chunks
            let id = (u64::from(b2) << 56)
                + (u64::from(b3) << 48)
                + (u64::from(b4) << 40)
                + (u64::from(b5) << 32)
                + (u64::from(b6) << 24)
                + (u64::from(b7) << 16)
                + (u64::from(b8) << 8)
                + u64::from(b9);
            Ok(GelfSegment {
                id,
                seq,
                count,
                data: rest.to_vec(),
            })
        }
        [b'{', _, ..] => Ok(GelfSegment {
            id: 0,
            seq: 0,
            count: 1,
            data: bin.to_vec(),
        }),
        [a, b, ..] => Err(ErrorKind::InvalidGelfHeader(bin.len(), Some([a, b])).into()),
        [v] => Err(ErrorKind::InvalidGelfHeader(1, Some([v, 0])).into()),
        [] => Err(ErrorKind::InvalidGelfHeader(0, None).into()),
    }
}

impl Preprocessor for Gelf {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "gelf"
    }

    fn process(&mut self, ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let msg = decode_gelf(data)?;
        if let Some(data) = self.enqueue(*ingest_ns, msg) {
            // TODO: WHY :sob:
            let len = if self.is_tcp {
                data.len() - 1
            } else {
                data.len()
            };
            if let Some(d) = data.get(0..len) {
                Ok(vec![d.to_vec()])
            } else {
                Ok(vec![])
            }
        } else {
            Ok(vec![])
        }
    }
}

impl Gelf {
    fn enqueue(&mut self, ingest_ns: u64, msg: GelfSegment) -> Option<Vec<u8>> {
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
                    let mut m = GelfMsgs {
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

fn assemble(key: u64, m: GelfMsgs) -> Option<Vec<u8>> {
    let mut result = Vec::with_capacity(m.bytes);
    for v in m.segments {
        if let Some(mut v) = v {
            result.append(&mut v)
        } else {
            error!("Missing segment in GELF chunks for {}", key);
            return None;
        }
    }
    Some(result)
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn bad_len() {
        let d = Vec::new();
        assert!(decode_gelf(&d).is_err());
        let d = vec![b'{'];
        assert!(decode_gelf(&d).is_err());

        let d = vec![b'{', b'}'];
        assert!(decode_gelf(&d).is_ok());
        let d = br#"{"snot": "badger"}"#;
        assert!(decode_gelf(d).is_ok());
    }
}
