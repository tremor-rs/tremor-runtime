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

//! Reassembles messages that were split apart using the [GELF chunking protocol](https://docs.graylog.org/en/3.0/pages/gelf.html#chunking).
//!
//! ## How do I handle compressed GELF?
//!
//! Where GELF messages are compressed, say over UDP, and the chunks are themselves compressed we can
//! use decompression processors to transform the raw stream to the tremor value system by leveraging
//! processors in tremor in concert with the `json` codec.
//!
//! ```tremor
//! define connector example from udp_client
//! with
//!   codec = "json",
//!   preprocessors = [
//!     "decompress",               # Decompress stream using a supported decompression algorithm
//!     "gelf-chunking",            # Parse out gelf messages
//!     "decompress",               # Decompress chunk using a supported decompression algorithm
//!   ],
//!   config = {
//!     "url": "127.0.0.1:12201"    # We are a client to a remote UDP service
//!   }
//! end;
//! ```
//!
//! The same methodology with a tcp backed endpoint where we're listening as a server:
//!
//! ```tremor
//! define connector example from tcp_server
//! with
//!   codec = "json",
//!   preprocessors = [
//!     "decompress",               # Decompress stream using a supported decompression algorithm
//!     "gelf-chunking",            # Parse out gelf messages
//!     "decompress",               # Decompress chunk using a supported decompression algorithm
//!   ],
//!   config = {
//!     "url": "127.0.0.1:12201"    # We are acting as a TCP server on port 12201 bound over localhost
//!   }
//! end;
//! ```

use super::prelude::*;
use log::{error, warn};
use rand::RngCore;
use std::collections::{hash_map::Entry, HashMap};

const FIVE_SEC: u64 = 5_000_000_000;

#[derive(Clone, Default)]
pub(crate) struct GelfChunking {
    buffer: HashMap<u64, GelfMsgs>,
    last_buffer: HashMap<u64, GelfMsgs>,
    last_swap: u64,
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

/// Gelf Error
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid Gelf Header
    #[error("Invalid Gelf Header: {0} bytes, {1:?}")]
    InvalidGelfHeader(usize, Option<[u8; 2]>),
}

fn decode_gelf(bin: &[u8]) -> Result<GelfSegment, Error> {
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
        [a, b, ..] => Err(Error::InvalidGelfHeader(bin.len(), Some([a, b]))),
        [v] => Err(Error::InvalidGelfHeader(1, Some([v, 0]))),
        [] => Err(Error::InvalidGelfHeader(0, None)),
    }
}

impl Preprocessor for GelfChunking {
    fn name(&self) -> &str {
        "gelf"
    }

    fn process(
        &mut self,
        ingest_ns: &mut u64,
        data: &[u8],
        meta: Value<'static>,
    ) -> anyhow::Result<Vec<(Vec<u8>, Value<'static>)>> {
        let msg = decode_gelf(data)?;
        if let Some(data) = self.enqueue(*ingest_ns, msg) {
            Ok(vec![(data, meta)])
        } else {
            Ok(vec![])
        }
    }
}

impl GelfChunking {
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
                        let bytes = msg.data.len();
                        if let Some(d) = m.segments.get_mut(idx) {
                            m.bytes += bytes;
                            m.stored += 1;
                            *d = Some(msg.data);
                        } else {
                            warn!(
                                "Discarding out of range chunk {}/{} for {} ({} bytes)",
                                idx, m.count, key, bytes
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
                    let count = msg.count;
                    let bytes = msg.data.len();
                    let mut m = GelfMsgs {
                        bytes,
                        count,
                        stored: 1,
                        segments: vec![None; count as usize],
                    };
                    if let Some(d) = m.segments.get_mut(idx) {
                        *d = Some(msg.data);
                    } else {
                        warn!(
                            "Discarding out of range chunk {}/{} for {} ({} bytes)",
                            idx, count, key, bytes
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
                    let bytes = msg.data.len();
                    if let Some(d) = m.segments.get_mut(idx) {
                        m.bytes += bytes;
                        m.stored += 1;
                        *d = Some(msg.data);
                    } else {
                        warn!(
                            "Discarding out of range chunk {}/{} for {} ({} bytes)",
                            idx, m.count, key, bytes
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
            result.append(&mut v);
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
    fn gelf_chunking_default() -> anyhow::Result<()> {
        let g = GelfChunking::default();
        assert!(g.buffer.is_empty());
        assert!(g.last_buffer.is_empty());
        assert_eq!(0, g.last_swap);
        let mut g = g;
        assert!(g.buffer.is_empty());
        assert!(g.last_buffer.is_empty());
        assert_eq!(0, g.last_swap);
        let d = br#"{"snot": "badger"}"#;
        let r = g.process(&mut 0, d, Value::object())?;
        assert_eq!(r[0].0, d.to_vec());
        Ok(())
    }

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
