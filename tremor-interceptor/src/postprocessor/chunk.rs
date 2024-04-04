// Copyright 2022, The Tremor Team
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

//! A postprocessor for concatenating serialized input chunks into a buffer that is guaranteed to never exceed the configured `max_bytes` configuration,
//! but tries to come as close to that as possible while not splitting apart given input chunks.
//!
//! Input chunks are concatenated until concatenating another chunk would exceed the configured `max_bytes`, then the accumulated buffer is emitted as a single chunk and the new one is accumulated until `max_bytes` would be hit again.
//!
//! If an input chunk exceeds `max_bytes` it is discarded with a warning.
//!
//! ## Configuration
//!
//! | Option      | Description                                                                                                            | Required | Default Value |
//! |-------------|------------------------------------------------------------------------------------------------------------------------|----------|---------------|
//! | `max_bytes` | The maximum number of bytes an output chunk should never exceed. Output chunks can and will have less number of bytes. | yes      |               |
//!
//! ## Example
//!
//! Ensure a maximum UDP packet size for the [`udp_client` connector](../connectors/udp.md#client):
//!
//! ```tremor
//! define connector my_udp_client from udp_client
//! with
//!     codec = "json",
//!     postprocessors = [
//!         "separate",
//!         {
//!             "name": "chunk",
//!             "config": {
//!                 "max_bytes": 1432
//!             }
//!         }
//!     ]
//!
//! end;
//! ```

use super::Postprocessor;
use log::warn;
use serde::Deserialize;
use tremor_value::Value;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
struct Config {
    max_bytes: usize,
}

pub(crate) struct Chunk {
    max_bytes: usize,
    chunk: Vec<u8>,
}

impl Postprocessor for Chunk {
    fn name(&self) -> &str {
        "chunk"
    }

    fn process(
        &mut self,
        _ingres_ns: u64,
        _egress_ns: u64,
        data: &[u8],
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        let new_len = self.chunk.len() + data.len();
        if data.len() > self.max_bytes {
            // ignore incoming data
            self.warn(data.len());
            Ok(vec![])
        } else if new_len > self.max_bytes {
            // append the new data to the buffer we do keep and send the current buffer along
            let mut output = Vec::with_capacity(self.max_bytes);
            output.extend_from_slice(data);
            std::mem::swap(&mut output, &mut self.chunk);
            Ok(vec![output])
        } else if new_len == self.max_bytes {
            // append the new data to the buffer and send both along
            let mut output = Vec::with_capacity(self.max_bytes);
            std::mem::swap(&mut output, &mut self.chunk);
            output.extend_from_slice(data);
            Ok(vec![output])
        } else {
            // not close to max_bytes yet, not emitting anything
            self.chunk.extend_from_slice(data);
            Ok(vec![])
        }
    }

    fn finish(&mut self, data: Option<&[u8]>) -> anyhow::Result<Vec<Vec<u8>>> {
        let mut output = vec![];
        std::mem::swap(&mut output, &mut self.chunk);
        let data = data.unwrap_or_default();

        let res = match (output.len(), data.len()) {
            // both empty
            (0, 0) => vec![],
            // chunk empty, data is too big -> discard
            (0, dl) if dl > self.max_bytes => {
                self.warn(dl);
                vec![]
            }
            // chunk empty -> only data
            (0, _) => vec![data.to_vec()],
            // ignore data if empty or too big
            (_, 0) => vec![output],
            (_, dl) if dl > self.max_bytes => {
                self.warn(dl);
                vec![output]
            }
            // return two chunks if both together would be too big
            (ol, dl) if (ol + dl) > self.max_bytes => {
                vec![output, data.to_vec()]
            }
            // all within bounds, concatenate
            (_, _) => {
                output.extend_from_slice(data);
                vec![output]
            }
        };
        Ok(res)
    }
}

/// Chunk postprocessor error
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// MaxBytes is zero
    #[error("`max_bytes` must be > 0")]
    MaxBytesZero,
}
impl Chunk {
    fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            chunk: Vec::with_capacity(max_bytes),
        }
    }

    fn warn(&self, len: usize) {
        warn!(
            "[postprocessor::chunk] payload of size {} exceeded max_bytes {}. Discarding...",
            len, self.max_bytes
        );
    }
    pub(crate) fn from_config(config: Option<&Value>) -> anyhow::Result<Self, super::Error> {
        if let Some(config) = config {
            let config: Config = tremor_value::structurize(config.clone())
                .map_err(|e| super::Error::InvalidConfig("chunk", e.into()))?;
            if config.max_bytes == 0 {
                return Err(super::Error::InvalidConfig(
                    "chunk",
                    Error::MaxBytesZero.into(),
                ));
            }
            Ok(Chunk::new(config.max_bytes))
        } else {
            Err(super::Error::MissingConfig("chunk"))
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::ignored_unit_patterns)]
    use super::*;
    use proptest::prelude::*;
    use proptest::{collection, num, option};
    use tremor_value::literal;

    #[test]
    fn from_config() {
        let res = Chunk::from_config(Some(&literal!({
            "max_bytes": 0
        })));
        assert!(res.is_err());
        let res = Chunk::from_config(Some(&literal!([])));
        assert!(res.is_err());
        let res = Chunk::from_config(Some(&literal!({
            "max_bytes": 100,
            "another_field": [true]
        })));
        assert!(res.is_err());
        assert!(Chunk::from_config(None).is_err());
        let res = Chunk::from_config(Some(&literal!({
            "max_bytes": 1432
        })));
        assert!(res.is_ok());
        let chunk = res.expect("unreachable");
        assert_eq!("chunk", chunk.name());
        assert_eq!(1432, chunk.max_bytes);
    }

    #[test]
    fn emit_on_exact_max_bytes() {
        let mut pp = Chunk::new(100);
        let data = [0_u8; 100];
        let res = pp.process(0, 0, &data).expect("chunk.process should work");
        assert_eq!(vec![data.to_vec()], res);
    }

    #[test]
    fn discard_too_big() {
        let mut pp = Chunk::new(100);
        let data = [0_u8; 101];
        let res = pp.process(0, 0, &data).expect("chunk.process should work");
        assert!(res.is_empty());
        assert!(pp
            .finish(None)
            .expect("chunk.finish should work")
            .is_empty()); // nothing left
    }

    proptest! {

        #[test]
        fn test_chunk(
            input in collection::vec(collection::vec(num::u8::ANY, 0usize..1000), 0usize..100),
            last in option::of(collection::vec(num::u8::ANY, 0usize..1000))
        ) {
            let max_bytes = 100;
            let mut pp = Chunk::new(max_bytes);
            let mut acc_size: usize = 0;
            for (len, chunk) in input.into_iter().map(|v| (v.len(), v)) {
                let res = pp.process(0, 0, &chunk).expect("chunk.process shouldn't fail");

                assert!(!res.iter().any(Vec::is_empty), "we have some empty chunks in {res:?}");
                assert!(!res.iter().any(|v| v.len() > max_bytes), "we have some chunks exceeding max_bytes");

                if len > max_bytes {
                    // ignored
                    assert!(res.is_empty());
                } else if (acc_size + len) > max_bytes {
                    assert_eq!(1, res.len());
                    assert!(!res[0].is_empty());
                    assert!(res[0].len() <= max_bytes, "a chunk exceeded max_bytes={max_bytes}");
                    acc_size = len;
                } else if (acc_size + len) == max_bytes {
                    assert_eq!(1, res.len());
                    assert_eq!(max_bytes, res[0].len());
                    acc_size = 0;
                } else {
                    assert!(res.is_empty());
                    acc_size += len;
                }
            }
            let last_len = last.as_ref().map(Vec::len).unwrap_or_default();
            let o_slice: Option<&[u8]> = last.as_ref().map(Vec::as_ref);
            let res = pp.finish(o_slice).expect("chunk.finish shouldn;t fail");
            assert!(!res.iter().any(Vec::is_empty), "we have some empty chunks in {res:?}");
            assert!(!res.iter().any(|v| v.len() > max_bytes), "we have some chunks exceeding max_bytes");
            if acc_size > 0 {
                assert!(!res.is_empty());
            }
            if last_len > 0 && last_len <= max_bytes {
                assert!(!res.is_empty());
            }
        }
    }
}
