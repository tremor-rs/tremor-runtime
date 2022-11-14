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

//! Chunking postprocessor that concatenates chunks of data, until its `max_bytes` is reached,
//! then it releases all the data as one chunk (`Vec<u8>`).

use super::Postprocessor;
use crate::errors::{Error, Kind as ErrorKind, Result};
use tremor_script::Value;

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

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        if (self.chunk.len() + data.len()) > self.max_bytes {
            if data.len() > self.max_bytes {
                self.warn(data.len());
                Ok(vec![])
            } else {
                let mut output = Vec::with_capacity(self.max_bytes);
                // append the new data to the buffer we do keep
                output.extend_from_slice(data);
                std::mem::swap(&mut output, &mut self.chunk);
                Ok(vec![output])
            }
        } else {
            self.chunk.extend_from_slice(data);
            Ok(vec![])
        }
    }

    fn finish(&mut self, data: Option<&[u8]>) -> Result<Vec<Vec<u8>>> {
        let mut output = vec![];
        std::mem::swap(&mut output, &mut self.chunk);

        let res = if let Some(data) = data {
            if data.len() + output.len() > self.max_bytes {
                if data.len() > self.max_bytes {
                    self.warn(data.len());
                    vec![output]
                } else {
                    vec![output, data.to_vec()]
                }
            } else {
                output.extend_from_slice(data);
                vec![output]
            }
        } else {
            vec![output]
        };
        Ok(res)
    }
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
    pub(crate) fn from_config(config: Option<&Value>) -> Result<Self> {
        if let Some(config) = config {
            let config: Config = tremor_value::structurize(config.clone()).map_err(|e| {
                let kind = ErrorKind::InvalidConfiguration(
                    "\"chunk\" postprocessor".to_string(),
                    e.to_string(),
                );
                Error::with_chain(e, kind)
            })?;
            if config.max_bytes == 0 {
                return Err(ErrorKind::InvalidConfiguration(
                    "\"chunk\" postprocessor".to_string(),
                    "`max_bytes` must be > 0".to_string(),
                )
                .into());
            }
            Ok(Chunk::new(config.max_bytes))
        } else {
            Err(ErrorKind::MissingConfiguration("\"chunk\" postprocessor".to_string()).into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use proptest::{collection, num, option};

    proptest! {

        #[test]
        fn test_chunk(input in collection::vec(collection::vec(num::u8::ANY, 1usize..1000), 1usize..100), last in option::of(collection::vec(num::u8::ANY, 1usize..1000))) {
            let max_bytes = 100;
            let mut pp = Chunk::new(max_bytes);
            let mut acc_size: usize = 0;
            for (len, chunk) in input.into_iter().map(|v| (v.len(), v)) {
                let res = pp.process(0, 0, &chunk).expect("chunk.process shouldn't fail");
                if len > max_bytes {
                    assert!(res.is_empty());
                } else if acc_size + len > max_bytes {
                    assert_eq!(1, res.len());
                    assert!(res[0].len() <= max_bytes, "a chunk exceeded max_bytes={max_bytes}");
                    acc_size = len;
                } else {
                    assert!(res.is_empty());
                    acc_size += len;
                }
            }
            let last_len = last.as_ref().map(Vec::len).unwrap_or_default();
            let o_slice: Option<&[u8]> = last.as_ref().map(Vec::as_ref);
            let res = pp.finish(o_slice).expect("chunk.finish shouldn;t fail");
            if acc_size > 0 {
                if last_len > max_bytes {
                    assert_eq!(1, res.len());
                    assert!(res[0].len() == acc_size);
                    assert!(res[0].len() <= max_bytes);
                } else if acc_size + last_len > max_bytes {
                    assert_eq!(2, res.len());
                    assert!(res[0].len() == acc_size);
                    assert!(res[0].len() <= max_bytes);
                    assert!(res[1].len() <= max_bytes);
                } else {
                    assert_eq!(1, res.len());
                    assert!(res[0].len() == acc_size + last_len);
                    assert!(res[0].len() <= max_bytes);
                }

            } else {
                assert_eq!(1, res.len());
                assert!(res[0].len() <= max_bytes, "the last chunk exceeded max_bytes={max_bytes}");
            }

        }
    }
}
