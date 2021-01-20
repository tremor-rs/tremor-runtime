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

use super::Postprocessor;
use crate::errors::Result;

#[derive(Clone)]
pub struct GELF {
    id: u64,
    chunk_size: usize,
}

impl Default for GELF {
    fn default() -> Self {
        Self {
            id: 0,
            chunk_size: 8192,
        }
    }
}

impl GELF {
    // We cut i and n to u8 but check that n <= 128 before so it is safe.
    #[allow(clippy::cast_possible_truncation)]
    fn encode_gelf(&mut self, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let chunks = data.chunks(self.chunk_size - 12);
        let n = chunks.len();
        let id = self.id;
        if n > 128 {
            return Err(format!(
            "[GELF encoder] Maximum number of chunks is 128 this package would cause {} chunks.",
            n
            )
            .into());
        };
        self.id += 1;
        Ok(chunks
            .enumerate()
            .map(|(i, chunk)| {
                let mut buf: Vec<u8> = Vec::with_capacity(chunk.len() + 12);
                // Serialize header

                // magic number
                buf.append(&mut vec![0x1e, 0x0f]);
                // gelf package id
                buf.append(&mut id.to_be_bytes().to_vec());
                // sequence number
                buf.push(i as u8);
                // sequence count
                buf.push(n as u8);

                // data
                buf.append(&mut chunk.to_vec());
                buf
            })
            .collect())
    }
}

impl Postprocessor for GELF {
    fn name(&self) -> &str {
        "gelf"
    }

    fn process(&mut self, _ingest_ns: u64, _egest_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        self.encode_gelf(data)
    }
}

#[cfg(test)]
mod test {
    use crate::errors::*;
    use crate::postprocessor::{self, Postprocessor};
    use crate::preprocessor::{self, Preprocessor};

    #[test]
    fn simple_encode_decode() -> Result<()> {
        let mut ingest_ns = 0;
        let egest_ns = 0;
        let input_data = vec![
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        ];
        let mut encoder = postprocessor::GELF {
            id: 0,
            chunk_size: 20,
        };

        let mut decoder = preprocessor::GELF::default();

        let encoded_data = encoder.process(ingest_ns, egest_ns, &input_data)?;
        assert_eq!(encoded_data.len(), 3);

        assert!(decoder
            .process(&mut ingest_ns, &encoded_data[0])?
            .is_empty());
        assert!(decoder
            .process(&mut ingest_ns, &encoded_data[1])?
            .is_empty());
        let r = decoder.process(&mut ingest_ns, &encoded_data[2])?;
        assert_eq!(r.len(), 1);
        assert_eq!(r[0], input_data);
        Ok(())
    }
}
