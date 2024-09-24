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

//! Splits the data using [GELF chunking protocol](https://docs.graylog.org/en/3.0/pages/gelf.html#chunking).

use super::Postprocessor;
use std::time::{SystemTime, UNIX_EPOCH};
use rand::{thread_rng,Rng};

#[derive(Clone)]
pub struct Gelf {
    id: u64,
    chunk_size: usize,
}

impl Default for Gelf {
    fn default() -> Self {
        Self {
            id: 0,
            chunk_size: 8192,
        }
    }
}

/// Gelf Error
#[derive(Debug, thiserror::Error)]

enum Error {
    /// Error
    #[error("[GELF encoder] Maximum number of chunks is 128 this package would cause {0} chunks.")]
    ChunkCount(usize),
}

impl Gelf {
    // We cut i and n to u8 but check that n <= 128 before so it is safe.
    #[allow(clippy::cast_possible_truncation)]
    fn encode_gelf(&mut self, data: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
        let chunks = data.chunks(self.chunk_size - 12);
        let n = chunks.len();
        let id = self.id;
        if n > 128 {
            return Err(Error::ChunkCount(n));
        };


        /*
         * REFERENCE(/Explaination) TAKEN FROM: https://github.com/osiegmar/logback-gelf/blob/master/src/main/java/de/siegmar/logbackgelf/MessageIdSupplier.java#L61
         * 
         * 
         * Idea is borrowed from logstash-gelf by <a href="https://github.com/mp911de">Mark Paluch</a>, MIT licensed
         * <a href="https://github.com/mp911de/logstash-gelf/blob/a938063de1f822c8d26c8d51ed3871db24355017/src/main/java/biz/paluch/logging/gelf/intern/GelfMessage.java">GelfMessage.java</a>
         *
         * Considerations about generating the message ID: The GELF documentation suggests to
         * "Generate from millisecond timestamp + hostname, for example.":
         * https://go2docs.graylog.org/5-1/getting_in_log_data/gelf.html#GELFviaUDP
         *
         * However, relying on current time in milliseconds on the same system will result in a high collision
         * probability if lots of messages are generated quickly. Things will be even worse if multiple servers send
         * to the same log server. Adding the hostname is not guaranteed to help, and if the hostname is the FQDN it
         * is even unlikely to be unique at all.
         *
         * The GELF module used by Logstash uses the first eight bytes of an MD5 hash of the current time as floating
         * point, a hyphen, and an eight byte random number: https://github.com/logstash-plugins/logstash-output-gelf
         * https://github.com/graylog-labs/gelf-rb/blob/master/lib/gelf/notifier.rb#L239 It probably doesn't have to
         * be that clever:
         *
         * Using the timestamp plus a random number will mean we only have to worry about collision of random numbers
         * within the same milliseconds. How short can the timestamp be before it will collide with old timestamps?
         * Every second Graylog will evict expired messaged (5 seconds old) from the pool:
         * https://github.com/Graylog2/graylog2-server/blob/master/graylog2-server/src/main/java/org/graylog2/inputs/codecs/
         * GelfChunkAggregator.java Thus, we just need six seconds which will require 13 bits.
         * Then we can spend the rest on a random number.
         */


        const BITS_13: u64 = 0b1_1111_1111_1111;

        let message_id_current_epoch_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;

        let message_id_random_number = thread_rng().gen::<u64>();

        self.id = (message_id_current_epoch_time & BITS_13) | (message_id_random_number & !BITS_13);

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

impl Postprocessor for Gelf {
    fn is_streaming(&self) -> bool {
        false
    }
    fn name(&self) -> &str {
        "gelf"
    }

    fn process(
        &mut self,
        _ingest_ns: u64,
        _egress_ns: u64,
        data: &[u8],
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        Ok(self.encode_gelf(data)?)
    }

    fn finish(&mut self, data: Option<&[u8]>) -> anyhow::Result<Vec<Vec<u8>>> {
        if let Some(data) = data {
            Ok(self.encode_gelf(data)?)
        } else {
            Ok(vec![])
        }
    }
}

#[cfg(test)]
mod test {
    use crate::postprocessor::Postprocessor;
    use crate::preprocessor::{self as pre, prelude::*};

    #[test]
    fn is_streaming() {
        let gelf = super::Gelf::default();
        assert!(!gelf.is_streaming());
    }

    #[test]
    fn name() {
        let gelf = super::Gelf::default();
        assert_eq!(gelf.name(), "gelf");
    }

    #[test]
    fn simple_encode_decode() -> anyhow::Result<()> {
        let mut ingest_ns = 0;
        let egest_ns = 0;
        let input_data = vec![
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        ];
        let mut encoder = super::Gelf {
            id: 0,
            chunk_size: 20,
        };

        let mut decoder = pre::gelf_chunking::GelfChunking::default();

        let encoded_data = encoder.process(ingest_ns, egest_ns, &input_data)?;
        assert_eq!(encoded_data.len(), 3);

        assert!(decoder
            .process(&mut ingest_ns, &encoded_data[0], Value::const_null())?
            .is_empty());
        assert!(decoder
            .process(&mut ingest_ns, &encoded_data[1], Value::const_null())?
            .is_empty());
        let r = decoder.process(&mut ingest_ns, &encoded_data[2], Value::const_null())?;
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].0, input_data);
        Ok(())
    }
}
