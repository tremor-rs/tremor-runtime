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
use std::any::Any;

#[derive(Clone)]
pub struct Lines {
    separator: char,
    max_length: usize,
    // to keep track of partial line reads
    buffer: Vec<u8>,
    message_fragment_length: usize, // TODO better naming/type
}

impl Lines {
    // TODO have the params here as a config struct
    pub fn new(separator: char, max_length: usize) -> Self {
        Lines {
            separator,
            max_length,
            buffer: vec![0; max_length],
            message_fragment_length: 0,
        }
    }
}

impl Preprocessor for Lines {
    fn as_any(&self) -> &dyn Any {
        self
    }

    // TODO implement main process here in functional style and compare with the current
    // imperative implementation
    /*
    fn process(&mut self, _ingest_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(data
            .split(|c| *c == self.separator as u8)
            .map(Vec::from)
            .collect())
    }
    */

    // TODO separate out some of the logic here in other functions for readability
    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let mut result: Vec<Vec<u8>> = Vec::new();
        let mut message_start_index = 0;

        for (i, c) in data.iter().enumerate() {
            if *c == self.separator as u8 {
                // doesn't take the separator index into account
                let mut message_length = i - message_start_index;

                // if there's a message fragment from earlier, account it for the total message length
                if self.message_fragment_length > 0 {
                    message_length += self.message_fragment_length;
                }

                if message_length > self.max_length {
                    warn!(
                        "Ignored message of length {} since it exceeds maximum allowed line length of {}",
                        message_length,
                        self.max_length
                    );
                } else if self.message_fragment_length > 0 {
                    trace!(
                        "Adding message fragment of length {} from processor buffer: {}",
                        self.message_fragment_length,
                        String::from_utf8_lossy(&self.buffer[0..self.message_fragment_length])
                    );
                    result.push(
                        [
                            &self.buffer[0..self.message_fragment_length],
                            &data[message_start_index..i],
                        ]
                        .concat(),
                    );

                    // clear the buffer now that we have used (not strictly necessary though...)
                    for i in &mut self.buffer[0..self.message_fragment_length] {
                        *i = 0;
                    }
                } else {
                    result.push(data[message_start_index..i].to_vec());
                }

                // reset the start index for the message, as well as the fragment length
                message_start_index = i + 1;
                self.message_fragment_length = 0;
            }

            // check if there's residual data left
            if i == data.len() - 1 && message_start_index != i + 1 {
                let total_fragment_length =
                    self.message_fragment_length + (i - message_start_index + 1);

                if total_fragment_length <= self.max_length {
                    // for resuming message reads
                    self.buffer[self.message_fragment_length..total_fragment_length]
                        .copy_from_slice(&data[message_start_index..=i]);

                    trace!(
                        "Saved message fragment of length {} to processor buffer: {}",
                        total_fragment_length - self.message_fragment_length,
                        String::from_utf8_lossy(
                            &self.buffer[self.message_fragment_length..total_fragment_length]
                        )
                    );

                    self.message_fragment_length = total_fragment_length;
                } else {
                    warn!(
                        "Discarded message fragment of length {} since it exceeds maximum allowed line length of {}",
                        self.message_fragment_length,
                        self.max_length
                    );
                }
            }
        }

        // TODO remove later?
        trace!(
            "Processor buffer: {}",
            String::from_utf8_lossy(&self.buffer)
        );

        Ok(result)
    }
}
