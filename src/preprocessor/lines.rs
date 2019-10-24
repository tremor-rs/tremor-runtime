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
use std::cmp::min;

#[derive(Clone)]
pub struct Lines {
    separator: char,
    max_length: usize,
    // to keep track of line fragments when partial data comes through
    fragment_length: usize,
    buffer: Vec<u8>,
}

impl Lines {
    // TODO have the params here as a config struct
    pub fn new(separator: char, max_length: usize) -> Self {
        Lines {
            separator,
            max_length,
            fragment_length: 0,
            // allocating at once with enough capacity to ensure we don't do re-allocations
            // optimizing for performance here instead of memory usage
            buffer: Vec::with_capacity(max_length),
        }
    }

    fn is_valid_line(&self, v: &[u8]) -> bool {
        if v.len() <= self.max_length {
            true
        } else {
            warn!(
                "Invalid line of length {} since it exceeds maximum allowed length of {}: {:?}",
                v.len(),
                self.max_length,
                String::from_utf8_lossy(&v[0..min(v.len(), 256)]),
            );
            false
        }
    }

    fn save_fragment(&mut self, v: &[u8]) {
        let total_fragment_length = self.fragment_length + v.len();

        if total_fragment_length <= self.max_length {
            self.buffer.extend_from_slice(v);
            trace!(
                "Saved line fragment of length {} to preprocessor buffer: {:?}",
                v.len(),
                String::from_utf8_lossy(v)
            );
        } else {
            warn!(
                "Discarded line fragment of length {} since total length of {} exceeds maximum allowed length of {}: {:?}",
                v.len(),
                total_fragment_length,
                self.max_length,
                String::from_utf8_lossy(v)
            );
            // since we are not saving the current fragment, anything that was saved earlier is
            // useless now so clear the buffer
            self.buffer.clear();
        }

        // remember the total fragment length for later use
        self.fragment_length = total_fragment_length;
    }

    fn complete_fragment(&mut self, v: &mut Vec<u8>) {
        let total_fragment_length = self.fragment_length + v.len();

        if total_fragment_length <= self.max_length {
            // prepend v with buffer content
            // extend the buffer first (we know it has enough capacity to hold v without any
            // further allocation), then copy the buffer over to v
            self.buffer.append(v);
            *v = self.buffer.clone();
            // alt methods
            // TODO remove
            //v.splice(0..0, self.buffer.iter().cloned());
            //*v = [&self.buffer[..], v].concat();

            trace!(
                "Added line fragment of length {} from preprocessor buffer: {:?}",
                self.buffer.len(),
                String::from_utf8_lossy(&self.buffer)
            );
        } else {
            warn!(
                "Discarded line fragment of length {} since total length of {} exceeds maximum allowed line length of {}: {:?}",
                v.len(),
                total_fragment_length,
                self.max_length,
                String::from_utf8_lossy(v)
            );
            // since v is of no use anymore (we did not make a complete line out of it), clear it
            // this enables callers of this function to handle v differently for this case.
            v.clear();
        }

        // reset the preprocessor to initial state
        self.buffer.clear();
        self.fragment_length = 0;
    }
}

impl Preprocessor for Lines {
    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        // split incoming bytes by specifed line separator
        let mut events: Vec<Vec<u8>> = data
            .split(|c| *c == self.separator as u8)
            .map(Vec::from)
            .collect();

        if let Some(last_event) => events.pop() {
            // if incoming data had at least one line separator boundary (anywhere)
            // AND if the preprocessor has memory of line fragment from earlier,
            // reconstruct the first event fully (by adding the buffer contents to it)
            if (last_event.is_empty() || !events.is_empty()) && self.fragment_length > 0 {
                self.complete_fragment(&mut events[0]);
            }

            // if the incoming data did not end in a line boundary, last event is actually
            // a fragment so we need to remmeber it for later (when more data arrives)
            if !last_event.is_empty() {
                self.save_fragment(&last_event);
            }
        }

        Ok(events
            .into_iter()
            .filter(|event| !event.is_empty() && self.is_valid_line(event))
            .collect::<Vec<Vec<u8>>>())
    }
}

// TODO add tests
