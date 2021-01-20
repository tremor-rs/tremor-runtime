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
use crate::errors::Result;

#[derive(Clone)]
pub struct Lines {
    separator: u8,
    max_length: usize, //set to 0 if no limit for length of the data fragments
    buffer: Vec<u8>,
    is_buffered: bool, //indicates if buffering is needed.
}

impl Default for Lines {
    fn default() -> Self {
        Self::new('\n', 4096, true)
    }
}

impl Lines {
    // TODO have the params here as a config struct
    // also break lines on string (eg: \r\n)
    pub fn new(separator: char, max_length: usize, is_buffered: bool) -> Self {
        Self {
            separator: separator as u8,
            max_length,
            // allocating at once with enough capacity to ensure we don't do re-allocations
            // optimizing for performance here instead of memory usage
            buffer: Vec::with_capacity(max_length),
            is_buffered,
        }
    }

    fn is_valid_line(&self, v: &[u8]) -> bool {
        //return true if is there is no limit on max length of the data fragment
        if self.max_length == 0 {
            return true;
        }

        if v.len() <= self.max_length {
            true
        } else {
            warn!(
                "Invalid line of length {} since it exceeds maximum allowed length of {}",
                v.len(),
                self.max_length,
            );
            false
        }
    }

    fn save_fragment(&mut self, v: &[u8]) -> Result<()> {
        let total_fragment_length = self.buffer.len() + v.len();

        if total_fragment_length <= self.max_length {
            self.buffer.extend_from_slice(v);
            // TODO evaluate if the overhead of trace logging is worth it
            trace!(
                "Saved line fragment of length {} to preprocessor buffer",
                v.len(),
            );
            Ok(())
        } else {
            // since we are not saving the current fragment, anything that was saved earlier is
            // useless now so clear the buffer
            self.buffer.clear();
            Err(format!(
                "Discarded line fragment of length {} since total length of {} exceeds maximum allowed length of {}",
                v.len(),
                total_fragment_length,
                self.max_length,
            ).into())
        }
    }

    fn complete_fragment(&mut self, v: &[u8]) -> Result<Vec<u8>> {
        let total_fragment_length = self.buffer.len() + v.len();

        if total_fragment_length <= self.max_length {
            let mut result = Vec::with_capacity(self.max_length);
            self.buffer.extend_from_slice(v);
            // also resets the preprocessor to initial state (empty buffer)
            std::mem::swap(&mut self.buffer, &mut result);

            trace!(
                "Added line fragment of length {} from preprocessor buffer",
                self.buffer.len(),
            );
            Ok(result)
        } else {
            self.buffer.clear();
            Err(format!(
                "Discarded line fragment of length {} since total length of {} exceeds maximum allowed line length of {}",
                v.len(),
                total_fragment_length,
                self.max_length,
            ).into())
        }
    }
}

impl Preprocessor for Lines {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "lines"
    }

    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        // split incoming bytes by specifed line separator
        let separator = self.separator;
        let mut lines = data.split(|c| *c == separator).peekable();

        if !self.is_buffered {
            return Ok(lines
                .filter_map(|line| {
                    if self.is_valid_line(line) {
                        Some(line.to_vec())
                    } else {
                        None
                    }
                })
                .collect::<Vec<Vec<u8>>>());
        }

        // logic for when we need to buffer any data fragments present (after the last line separator)

        // assume we don't get more then 64 lines by default (in a single data payload)
        let mut events = Vec::with_capacity(lines.size_hint().1.unwrap_or(64));

        if let Some(first_line) = lines.next() {
            // if there's no other lines, or if data did not end in a line boundary
            if lines.peek().is_none() {
                self.save_fragment(first_line)?;
            } else {
                if !self.buffer.is_empty() {
                    events.push(self.complete_fragment(first_line)?);
                // invalid lines are ignored (and logged about here)
                } else if self.is_valid_line(first_line) {
                    events.push(first_line.to_vec());
                }

                while let Some(line) = lines.next() {
                    if lines.peek().is_none() {
                        // this is the last line and since it did not end in a line boundary, it
                        // needs to be remembered for later (when more data arrives)
                        self.save_fragment(line)?;
                    // invalid lines are ignored (and logged about here)
                    } else if self.is_valid_line(line) {
                        events.push(line.to_vec());
                    }
                }
            }
        }

        Ok(events)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::Result;

    #[test]
    fn test6() -> Result<()> {
        let mut pp = Lines::default();
        let mut i = 0_u64;
        pp.max_length = 10;

        // Test simple split
        let mut r = pp.process(&mut i, b"012345\n0123456789\n")?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap(), b"0123456789");
        assert_eq!(r.pop().unwrap(), b"012345");
        assert!(r.is_empty());

        // split test
        assert!(pp.process(&mut i, b"012345")?.is_empty());
        let mut r = pp.process(&mut i, b"\n0123456789\n")?;
        assert_eq!(r.pop().unwrap(), b"0123456789");
        assert_eq!(r.pop().unwrap(), b"012345");
        assert!(r.is_empty());

        // error for adding too much
        assert!(pp.process(&mut i, b"012345")?.is_empty());
        assert!(pp.process(&mut i, b"0123456789\n").is_err());

        // Test if we still work with new data
        let mut r = pp.process(&mut i, b"012345\n0123456789\n")?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap(), b"0123456789");
        assert_eq!(r.pop().unwrap(), b"012345");
        assert!(r.is_empty());

        // error for adding too much
        assert!(pp.process(&mut i, b"012345")?.is_empty());
        assert!(pp.process(&mut i, b"0123456789").is_err());

        // Test if we still work with new data
        let mut r = pp.process(&mut i, b"012345\n0123456789\n")?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap(), b"0123456789");
        assert_eq!(r.pop().unwrap(), b"012345");
        assert!(r.is_empty());

        Ok(())
    }

    #[test]
    fn test5() -> Result<()> {
        let mut pp = Lines::default();
        let mut i = 0_u64;
        pp.max_length = 10;

        // Test simple split
        let mut r = pp.process(&mut i, b"012345\n0123456789\n")?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap(), b"0123456789");
        assert_eq!(r.pop().unwrap(), b"012345");
        assert!(r.is_empty());

        // split test
        assert!(pp.process(&mut i, b"012345")?.is_empty());
        let mut r = pp.process(&mut i, b"\n0123456789\n")?;
        assert_eq!(r.pop().unwrap(), b"0123456789");
        assert_eq!(r.pop().unwrap(), b"012345");
        assert!(r.is_empty());

        // error for adding too much
        assert!(pp.process(&mut i, b"012345")?.is_empty());
        assert!(pp.process(&mut i, b"0123456789\n").is_err());

        // Test if we still work with new data
        let mut r = pp.process(&mut i, b"012345\n0123456789\n")?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap(), b"0123456789");
        assert_eq!(r.pop().unwrap(), b"012345");
        assert!(r.is_empty());

        // error for adding too much
        assert!(pp.process(&mut i, b"012345")?.is_empty());
        assert!(pp.process(&mut i, b"0123456789").is_err());

        Ok(())
    }

    #[test]
    fn test4() -> Result<()> {
        let mut pp = Lines::default();
        let mut i = 0_u64;
        pp.max_length = 10;

        // Test simple split
        let mut r = pp.process(&mut i, b"012345\n0123456789\n")?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap(), b"0123456789");
        assert_eq!(r.pop().unwrap(), b"012345");
        assert!(r.is_empty());

        // split test
        assert!(pp.process(&mut i, b"012345")?.is_empty());
        let mut r = pp.process(&mut i, b"\n0123456789\n")?;
        assert_eq!(r.pop().unwrap(), b"0123456789");
        assert_eq!(r.pop().unwrap(), b"012345");
        assert!(r.is_empty());

        // error for adding too much
        assert!(pp.process(&mut i, b"012345")?.is_empty());
        assert!(pp.process(&mut i, b"0123456789\n").is_err());

        // Test if we still work with new data
        let mut r = pp.process(&mut i, b"012345\n0123456789\n")?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap(), b"0123456789");
        assert_eq!(r.pop().unwrap(), b"012345");
        assert!(r.is_empty());

        Ok(())
    }

    #[test]
    fn test3() -> Result<()> {
        let mut pp = Lines::default();
        let mut i = 0_u64;
        pp.max_length = 10;

        // Test simple split
        let mut r = pp.process(&mut i, b"012345\n0123456789\n")?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap(), b"0123456789");
        assert_eq!(r.pop().unwrap(), b"012345");
        assert!(r.is_empty());

        // split test
        assert!(pp.process(&mut i, b"012345")?.is_empty());
        let mut r = pp.process(&mut i, b"\n0123456789\n")?;
        assert_eq!(r.pop().unwrap(), b"0123456789");
        assert_eq!(r.pop().unwrap(), b"012345");
        assert!(r.is_empty());

        // error for adding too much
        assert!(pp.process(&mut i, b"012345")?.is_empty());
        assert!(pp.process(&mut i, b"0123456789\n").is_err());

        Ok(())
    }

    #[test]
    fn test2() -> Result<()> {
        let mut pp = Lines::default();
        let mut i = 0_u64;
        pp.max_length = 10;

        // Test simple split
        let mut r = pp.process(&mut i, b"012345\n0123456789\n")?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap(), b"0123456789");
        assert_eq!(r.pop().unwrap(), b"012345");
        assert!(r.is_empty());

        // split test
        assert!(pp.process(&mut i, b"012345")?.is_empty());
        let mut r = pp.process(&mut i, b"\n0123456789\n")?;
        assert_eq!(r.pop().unwrap(), b"0123456789");
        assert_eq!(r.pop().unwrap(), b"012345");
        assert!(r.is_empty());

        Ok(())
    }

    #[test]
    fn test1() -> Result<()> {
        let mut pp = Lines::default();
        let mut i = 0_u64;
        pp.max_length = 10;

        // Test simple split
        let mut r = pp.process(&mut i, b"012345\n0123456789\n")?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap(), b"0123456789");
        assert_eq!(r.pop().unwrap(), b"012345");
        assert!(r.is_empty());

        Ok(())
    }

    #[test]
    fn test_non_default_separator() -> Result<()> {
        let mut pp = Lines::new('\0', 4096, true);
        let mut i = 0_u64;
        pp.max_length = 10;

        // Test simple split
        let mut r = pp.process(&mut i, b"012345\00123456789\0")?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap(), b"0123456789");
        assert_eq!(r.pop().unwrap(), b"012345");
        assert!(r.is_empty());

        Ok(())
    }

    #[test]
    fn test_empty_data() -> Result<()> {
        let mut pp = Lines::default();
        let mut i = 0_u64;
        assert!(pp.process(&mut i, b"")?.is_empty());
        Ok(())
    }

    #[test]
    fn test_empty_data_after_buffer() -> Result<()> {
        let mut pp = Lines::default();
        let mut i = 0_u64;
        assert!(pp.process(&mut i, b"a")?.is_empty());
        assert!(pp.process(&mut i, b"")?.is_empty());
        Ok(())
    }

    #[test]
    fn test_split_split_split() -> Result<()> {
        let mut pp = Lines::default();
        let mut i = 0_u64;
        pp.max_length = 10;

        // Test simple split
        let mut r = pp.process(&mut i, b"012345\n0123456789\nabc\n")?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap(), b"abc");
        assert_eq!(r.pop().unwrap(), b"0123456789");
        assert_eq!(r.pop().unwrap(), b"012345");
        assert!(r.is_empty());

        Ok(())
    }

    #[test]
    fn test_split_buffer_split() -> Result<()> {
        let mut pp = Lines::default();
        let mut i = 0_u64;
        pp.max_length = 10;

        // both split and buffer
        let mut r = pp.process(&mut i, b"0123456789\n012345")?;
        assert_eq!(r.pop().unwrap(), b"0123456789");
        assert!(r.is_empty());

        // test picking up from the buffer
        let mut r = pp.process(&mut i, b"\n")?;
        assert_eq!(r.pop().unwrap(), b"012345");
        assert!(r.is_empty());

        Ok(())
    }

    #[test]
    fn test_split_buffer_split_buffer() -> Result<()> {
        let mut pp = Lines::default();
        let mut i = 0_u64;
        pp.max_length = 10;

        // both split and buffer
        let mut r = pp.process(&mut i, b"0123456789\n012345")?;
        assert_eq!(r.pop().unwrap(), b"0123456789");
        assert!(r.is_empty());

        // pick up from the buffer and add to buffer
        let mut r = pp.process(&mut i, b"\nabc")?;
        assert_eq!(r.pop().unwrap(), b"012345");
        assert!(r.is_empty());

        Ok(())
    }

    #[test]
    fn test_split_buffer_buffer_split() -> Result<()> {
        let mut pp = Lines::default();
        let mut i = 0_u64;
        pp.max_length = 10;

        // both split and buffer
        let mut r = pp.process(&mut i, b"0123456789\n012345")?;
        assert_eq!(r.pop().unwrap(), b"0123456789");
        assert!(r.is_empty());

        // pick up from the buffer and add to buffer as well
        let mut r = pp.process(&mut i, b"abc\n")?;
        assert_eq!(r.pop().unwrap(), b"012345abc");
        assert!(r.is_empty());

        Ok(())
    }
}
