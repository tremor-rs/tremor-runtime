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

use std::num::NonZeroUsize;

use super::Preprocessor;
use crate::connectors::prelude::DEFAULT_BUF_SIZE;
use crate::errors::{Kind as ErrorKind, Result};
use memchr::memchr_iter;
use tremor_pipeline::{ConfigImpl, ConfigMap};

const DEFAULT_SEPARATOR: u8 = b'\n';
const INITIAL_LINES_PER_CHUNK: usize = 64;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    separator: String,
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    max_length: Option<usize>,
    #[serde(default = "default_buffered")]
    buffered: bool,
}

fn default_buffered() -> bool {
    true
}

impl ConfigImpl for Config {}

#[derive(Clone)]
pub struct Lines {
    separator: u8,
    max_length: Option<NonZeroUsize>, //set to 0 if no limit for length of the data fragments
    buffer: Vec<u8>,
    is_buffered: bool, //indicates if buffering is needed.
    lines_per_chunk: usize,
}

impl Default for Lines {
    fn default() -> Self {
        Self::new(DEFAULT_SEPARATOR, DEFAULT_BUF_SIZE, true)
    }
}

impl Lines {
    pub fn from_config(config: &ConfigMap) -> Result<Self> {
        if let Some(raw_config) = config {
            let config = Config::new(raw_config)?;
            let separator = {
                if config.separator.len() != 1 {
                    return Err(ErrorKind::InvalidConfiguration(
                        String::from("lines preprocessor"),
                        format!(
                            "Invalid 'separator': \"{}\", must be 1 byte.",
                            config.separator
                        ),
                    )
                    .into());
                } else {
                    config.separator.as_bytes()[0]
                }
            };
            Ok(Self::new(
                separator,
                config.max_length.unwrap_or_default(),
                config.buffered,
            ))
        } else {
            Ok(Self::default())
        }
    }

    pub fn new(separator: u8, max_length: usize, is_buffered: bool) -> Self {
        let max_length = NonZeroUsize::new(max_length);
        let bufsize = max_length
            .map(NonZeroUsize::get)
            .unwrap_or(DEFAULT_BUF_SIZE);
        Self {
            separator,
            max_length,
            // allocating at once with enough capacity to ensure we don't do re-allocations
            // optimizing for performance here instead of memory usage
            buffer: Vec::with_capacity(bufsize),
            is_buffered,
            lines_per_chunk: INITIAL_LINES_PER_CHUNK,
        }
    }

    fn is_valid_line(&self, v: &[u8]) -> bool {
        !self.exceeds_max_length(v.len())
    }

    fn exceeds_max_length(&self, len: usize) -> bool {
        self.max_length
            .map(|max| max.get() < len)
            .unwrap_or_default()
    }

    fn save_fragment(&mut self, v: &[u8]) -> Result<()> {
        let total_fragment_length = self.buffer.len() + v.len();
        if self.exceeds_max_length(total_fragment_length) {
            // exceeding the max_length
            // since we are not saving the current fragment, anything that was saved earlier is
            // useless now so clear the buffer
            self.buffer.clear();
            Err(format!(
                "Discarded line fragment of length {} since total length of {} exceeds maximum allowed length of {}",
                v.len(),
                total_fragment_length,
                self.max_length.map(NonZeroUsize::get).unwrap_or_default(),
            ).into())
        } else {
            self.buffer.extend_from_slice(v);
            // TODO evaluate if the overhead of trace logging is worth it
            trace!(
                "Saved line fragment of length {} to preprocessor buffer",
                v.len(),
            );
            Ok(())
        }
    }

    fn complete_fragment(&mut self, v: &[u8]) -> Result<Vec<u8>> {
        let total_fragment_length = self.buffer.len() + v.len();
        if self.exceeds_max_length(total_fragment_length) {
            self.buffer.clear();
            Err(format!(
                "Discarded line fragment of length {} since total length of {} exceeds maximum allowed line length of {}",
                v.len(),
                total_fragment_length,
                self.max_length.map(NonZeroUsize::get).unwrap_or_default(),
            ).into())
        } else {
            let mut result = Vec::with_capacity(self.buffer.capacity());
            self.buffer.extend_from_slice(v);
            // also resets the preprocessor to initial state (empty buffer)
            std::mem::swap(&mut self.buffer, &mut result);

            trace!(
                "Added line fragment of length {} from preprocessor buffer",
                self.buffer.len(),
            );
            Ok(result)
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
        let mut events = Vec::with_capacity(self.lines_per_chunk);

        let mut last_idx = 0_usize;
        let mut split_points = memchr_iter(separator, data);
        if self.is_buffered {
            if let Some(first_line_idx) = split_points.next() {
                let first_line = &data[last_idx..first_line_idx];
                if !self.buffer.is_empty() {
                    events.push(self.complete_fragment(first_line)?);
                // invalid lines are ignored (and logged about here)
                } else if self.is_valid_line(first_line) {
                    events.push(first_line.to_vec());
                }
                last_idx = first_line_idx + 1;

                while let Some(line_idx) = split_points.next() {
                    let line = &data[last_idx..line_idx];
                    if self.is_valid_line(line) {
                        events.push(line.to_vec());
                    }
                    last_idx = line_idx + 1;
                }
                if last_idx <= data.len() {
                    // this is the last line and since it did not end in a line boundary, it
                    // needs to be remembered for later (when more data arrives)
                    // invalid lines are ignored (and logged about here)
                    self.save_fragment(&data[last_idx..])?;
                }
            } else {
                // if there's no other lines, or if data did not end in a line boundary
                self.save_fragment(data)?;
            }
        } else {
            for split_point in split_points {
                if !self.exceeds_max_length(split_point - last_idx) {
                    events.push(data[last_idx..split_point].to_vec());
                }
                last_idx = split_point + 1;
            }
            // push the rest out, if finished or not
            if last_idx <= data.len() {
                events.push(data[last_idx..].to_vec());
            }
        }

        // update lines per chunk if necessary
        self.lines_per_chunk = self.lines_per_chunk.max(events.len());
        Ok(events)
    }

    fn finish(&mut self, data: Option<&[u8]>) -> Result<Vec<Vec<u8>>> {
        let mut tmp = 0_u64;
        if let Some(data) = data {
            self.process(&mut tmp, data).map(|mut processed| {
                if !self.buffer.is_empty() {
                    processed.push(self.buffer.split_off(0));
                }
                processed
            })
        } else if !self.buffer.is_empty() {
            Ok(vec![self.buffer.split_off(0)])
        } else {
            Ok(vec![])
        }
        
    }
}

#[cfg(test)]
mod test {
    use tremor_script::literal;

    use super::*;
    use crate::Result;

    #[test]
    fn from_config() -> Result<()> {
        let config = Some(literal!({
            "separator": "\n",
            "max_length": 12345,
            "buffered": false
        }));
        let lines = Lines::from_config(&config)?;
        assert!(!lines.is_buffered);
        assert_eq!(NonZeroUsize::new(12345), lines.max_length);
        assert_eq!(b'\n', lines.separator);
        Ok(())
    }

    #[test]
    fn from_config_invalid_separator() -> Result<()> {
        let config = Some(literal!({
        "separator": "abc"
        }));
        let res = Lines::from_config(&config).err().unwrap();

        assert_eq!("Invalid Configuration for lines preprocessor: Invalid 'separator': \"abc\", must be 1 byte.", res.to_string().as_str());
        Ok(())
    }

    #[test]
    fn test6() -> Result<()> {
        let mut pp = Lines::new(DEFAULT_SEPARATOR, 10, true);
        let mut i = 0_u64;

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
        assert!(pp.finish(None)?.is_empty());

        Ok(())
    }

    #[test]
    fn test5() -> Result<()> {
        let mut pp = Lines::new(DEFAULT_SEPARATOR, 10, true);
        let mut i = 0_u64;

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
        let mut pp = Lines::new(DEFAULT_SEPARATOR, 10, true);
        let mut i = 0_u64;

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
        assert!(pp.finish(None)?.is_empty());

        Ok(())
    }

    #[test]
    fn test3() -> Result<()> {
        let mut pp = Lines::new(DEFAULT_SEPARATOR, 10, true);
        let mut i = 0_u64;

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
        let mut pp = Lines::new(DEFAULT_SEPARATOR, 10, true);

        let mut i = 0_u64;

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
        let mut pp = Lines::new(DEFAULT_SEPARATOR, 10, true);

        let mut i = 0_u64;

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
        let mut pp = Lines::new(b'\0', 10, true);
        let mut i = 0_u64;

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
        let mut pp = Lines::new(DEFAULT_SEPARATOR, 10, true);
        let mut i = 0_u64;

        // Test simple split
        let mut r = pp.process(&mut i, b"012345\n0123456789\nabc\n")?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap(), b"abc");
        assert_eq!(r.pop().unwrap(), b"0123456789");
        assert_eq!(r.pop().unwrap(), b"012345");
        assert!(r.is_empty());
        let r = pp.finish(None)?;
        assert!(r.is_empty());

        Ok(())
    }

    #[test]
    fn test_split_buffer_split() -> Result<()> {
        let mut pp = Lines::new(DEFAULT_SEPARATOR, 10, true);
        let mut i = 0_u64;

        // both split and buffer
        let mut r = pp.process(&mut i, b"0123456789\n012345")?;
        assert_eq!(r.pop().unwrap(), b"0123456789");
        assert!(r.is_empty());

        // test picking up from the buffer
        let mut r = pp.process(&mut i, b"\n")?;
        assert_eq!(r.pop().unwrap(), b"012345");
        assert!(r.is_empty());
        assert!(pp.finish(None)?.is_empty());

        Ok(())
    }

    #[test]
    fn test_split_buffer_split_buffer() -> Result<()> {
        let mut pp = Lines::new(DEFAULT_SEPARATOR, 10, true);
        let mut i = 0_u64;

        // both split and buffer
        let mut r = pp.process(&mut i, b"0123456789\n012345")?;
        assert_eq!(r.pop().unwrap(), b"0123456789");
        assert!(r.is_empty());

        // pick up from the buffer and add to buffer
        let mut r = pp.process(&mut i, b"\nabc")?;
        assert_eq!(r.pop().unwrap(), b"012345");
        assert!(r.is_empty());

        let mut r = pp.finish(None)?;
        assert_eq!(r.pop().unwrap(), b"abc");
        assert!(r.is_empty());

        Ok(())
    }

    #[test]
    fn test_split_buffer_buffer_split() -> Result<()> {
        let mut pp = Lines::new(DEFAULT_SEPARATOR, 10, true);
        let mut i = 0_u64;

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

    #[test]
    fn test_leftovers() -> Result<()> {
        let mut pp = Lines::new(DEFAULT_SEPARATOR, 10, true);
        let mut ingest_ns = 0_u64;

        let data = b"123\n456";
        let mut r = pp.process(&mut ingest_ns, data)?;
        assert_eq!(r.pop().unwrap(), b"123");
        assert!(r.is_empty());

        assert!(pp.process(&mut ingest_ns, b"7890")?.is_empty());
        let mut r = pp.process(&mut ingest_ns, data)?;
        assert_eq!(r.pop().unwrap(), b"4567890123");
        assert!(r.is_empty());
        let mut r = pp.finish(None)?;
        assert_eq!(r.pop().unwrap(), b"456");
        assert!(r.is_empty());
        assert!(pp.buffer.is_empty());
        Ok(())
    }

    #[test]
    fn test_max_length_unbuffered() -> Result<()> {
        let mut pp = Lines::new(DEFAULT_SEPARATOR, 0, false);
        let mut ingest_ns = 0_u64;

        let mut data = [b'A'; 10000];
        data[9998] = b'\n';
        let r = pp.process(&mut ingest_ns, &data)?;
        assert_eq!(2, r.len());
        assert_eq!(9998, r[0].len());
        assert_eq!(1, r[1].len());
        let r = pp.finish(None)?;
        assert_eq!(0, dbg!(r).len());
        Ok(())
    }

    #[test]
    fn from_config_len() -> Result<()> {
        let config = Some(literal!({
            "separator": "\n",
            "max_length": 12345
        }));
        let mut pp = Lines::from_config(&config)?;
        assert_eq!(NonZeroUsize::new(12345), pp.max_length);
        let mut ingest_ns = 0_u64;

        let mut data = [b'A'; 10000];
        data[9998] = b'\n';
        let r = pp.process(&mut ingest_ns, &data)?;
        assert_eq!(1, r.len());
        assert_eq!(9998, r[0].len());

        let r = pp.finish(None)?;
        assert_eq!(1, r.len());
        assert_eq!(1, r[0].len());
        Ok(())
    }

    #[test]
    fn test_finish_chain_unbuffered() -> Result<()> {
        let mut ingest_ns = 0;
        let config = Some(literal!({
            "separator": "|",
            "max_length": 0,
            "buffered": false
        }));
        let mut pp = Lines::from_config(&config)?;
        let mut data = [b'A'; 100];
        data[89] = b'|';
        let r = pp.process(&mut ingest_ns, &data)?;
        assert_eq!(2, r.len());
        assert_eq!(89, r[0].len());
        assert_eq!(10, r[1].len());

        let r = pp.finish(Some(&[b'|']))?;
        assert_eq!(2, r.len());
        assert_eq!(0, r[0].len());
        assert_eq!(0, r[1].len());
        Ok(())
    }

    #[test]
    fn test_finish_chain_buffered() -> Result<()> {
        let mut ingest_ns = 0;
        let config = Some(literal!({
            "separator": "|",
            "max_length": 0,
            "buffered": true
        }));
        let mut pp = Lines::from_config(&config)?;
        let mut data = [b'A'; 100];
        data[89] = b'|';
        let r = pp.process(&mut ingest_ns, &data)?;
        assert_eq!(1, r.len());
        assert_eq!(89, r[0].len());

        let r = pp.finish(Some(&[b'|', b'A']))?;
        assert_eq!(2, r.len());
        assert_eq!(10, r[0].len());
        assert_eq!(1, r[1].len());
        Ok(())
    }
}
