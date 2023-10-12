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

//! Splits the input into events, using a given separator, the default being `\n` (newline).
//!
//! The default can be overwritten using the `separator` option.
//!
//! Buffers any fragment that may be present (after the last separator), till more data arrives. This makes it ideal for use with streaming onramps like [tcp](../connectors/tcp), to break down incoming data into distinct events.
//!
//! Additional options are:
//!
//! | Option       | Description                                                                                                                                                                                                                                                                                                                                                     | Required | Default Value |
//! |--------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------------|
//! | `separator ` | The separator to split the incoming bytes at                                                                                                                                                                                                                                                                                                                    | no       | `\n`          |
//! | `max_length` | the maximum length in bytes to keep buffering before giving up finding a separator character. This prevents consuming huge ammounts of memory if no separator ever arrives.                                                                                                                                                                                     | no       | `8192`        |
//! | `buffered`   | buffer multiple fragments to find a seperator, if this is set to false each fragment will be considered to be followed by a separator so "hello\nworld" would turn into two events "hello" and "world". With buffered true "hello\nworld" would turn into one event "hello" and "world" will be buffered until a next event includes a `\n` or the stream ends. | no       | `true`        |
//!
//!
//! If this preprocessor is only configured by name, it will split on `\n`, does not enforce a maximum length of `8192` and buffer incoming byte fragments until a separator is found or the `max_length` is hit, at which point the fragment is discarded.
//!
//! Example configuration:
//!
//! ```tremor
//! define connector foo from ws_client
//! with
//!     preprocessors = ["separate"],
//!     postprocessors = ["separate"]
//!     codec = "json",
//!     config = {
//!         "url": "ws://localhost:12345"
//!     }
//! end;
//!
//! define connector snot from ws_server
//! with
//!     preprocessors = [
//!         {
//!             "name": "separate",
//!             "config": {
//!                 "separator": "|",
//!                 "max_length": 100000,
//!                 "buffered": false
//!             }
//!         }
//!     ],
//!     ...
//! end;
//! ```

use super::prelude::*;
use crate::errors::{ErrorKind, Result};
use log::trace;
use memchr::memchr_iter;
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;
use tremor_config::{Impl as ConfigImpl, Map as ConfigMap};

pub(crate) const DEFAULT_BUF_SIZE: usize = 8 * 1024;
pub(crate) const DEFAULT_SEPARATOR: u8 = b'\n';
const INITIAL_PARTS_PER_CHUNK: usize = 64;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_separator")]
    separator: String,
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    max_length: Option<usize>,
    #[serde(default = "default_true")]
    buffered: bool,
}

pub(crate) fn default_separator() -> String {
    String::from_utf8_lossy(&[DEFAULT_SEPARATOR]).into_owned()
}

impl tremor_config::Impl for Config {}

#[derive(Clone)]
pub struct Separate {
    separator: u8,
    max_length: Option<NonZeroUsize>, //set to 0 if no limit for length of the data fragments
    buffer: Vec<u8>,
    is_buffered: bool, //indicates if buffering is needed.
    parts_per_chunk: usize,
}

impl Default for Separate {
    fn default() -> Self {
        Self::new(DEFAULT_SEPARATOR, DEFAULT_BUF_SIZE, true)
    }
}

impl Separate {
    pub fn from_config(config: &ConfigMap) -> Result<Self> {
        if let Some(raw_config) = config {
            let config = Config::new(raw_config)?;
            let separator = {
                if config.separator.len() != 1 {
                    return Err(ErrorKind::InvalidConfiguration(
                        String::from("split preprocessor"),
                        format!(
                            "Invalid 'separator': \"{}\", must be 1 byte.",
                            config.separator
                        ),
                    )
                    .into());
                }
                config.separator.as_bytes()[0]
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
        let bufsize = max_length.map_or(DEFAULT_BUF_SIZE, NonZeroUsize::get);
        Self {
            separator,
            max_length,
            // allocating at once with enough capacity to ensure we don't do re-allocations
            // optimizing for performance here instead of memory usage
            buffer: Vec::with_capacity(bufsize),
            is_buffered,
            parts_per_chunk: INITIAL_PARTS_PER_CHUNK,
        }
    }

    fn is_valid_chunk(&self, v: &[u8]) -> bool {
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
                "Discarded fragment of length {} since total length of {} exceeds maximum allowed length of {}",
                v.len(),
                total_fragment_length,
                self.max_length.map(NonZeroUsize::get).unwrap_or_default(),
            ).into())
        } else {
            self.buffer.extend_from_slice(v);
            // TODO evaluate if the overhead of trace logging is worth it
            trace!(
                "Saved fragment of length {} to preprocessor buffer",
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
                "Discarded fragment of length {} since total length of {} exceeds maximum allowed chunk size of {}",
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
                "Added fragment of length {} from preprocessor buffer",
                self.buffer.len(),
            );
            Ok(result)
        }
    }
}

impl Preprocessor for Separate {
    fn name(&self) -> &str {
        "separate"
    }

    fn process(
        &mut self,
        _ingest_ns: &mut u64,
        data: &[u8],
        meta: Value<'static>,
    ) -> Result<Vec<(Vec<u8>, Value<'static>)>> {
        // split incoming bytes by specifed separator
        let separator = self.separator;
        let mut events = Vec::with_capacity(self.parts_per_chunk);

        let mut last_idx = 0_usize;
        let mut split_points = memchr_iter(separator, data);
        if self.is_buffered {
            if let Some(first_fragment_idx) = split_points.next() {
                // ALLOW
                let first_fragment = data
                    .get(last_idx..first_fragment_idx)
                    .ok_or(ErrorKind::InvalidInputData("Out of bounds"))?;
                if !self.buffer.is_empty() {
                    events.push((self.complete_fragment(first_fragment)?, meta.clone()));
                // invalid lines are ignored (and logged about here)
                } else if self.is_valid_chunk(first_fragment) {
                    events.push((first_fragment.to_vec(), meta.clone()));
                }
                last_idx = first_fragment_idx + 1;

                for fragment_idx in split_points.by_ref() {
                    let fragment = data
                        .get(last_idx..fragment_idx)
                        .ok_or(ErrorKind::InvalidInputData("Out of bounds"))?;
                    if self.is_valid_chunk(fragment) {
                        events.push((fragment.to_vec(), meta.clone()));
                    }
                    last_idx = fragment_idx + 1;
                }
                if last_idx <= data.len() {
                    // this is the last line and since it did not end in a line boundary, it
                    // needs to be remembered for later (when more data arrives)
                    // invalid lines are ignored (and logged about here)
                    self.save_fragment(
                        data.get(last_idx..)
                            .ok_or(ErrorKind::InvalidInputData("Out of bounds"))?,
                    )?;
                }
            } else {
                // if there's no other fragment, or if data did not end in a separator boundary
                self.save_fragment(data)?;
            }
        } else {
            for split_point in split_points {
                if !self.exceeds_max_length(split_point - last_idx) {
                    events.push((
                        data.get(last_idx..split_point)
                            .ok_or(ErrorKind::InvalidInputData("Out of bounds"))?
                            .to_vec(),
                        meta.clone(),
                    ));
                }
                last_idx = split_point + 1;
            }
            // push the rest out, if finished or not
            if last_idx <= data.len() {
                events.push((
                    data.get(last_idx..)
                        .ok_or(ErrorKind::InvalidInputData("Out of bounds"))?
                        .to_vec(),
                    meta,
                ));
            }
        }

        // update parts per chunk if necessary
        self.parts_per_chunk = self.parts_per_chunk.max(events.len());
        Ok(events)
    }

    fn finish(
        &mut self,
        data: Option<&[u8]>,
        meta: Option<Value<'static>>,
    ) -> Result<Vec<(Vec<u8>, Value<'static>)>> {
        let mut tmp = 0_u64;
        if let Some(data) = data {
            self.process(&mut tmp, data, meta.clone().unwrap_or_else(Value::object))
                .map(|mut processed| {
                    if !self.buffer.is_empty() {
                        processed
                            .push((self.buffer.split_off(0), meta.unwrap_or_else(Value::object)));
                    }
                    processed
                })
        } else if !self.buffer.is_empty() {
            Ok(vec![(
                self.buffer.split_off(0),
                meta.unwrap_or_else(Value::object),
            )])
        } else {
            Ok(vec![])
        }
    }
}

#[cfg(test)]
mod test {
    use tremor_value::literal;

    use super::*;
    use crate::errors::Result;

    #[test]
    fn from_config() -> Result<()> {
        let config = Some(literal!({
            "separator": "\n",
            "max_length": 12345,
            "buffered": false
        }));
        let separate = Separate::from_config(&config)?;
        assert!(!separate.is_buffered);
        assert_eq!(NonZeroUsize::new(12345), separate.max_length);
        assert_eq!(b'\n', separate.separator);
        Ok(())
    }

    #[test]
    fn from_config_invalid_separator() {
        let config = Some(literal!({
        "separator": "abc"
        }));
        let res = Separate::from_config(&config)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();

        assert_eq!("Invalid Configuration for split preprocessor: Invalid 'separator': \"abc\", must be 1 byte.", res);
    }

    #[test]
    fn test6() -> Result<()> {
        let mut pp = Separate::new(DEFAULT_SEPARATOR, 10, true);
        let mut i = 0_u64;

        // Test simple split
        let mut r = pp.process(&mut i, b"012345\n0123456789\n", Value::object())?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap_or_default().0, b"0123456789");
        assert_eq!(r.pop().unwrap_or_default().0, b"012345");
        assert!(r.is_empty());

        // split test
        assert!(pp.process(&mut i, b"012345", Value::object())?.is_empty());
        let mut r = pp.process(&mut i, b"\n0123456789\n", Value::object())?;
        assert_eq!(r.pop().unwrap_or_default().0, b"0123456789");
        assert_eq!(r.pop().unwrap_or_default().0, b"012345");
        assert!(r.is_empty());

        // error for adding too much
        assert!(pp.process(&mut i, b"012345", Value::object())?.is_empty());
        assert!(pp
            .process(&mut i, b"0123456789\n", Value::object())
            .is_err());

        // Test if we still work with new data
        let mut r = pp.process(&mut i, b"012345\n0123456789\n", Value::object())?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap_or_default().0, b"0123456789");
        assert_eq!(r.pop().unwrap_or_default().0, b"012345");
        assert!(r.is_empty());

        // error for adding too much
        assert!(pp.process(&mut i, b"012345", Value::object())?.is_empty());
        assert!(pp.process(&mut i, b"0123456789", Value::object()).is_err());

        // Test if we still work with new data
        let mut r = pp.process(&mut i, b"012345\n0123456789\n", Value::object())?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap_or_default().0, b"0123456789");
        assert_eq!(r.pop().unwrap_or_default().0, b"012345");
        assert!(r.is_empty());
        assert!(pp.finish(None, None)?.is_empty());

        Ok(())
    }

    #[test]
    fn test5() -> Result<()> {
        let mut pp = Separate::new(DEFAULT_SEPARATOR, 10, true);
        let mut i = 0_u64;

        // Test simple split
        let mut r = pp.process(&mut i, b"012345\n0123456789\n", Value::object())?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap_or_default().0, b"0123456789");
        assert_eq!(r.pop().unwrap_or_default().0, b"012345");
        assert!(r.is_empty());

        // split test
        assert!(pp.process(&mut i, b"012345", Value::object())?.is_empty());
        let mut r = pp.process(&mut i, b"\n0123456789\n", Value::object())?;
        assert_eq!(r.pop().unwrap_or_default().0, b"0123456789");
        assert_eq!(r.pop().unwrap_or_default().0, b"012345");
        assert!(r.is_empty());

        // error for adding too much
        assert!(pp.process(&mut i, b"012345", Value::object())?.is_empty());
        assert!(pp
            .process(&mut i, b"0123456789\n", Value::object())
            .is_err());

        // Test if we still work with new data
        let mut r = pp.process(&mut i, b"012345\n0123456789\n", Value::object())?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap_or_default().0, b"0123456789");
        assert_eq!(r.pop().unwrap_or_default().0, b"012345");
        assert!(r.is_empty());

        // error for adding too much
        assert!(pp.process(&mut i, b"012345", Value::object())?.is_empty());
        assert!(pp.process(&mut i, b"0123456789", Value::object()).is_err());
        Ok(())
    }

    #[test]
    fn test4() -> Result<()> {
        let mut pp = Separate::new(DEFAULT_SEPARATOR, 10, true);
        let mut i = 0_u64;

        // Test simple split
        let mut r = pp.process(&mut i, b"012345\n0123456789\n", Value::object())?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap_or_default().0, b"0123456789");
        assert_eq!(r.pop().unwrap_or_default().0, b"012345");
        assert!(r.is_empty());

        // split test
        assert!(pp.process(&mut i, b"012345", Value::object())?.is_empty());
        let mut r = pp.process(&mut i, b"\n0123456789\n", Value::object())?;
        assert_eq!(r.pop().unwrap_or_default().0, b"0123456789");
        assert_eq!(r.pop().unwrap_or_default().0, b"012345");
        assert!(r.is_empty());

        // error for adding too much
        assert!(pp.process(&mut i, b"012345", Value::object())?.is_empty());
        assert!(pp
            .process(&mut i, b"0123456789\n", Value::object())
            .is_err());

        // Test if we still work with new data
        let mut r = pp.process(&mut i, b"012345\n0123456789\n", Value::object())?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap_or_default().0, b"0123456789");
        assert_eq!(r.pop().unwrap_or_default().0, b"012345");
        assert!(r.is_empty());
        assert!(pp.finish(None, None)?.is_empty());

        Ok(())
    }

    #[test]
    fn test3() -> Result<()> {
        let mut pp = Separate::new(DEFAULT_SEPARATOR, 10, true);
        let mut i = 0_u64;

        // Test simple split
        let mut r = pp.process(&mut i, b"012345\n0123456789\n", Value::object())?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap_or_default().0, b"0123456789");
        assert_eq!(r.pop().unwrap_or_default().0, b"012345");
        assert!(r.is_empty());

        // split test
        assert!(pp.process(&mut i, b"012345", Value::object())?.is_empty());
        let mut r = pp.process(&mut i, b"\n0123456789\n", Value::object())?;
        assert_eq!(r.pop().unwrap_or_default().0, b"0123456789");
        assert_eq!(r.pop().unwrap_or_default().0, b"012345");
        assert!(r.is_empty());

        // error for adding too much
        assert!(pp.process(&mut i, b"012345", Value::object())?.is_empty());
        assert!(pp
            .process(&mut i, b"0123456789\n", Value::object())
            .is_err());

        Ok(())
    }

    #[test]
    fn test2() -> Result<()> {
        let mut pp = Separate::new(DEFAULT_SEPARATOR, 10, true);

        let mut i = 0_u64;

        // Test simple split
        let mut r = pp.process(&mut i, b"012345\n0123456789\n", Value::object())?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap_or_default().0, b"0123456789");
        assert_eq!(r.pop().unwrap_or_default().0, b"012345");
        assert!(r.is_empty());

        // split test
        assert!(pp.process(&mut i, b"012345", Value::object())?.is_empty());
        let mut r = pp.process(&mut i, b"\n0123456789\n", Value::object())?;
        assert_eq!(r.pop().unwrap_or_default().0, b"0123456789");
        assert_eq!(r.pop().unwrap_or_default().0, b"012345");
        assert!(r.is_empty());

        Ok(())
    }

    #[test]
    fn test1() -> Result<()> {
        let mut pp = Separate::new(DEFAULT_SEPARATOR, 10, true);

        let mut i = 0_u64;

        // Test simple split
        let mut r = pp.process(&mut i, b"012345\n0123456789\n", Value::object())?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap_or_default().0, b"0123456789");
        assert_eq!(r.pop().unwrap_or_default().0, b"012345");
        assert!(r.is_empty());

        Ok(())
    }

    #[test]
    fn test_non_default_separator() -> Result<()> {
        let mut pp = Separate::new(b'\0', 10, true);
        let mut i = 0_u64;

        // Test simple split
        let mut r = pp.process(&mut i, b"012345\x000123456789\x00", Value::object())?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap_or_default().0, b"0123456789");
        assert_eq!(r.pop().unwrap_or_default().0, b"012345");
        assert!(r.is_empty());

        Ok(())
    }

    #[test]
    fn test_empty_data() -> Result<()> {
        let mut pp = Separate::default();
        let mut i = 0_u64;
        assert!(pp.process(&mut i, b"", Value::object())?.is_empty());
        Ok(())
    }

    #[test]
    fn test_empty_data_after_buffer() -> Result<()> {
        let mut pp = Separate::default();
        let mut i = 0_u64;
        assert!(pp.process(&mut i, b"a", Value::object())?.is_empty());
        assert!(pp.process(&mut i, b"", Value::object())?.is_empty());
        Ok(())
    }

    #[test]
    fn test_split_split_split() -> Result<()> {
        let mut pp = Separate::new(DEFAULT_SEPARATOR, 10, true);
        let mut i = 0_u64;

        // Test simple split
        let mut r = pp.process(&mut i, b"012345\n0123456789\nabc\n", Value::object())?;
        // since we pop this is going to be reverse order
        assert_eq!(r.pop().unwrap_or_default().0, b"abc");
        assert_eq!(r.pop().unwrap_or_default().0, b"0123456789");
        assert_eq!(r.pop().unwrap_or_default().0, b"012345");
        assert!(r.is_empty());
        let r = pp.finish(None, None)?;
        assert!(r.is_empty());

        Ok(())
    }

    #[test]
    fn test_split_buffer_split() -> Result<()> {
        let mut pp = Separate::new(DEFAULT_SEPARATOR, 10, true);
        let mut i = 0_u64;

        // both split and buffer
        let mut r = pp.process(&mut i, b"0123456789\n012345", Value::object())?;
        assert_eq!(r.pop().unwrap_or_default().0, b"0123456789");
        assert!(r.is_empty());

        // test picking up from the buffer
        let mut r = pp.process(&mut i, b"\n", Value::object())?;
        assert_eq!(r.pop().unwrap_or_default().0, b"012345");
        assert!(r.is_empty());
        assert!(pp.finish(None, None)?.is_empty());

        Ok(())
    }

    #[test]
    fn test_split_buffer_split_buffer() -> Result<()> {
        let mut pp = Separate::new(DEFAULT_SEPARATOR, 10, true);
        let mut i = 0_u64;

        // both split and buffer
        let mut r = pp.process(&mut i, b"0123456789\n012345", Value::object())?;
        assert_eq!(r.pop().unwrap_or_default().0, b"0123456789");
        assert!(r.is_empty());

        // pick up from the buffer and add to buffer
        let mut r = pp.process(&mut i, b"\nabc", Value::object())?;
        assert_eq!(r.pop().unwrap_or_default().0, b"012345");
        assert!(r.is_empty());

        let mut r = pp.finish(None, None)?;
        assert_eq!(r.pop().unwrap_or_default().0, b"abc");
        assert!(r.is_empty());

        Ok(())
    }

    #[test]
    fn test_split_buffer_buffer_split() -> Result<()> {
        let mut pp = Separate::new(DEFAULT_SEPARATOR, 10, true);
        let mut i = 0_u64;

        // both split and buffer
        let mut r = pp.process(&mut i, b"0123456789\n012345", Value::object())?;
        assert_eq!(r.pop().unwrap_or_default().0, b"0123456789");
        assert!(r.is_empty());

        // pick up from the buffer and add to buffer as well
        let mut r = pp.process(&mut i, b"abc\n", Value::object())?;
        assert_eq!(r.pop().unwrap_or_default().0, b"012345abc");
        assert!(r.is_empty());

        Ok(())
    }

    #[test]
    fn test_leftovers() -> Result<()> {
        let mut pp = Separate::new(DEFAULT_SEPARATOR, 10, true);
        let mut ingest_ns = 0_u64;

        let data = b"123\n456";
        let mut r = pp.process(&mut ingest_ns, data, Value::object())?;
        assert_eq!(r.pop().unwrap_or_default().0, b"123");
        assert!(r.is_empty());

        assert!(pp
            .process(&mut ingest_ns, b"7890", Value::object())?
            .is_empty());
        let mut r = pp.process(&mut ingest_ns, data, Value::object())?;
        assert_eq!(r.pop().unwrap_or_default().0, b"4567890123");
        assert!(r.is_empty());
        let mut r = pp.finish(None, None)?;
        assert_eq!(r.pop().unwrap_or_default().0, b"456");
        assert!(r.is_empty());
        assert!(pp.buffer.is_empty());
        Ok(())
    }

    #[test]
    fn test_max_length_unbuffered() -> Result<()> {
        let mut pp = Separate::new(DEFAULT_SEPARATOR, 0, false);
        let mut ingest_ns = 0_u64;

        let mut data = [b'A'; 10000];
        data[9998] = b'\n';
        let r = pp.process(&mut ingest_ns, &data, Value::object())?;
        assert_eq!(2, r.len());
        assert_eq!(9998, r[0].0.len());
        assert_eq!(1, r[1].0.len());
        let r = pp.finish(None, None)?;
        assert_eq!(0, r.len());
        Ok(())
    }

    #[test]
    fn from_config_len() -> Result<()> {
        let config = Some(literal!({
            "separator": "\n",
            "max_length": 12345
        }));
        let mut pp = Separate::from_config(&config)?;
        assert_eq!(NonZeroUsize::new(12345), pp.max_length);
        let mut ingest_ns = 0_u64;

        let mut data = [b'A'; 10000];
        data[9998] = b'\n';
        let r = pp.process(&mut ingest_ns, &data, Value::object())?;
        assert_eq!(1, r.len());
        assert_eq!(9998, r[0].0.len());

        let r = pp.finish(None, None)?;
        assert_eq!(1, r.len());
        assert_eq!(1, r[0].0.len());
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
        let mut pp = Separate::from_config(&config)?;
        let mut data = [b'A'; 100];
        data[89] = b'|';
        let r = pp.process(&mut ingest_ns, &data, Value::object())?;
        assert_eq!(2, r.len());
        assert_eq!(89, r[0].0.len());
        assert_eq!(10, r[1].0.len());

        let r = pp.finish(Some(&[b'|']), None)?;
        assert_eq!(2, r.len());
        assert_eq!(0, r[0].0.len());
        assert_eq!(0, r[1].0.len());
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
        let mut pp = Separate::from_config(&config)?;
        let mut data = [b'A'; 100];
        data[89] = b'|';
        let r = pp.process(&mut ingest_ns, &data, Value::object())?;
        assert_eq!(1, r.len());
        assert_eq!(89, r[0].0.len());

        let r = pp.finish(Some(&[b'|', b'A']), None)?;
        assert_eq!(2, r.len());
        assert_eq!(10, r[0].0.len());
        assert_eq!(1, r[1].0.len());
        Ok(())
    }
}
