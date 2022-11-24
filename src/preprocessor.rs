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

mod decompress;
pub(crate) mod gelf;
pub(crate) mod separate;

use crate::config::Preprocessor as PreprocessorConfig;
use crate::connectors::Alias;
use crate::errors::{Error, Result};
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use bytes::{buf::Buf, BytesMut};
use std::str;

//pub type Lines = lines::Lines;

/// A set of preprocessors
pub type Preprocessors = Vec<Box<dyn Preprocessor>>;

/// Preprocessor trait
pub trait Preprocessor: Sync + Send {
    /// Canonical name for this preprocessor
    fn name(&self) -> &str;
    /// process data
    ///
    /// # Errors
    ///
    /// * Errors if the data can not processed
    fn process(&mut self, ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>>;

    /// Finish processing data and emit anything that might be left.
    /// Takes a `data` buffer of input data, that is potentially empty,
    /// especially if this is the first preprocessor in a chain.
    ///
    /// # Errors
    ///
    /// * if finishing fails for some reason lol
    fn finish(&mut self, _data: Option<&[u8]>) -> Result<Vec<Vec<u8>>> {
        Ok(vec![])
    }
}

/// Lookup a preprocessor implementation via its configuration
///
/// # Errors
///
///   * Errors if the preprocessor is not known

pub fn lookup_with_config(config: &PreprocessorConfig) -> Result<Box<dyn Preprocessor>> {
    match config.name.as_str() {
        "separate" => Ok(Box::new(Separate::from_config(&config.config)?)),
        "base64" => Ok(Box::new(Base64::default())),
        "decompress" => Ok(Box::new(decompress::Decompress::from_config(
            config.config.as_ref(),
        )?)),
        "remove-empty" => Ok(Box::new(FilterEmpty::default())),
        "gelf-chunking" => Ok(Box::new(gelf::Gelf::default())),
        "ingest-ns" => Ok(Box::new(ExtractIngestTs {})),
        "length-prefixed" => Ok(Box::new(LengthPrefix::default())),
        "textual-length-prefix" => Ok(Box::new(TextualLength::default())),
        name => Err(format!("Preprocessor '{}' not found.", name).into()),
    }
}

/// Lookup a preprocessor implementation via its unique id
///
/// # Errors
///
/// * if the preprocessor with `name` is not known
pub fn lookup(name: &str) -> Result<Box<dyn Preprocessor>> {
    lookup_with_config(&PreprocessorConfig::from(name))
}

/// Given the slice of preprocessor names: Look them up and return them as `Preprocessors`.
///
/// # Errors
///
///   * If the preprocessor is not known.
pub fn make_preprocessors(preprocessors: &[PreprocessorConfig]) -> Result<Preprocessors> {
    preprocessors.iter().map(lookup_with_config).collect()
}

/// Canonical way to preprocess data before it is fed to a codec for decoding.
///
/// Preprocessors might split up the given data in multiple chunks. Each of those
/// chunks must be seperately decoded by a `Codec`.
///
/// # Errors
///
///   * If a preprocessor failed
pub fn preprocess(
    preprocessors: &mut [Box<dyn Preprocessor>],
    ingest_ns: &mut u64,
    data: Vec<u8>,
    alias: &Alias,
) -> Result<Vec<Vec<u8>>> {
    let mut data = vec![data];
    let mut data1 = Vec::new();
    for pp in preprocessors {
        data1.clear();
        for (i, d) in data.iter().enumerate() {
            match pp.process(ingest_ns, d) {
                Ok(mut r) => data1.append(&mut r),
                Err(e) => {
                    error!("[Connector::{alias}] Preprocessor [{i}] error: {e}");
                    return Err(e);
                }
            }
        }
        std::mem::swap(&mut data, &mut data1);
    }
    Ok(data)
}

/// Canonical way to finish preprocessors up
///
/// # Errors
///
/// * If a preprocessor failed
pub fn finish(preprocessors: &mut [Box<dyn Preprocessor>], alias: &Alias) -> Result<Vec<Vec<u8>>> {
    if let Some((head, tail)) = preprocessors.split_first_mut() {
        let mut data = match head.finish(None) {
            Ok(d) => d,
            Err(e) => {
                error!(
                    "[Connector::{alias}] Preprocessor '{}' finish error: {e}",
                    head.name()
                );
                return Err(e);
            }
        };
        let mut data1 = Vec::new();
        for pp in tail {
            data1.clear();
            for d in &data {
                match pp.finish(Some(d)) {
                    Ok(mut r) => data1.append(&mut r),
                    Err(e) => {
                        error!(
                            "[Connector::{alias}] Preprocessor '{}' finish error: {e}",
                            pp.name()
                        );
                        return Err(e);
                    }
                }
            }
            std::mem::swap(&mut data, &mut data1);
        }
        Ok(data)
    } else {
        Ok(vec![])
    }
}

pub(crate) use separate::Separate;

#[derive(Default, Debug, Clone)]
pub(crate) struct FilterEmpty {}

impl Preprocessor for FilterEmpty {
    fn name(&self) -> &str {
        "remove-empty"
    }
    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        if data.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![data.to_vec()])
        }
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct ExtractIngestTs {}
impl Preprocessor for ExtractIngestTs {
    fn name(&self) -> &str {
        "ingest-ts"
    }

    fn process(&mut self, ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use std::io::Cursor;
        if let Some(d) = data.get(8..) {
            *ingest_ns = Cursor::new(data).read_u64::<BigEndian>()?;
            Ok(vec![d.to_vec()])
        } else {
            Err(Error::from("Extract Ingest Ts Preprocessor: < 8 byte"))
        }
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct Base64 {}
impl Preprocessor for Base64 {
    fn name(&self) -> &str {
        "base64"
    }

    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(vec![base64::decode(data)?])
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct LengthPrefix {
    len: Option<usize>,
    buffer: BytesMut,
}
impl Preprocessor for LengthPrefix {
    fn name(&self) -> &str {
        "length-prefix"
    }

    #[allow(clippy::cast_possible_truncation)]
    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        self.buffer.extend(data);

        let mut res = Vec::new();
        loop {
            if let Some(l) = self.len {
                if self.buffer.len() >= l {
                    let mut part = self.buffer.split_off(l);
                    std::mem::swap(&mut part, &mut self.buffer);
                    res.push(part.to_vec());
                    self.len = None;
                } else {
                    break;
                }
            }
            if self.buffer.len() > 8 {
                self.len = Some(BigEndian::read_u64(&self.buffer) as usize);
                self.buffer.advance(8);
            } else {
                break;
            }
        }
        Ok(res)
    }
}
#[derive(Clone, Default, Debug)]
pub(crate) struct TextualLength {
    len: Option<usize>,
    buffer: BytesMut,
}
impl Preprocessor for TextualLength {
    fn name(&self) -> &str {
        "textual-length-prefix"
    }

    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        self.buffer.extend(data);

        let mut res = Vec::new();
        loop {
            if let Some(l) = self.len {
                if self.buffer.len() >= l {
                    let mut part = self.buffer.split_off(l);
                    std::mem::swap(&mut part, &mut self.buffer);
                    res.push(part.to_vec());
                    self.len = None;
                } else {
                    break;
                }
            }

            // find the whitespace
            if let Some((i, _c)) = self
                .buffer
                .iter()
                .enumerate()
                .find(|(_i, c)| c.is_ascii_whitespace())
            {
                let mut buf = self.buffer.split_off(i);
                std::mem::swap(&mut buf, &mut self.buffer);
                // parse the textual length
                let l = str::from_utf8(&buf)?;
                self.len = Some(l.parse::<u32>()? as usize);
                self.buffer.advance(1); // advance beyond the whitespace delimiter
            } else {
                // no whitespace found
                break;
            }
        }
        Ok(res)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::postprocessor::{self as post, separate::Separate as SeparatePost, Postprocessor};
    use crate::preprocessor::{self as pre, separate::Separate, Preprocessor};
    #[test]
    fn ingest_ts() -> Result<()> {
        let mut pre_p = pre::ExtractIngestTs {};
        let mut post_p = post::AttachIngresTs {};

        let data = vec![1_u8, 2, 3];

        let encoded = post_p.process(42, 23, &data)?.pop().ok_or("no data")?;

        let mut in_ns = 0u64;
        let decoded = pre_p
            .process(&mut in_ns, &encoded)?
            .pop()
            .ok_or("no data")?;

        assert!(pre_p.finish(None)?.is_empty());

        assert_eq!(data, decoded);
        assert_eq!(in_ns, 42);

        // data too short
        assert!(pre_p.process(&mut in_ns, &[0_u8]).is_err());
        Ok(())
    }

    fn textual_prefix(len: usize) -> String {
        format!("{} {}", len, String::from_utf8_lossy(&vec![b'O'; len]))
    }

    use proptest::prelude::*;

    // generate multiple chopped length-prefixed strings
    fn multiple_textual_lengths(max_elements: usize) -> BoxedStrategy<(Vec<usize>, Vec<String>)> {
        proptest::collection::vec(".+", 1..max_elements) // generator for Vec<String> of arbitrary strings, maximum length of vector: `max_elements`
            .prop_map(|ss| {
                let s: (Vec<usize>, Vec<String>) = ss
                    .into_iter()
                    .map(|s| (s.len(), format!("{} {}", s.len(), s))) // for each string, extract the length, and create a textual length prefix
                    .unzip();
                s
            })
            .prop_map(|tuple| (tuple.0, tuple.1.join(""))) // generator for a tuple of 1. the sizes of the length prefixed strings, 2. the concatenated length prefixed strings as one giant string
            .prop_map(|tuple| {
                // here we chop the big string into up to 4 bits
                let mut chopped = Vec::with_capacity(4);
                let mut giant_string: String = tuple.1.clone();
                while !giant_string.is_empty() && chopped.len() < 4 {
                    // verify we are at a char boundary
                    let mut indices = giant_string.char_indices();
                    let num_chars = giant_string.chars().count();
                    if let Some((index, _)) = indices.nth(num_chars / 2) {
                        let mut splitted = giant_string.split_off(index);
                        std::mem::swap(&mut splitted, &mut giant_string);
                        chopped.push(splitted);
                    } else {
                        break;
                    }
                }
                chopped.push(giant_string);
                (tuple.0, chopped)
            })
            .boxed()
    }

    proptest! {
        #[test]
        fn textual_length_prefix_prop((lengths, datas) in multiple_textual_lengths(5)) {
            let mut pre_p = pre::TextualLength::default();
            let mut in_ns = 0_u64;
            let res: Vec<Vec<u8>> = datas.into_iter().flat_map(|data| {
                pre_p.process(&mut in_ns, data.as_bytes()).unwrap_or_default()
            }).collect();
            assert_eq!(lengths.len(), res.len());
            for (processed, expected_len) in res.iter().zip(lengths) {
                assert_eq!(expected_len, processed.len());
            }
        }

        #[test]
        fn textual_length_pre_post(length in 1..100_usize) {
            let data = vec![1_u8; length];
            let mut pre_p = pre::TextualLength::default();
            let mut post_p = post::TextualLength::default();
            let encoded = post_p.process(0, 0, &data).unwrap_or_default().pop().unwrap_or_default();
            let mut in_ns = 0_u64;
            let mut res = pre_p.process(&mut in_ns, &encoded).unwrap_or_default();
            assert_eq!(1, res.len());
            let payload = res.pop().unwrap_or_default();
            assert_eq!(length, payload.len());
        }
    }

    #[test]
    fn textual_prefix_length_loop() {
        let datas = vec![
            "24 \'?\u{d617e}ѨR\u{202e}\u{f8f7c}\u{ede29}\u{ac784}36 ?{¥?MȺ\r\u{bac41}9\u{5bbbb}\r\u{1c46c}\u{4ba79}¥\u{7f}*?:\u{0}$i",
            "60 %\u{a825a}\u{a4269}\u{39e0c}\u{b3e21}<ì\u{f6c20}ѨÛ`HW\u{9523f}V",
            "\u{3}\u{605fe}%Fq\u{89b5e}\u{93780}Q3",
            "¥?\u{feff}9",
            " \'�2\u{4269b}",
        ];
        let lengths: Vec<usize> = vec![24, 36, 60, 9];
        let mut pre_p = pre::TextualLength::default();
        let mut in_ns = 0_u64;
        let res: Vec<Vec<u8>> = datas
            .into_iter()
            .flat_map(|data| {
                pre_p
                    .process(&mut in_ns, data.as_bytes())
                    .unwrap_or_default()
            })
            .collect();
        assert_eq!(lengths.len(), res.len());
        for (processed, expected_len) in res.iter().zip(lengths) {
            assert_eq!(expected_len, processed.len());
        }
    }

    #[test]
    fn textual_length_prefix() {
        let mut pre_p = pre::TextualLength::default();
        let data = textual_prefix(42);
        let mut in_ns = 0_u64;
        let mut res = pre_p
            .process(&mut in_ns, data.as_bytes())
            .unwrap_or_default();
        assert_eq!(1, res.len());
        let payload = res.pop().unwrap_or_default();
        assert_eq!(42, payload.len());
    }

    #[test]
    fn empty_textual_prefix() {
        let data = ("").as_bytes();
        let mut pre_p = pre::TextualLength::default();
        let mut post_p = post::TextualLength::default();
        let mut in_ns = 0_u64;
        let res = pre_p.process(&mut in_ns, data).unwrap_or_default();
        assert_eq!(0, res.len());

        let data_empty = vec![];
        let encoded = post_p
            .process(42, 23, &data_empty)
            .unwrap_or_default()
            .pop()
            .unwrap_or_default();
        assert_eq!("0 ", String::from_utf8_lossy(&encoded));
        let mut res2 = pre_p.process(&mut in_ns, &encoded).unwrap_or_default();
        assert_eq!(1, res2.len());
        let payload = res2.pop().unwrap_or_default();
        assert_eq!(0, payload.len());
    }

    #[test]
    fn length_prefix() -> Result<()> {
        let mut it = 0;

        let pre_p = pre::LengthPrefix::default();
        let mut post_p = post::LengthPrefix::default();

        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let wire = post_p.process(0, 0, &data)?;
        let (start, end) = wire[0].split_at(7);
        let alias = Alias::new("test", "test");
        let mut pps: Vec<Box<dyn Preprocessor>> = vec![Box::new(pre_p)];
        let recv = preprocess(pps.as_mut_slice(), &mut it, start.to_vec(), &alias)?;
        assert!(recv.is_empty());
        let recv = preprocess(pps.as_mut_slice(), &mut it, end.to_vec(), &alias)?;
        assert_eq!(recv[0], data);

        // incomplete data
        let processed = preprocess(pps.as_mut_slice(), &mut it, start.to_vec(), &alias)?;
        assert!(processed.is_empty());
        // not emitted upon finish
        let finished = finish(pps.as_mut_slice(), &alias)?;
        assert!(finished.is_empty());

        Ok(())
    }

    const LOOKUP_TABLE: [&str; 8] = [
        "separate",
        "base64",
        "decompress",
        "remove-empty",
        "gelf-chunking",
        "ingest-ns",
        "length-prefixed",
        "textual-length-prefix",
    ];

    #[test]
    fn test_lookup() {
        for t in &LOOKUP_TABLE {
            assert!(lookup(t).is_ok());
        }
        let t = "snot";
        assert!(lookup(t).is_err());
    }

    #[test]
    fn test_filter_empty() {
        let mut pre = FilterEmpty::default();
        assert_eq!(Ok(vec![]), pre.process(&mut 0_u64, &[]));
        assert_eq!(Ok(vec![]), pre.finish(None));
    }

    #[test]
    fn test_filter_null() {
        let mut pre = FilterEmpty::default();
        assert_eq!(Ok(vec![]), pre.process(&mut 0_u64, &[]));
        assert_eq!(Ok(vec![]), pre.finish(None));
    }

    #[test]
    fn test_lines() -> Result<()> {
        let int = "snot\nbadger".as_bytes();
        let enc = "snot\nbadger\n".as_bytes(); // First event ( event per line )
        let out = "snot".as_bytes();

        let mut post = SeparatePost::default();
        let mut pre = Separate::default();

        let mut ingest_ns = 0_u64;
        let egress_ns = 1_u64;

        let r = post.process(ingest_ns, egress_ns, int);
        assert!(r.is_ok(), "Expected Ok(...), Got: {r:?}");
        let ext = &r?[0];
        let ext = ext.as_slice();
        // Assert actual encoded form is as expected
        assert_eq!(enc, ext);

        let r = pre.process(&mut ingest_ns, ext);
        let out2 = &r?[0];
        let out2 = out2.as_slice();
        // Assert actual decoded form is as expected
        assert_eq!(out, out2);

        // assert empty finish, no leftovers
        assert!(pre.finish(None)?.is_empty());
        Ok(())
    }

    #[test]
    fn test_separate_buffered() -> Result<()> {
        let input = "snot\nbadger\nwombat\ncapybara\nquagga".as_bytes();
        let mut pre = Separate::new(b'\n', 1000, true);
        let mut ingest_ns = 0_u64;
        let mut res = pre.process(&mut ingest_ns, input)?;
        let splitted = input
            .split(|c| *c == b'\n')
            .map(<[u8]>::to_vec)
            .collect::<Vec<_>>();
        assert_eq!(splitted[..splitted.len() - 1].to_vec(), res);
        let mut finished = pre.finish(None)?;
        res.append(&mut finished);
        assert_eq!(splitted, res);
        Ok(())
    }

    macro_rules! assert_separate_no_buffer {
        ($inbound:expr, $outbound1:expr, $outbound2:expr, $case_number:expr, $separator:expr) => {
            let mut ingest_ns = 0_u64;
            let r = crate::preprocessor::Separate::new($separator, 0, false)
                .process(&mut ingest_ns, $inbound);

            let out = &r?;
            // Assert preprocessor output is as expected
            assert!(
                2 == out.len(),
                "Test case : {} => expected output = {}, actual output = {}",
                $case_number,
                "2",
                out.len()
            );
            assert!(
                $outbound1 == out[0].as_slice(),
                "Test case : {} => expected output = \"{}\", actual output = \"{}\"",
                $case_number,
                std::str::from_utf8($outbound1).unwrap(),
                std::str::from_utf8(out[0].as_slice()).unwrap()
            );
            assert!(
                $outbound2 == out[1].as_slice(),
                "Test case : {} => expected output = \"{}\", actual output = \"{}\"",
                $case_number,
                std::str::from_utf8($outbound2).unwrap(),
                std::str::from_utf8(out[1].as_slice()).unwrap()
            );
        };
    }

    #[allow(clippy::type_complexity)]
    #[test]
    fn test_separate_no_buffer_no_maxlength() -> Result<()> {
        let test_data: [(&'static [u8], &'static [u8], &'static [u8], &'static str); 4] = [
            (b"snot\nbadger", b"snot", b"badger", "0"),
            (b"snot\n", b"snot", b"", "1"),
            (b"\nsnot", b"", b"snot", "2"),
            (b"\n", b"", b"", "3"),
        ];
        for case in &test_data {
            assert_separate_no_buffer!(case.0, case.1, case.2, case.3, b'\n');
        }

        Ok(())
    }

    #[allow(clippy::type_complexity)]
    #[test]
    fn test_carriage_return_no_buffer_no_maxlength() -> Result<()> {
        let test_data: [(&'static [u8], &'static [u8], &'static [u8], &'static str); 4] = [
            (b"snot\rbadger", b"snot", b"badger", "0"),
            (b"snot\r", b"snot", b"", "1"),
            (b"\rsnot", b"", b"snot", "2"),
            (b"\r", b"", b"", "3"),
        ];
        for case in &test_data {
            assert_separate_no_buffer!(case.0, case.1, case.2, case.3, b'\r');
        }

        Ok(())
    }

    #[test]
    fn test_base64() -> Result<()> {
        let int = "snot badger".as_bytes();
        let enc = "c25vdCBiYWRnZXI=".as_bytes();

        let mut pre = crate::preprocessor::Base64::default();
        let mut post = crate::postprocessor::Base64::default();

        // Fake ingest_ns and egress_ns
        let mut ingest_ns = 0_u64;
        let egress_ns = 1_u64;

        let r = post.process(ingest_ns, egress_ns, int);
        let ext = &r?[0];
        let ext = ext.as_slice();
        // Assert actual encoded form is as expected
        assert_eq!(&enc, &ext);

        let r = pre.process(&mut ingest_ns, ext);
        let out = &r?[0];
        let out = out.as_slice();
        // Assert actual decoded form is as expected
        assert_eq!(&int, &out);

        // assert empty finish, no leftovers
        assert!(pre.finish(None)?.is_empty());
        Ok(())
    }
}
