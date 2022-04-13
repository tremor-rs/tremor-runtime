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

mod gelf;
pub(crate) use gelf::Gelf;
pub(crate) mod split;

use crate::config::Preprocessor as PreprocessorConfig;
use crate::errors::{Error, Result};
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use bytes::buf::Buf;
use bytes::BytesMut;
use std::str;

use std::io::{self, Read};

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
        "split" => Ok(Box::new(Split::from_config(&config.config)?)),
        "lines" => Ok(Box::new(Split::default())),
        "base64" => Ok(Box::new(Base64::default())),
        "gzip" => Ok(Box::new(Gzip::default())),
        "zlib" => Ok(Box::new(Zlib::default())),
        "xz2" => Ok(Box::new(Xz2::default())),
        "snappy" => Ok(Box::new(Snappy::default())),
        "lz4" => Ok(Box::new(Lz4::default())),
        "decompress" => Ok(Box::new(Decompress {})),
        "remove-empty" => Ok(Box::new(FilterEmpty::default())),
        "gelf-chunking" => Ok(Box::new(Gelf::default())),
        "gelf-chunking-tcp" => Ok(Box::new(Gelf::tcp())),
        "ingest-ns" => Ok(Box::new(ExtractIngestTs {})),
        "length-prefixed" => Ok(Box::new(LengthPrefix::default())),
        "textual-length-prefix" => Ok(Box::new(TextualLength::default())),
        "zstd" => Ok(Box::new(Zstd::default())),
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
    alias: &str,
) -> Result<Vec<Vec<u8>>> {
    let mut data = vec![data];
    let mut data1 = Vec::new();
    for pp in preprocessors {
        data1.clear();
        for (i, d) in data.iter().enumerate() {
            match pp.process(ingest_ns, d) {
                Ok(mut r) => data1.append(&mut r),
                Err(e) => {
                    error!("[{}] Preprocessor [{}] error: {}", alias, i, e);
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
pub fn finish(
    preprocessors: &mut [Box<dyn Preprocessor>],
    instance_id: &str,
) -> Result<Vec<Vec<u8>>> {
    if let Some((head, tail)) = preprocessors.split_first_mut() {
        let mut data = match head.finish(None) {
            Ok(d) => d,
            Err(e) => {
                error!(
                    "[{instance_id}] Preprocessor '{}' finish error: {e}",
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
                            "[{instance_id}] Preprocessor '{}' finish error: {e}",
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

pub(crate) use split::Split;

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
        Ok(vec![base64::decode(&data)?])
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct Gzip {}
impl Preprocessor for Gzip {
    fn name(&self) -> &str {
        "gzip"
    }

    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use libflate::gzip::MultiDecoder;
        let mut decoder = MultiDecoder::new(data)?;
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(vec![decompressed])
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct Zlib {}
impl Preprocessor for Zlib {
    fn name(&self) -> &str {
        "zlib"
    }

    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use libflate::zlib::Decoder;
        let mut decoder = Decoder::new(data)?;
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(vec![decompressed])
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct Xz2 {}
impl Preprocessor for Xz2 {
    fn name(&self) -> &str {
        "xz2"
    }

    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use xz2::read::XzDecoder as Decoder;
        let mut decoder = Decoder::new(data);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(vec![decompressed])
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct Snappy {}
impl Preprocessor for Snappy {
    fn name(&self) -> &str {
        "snappy"
    }

    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use snap::read::FrameDecoder;
        let mut rdr = FrameDecoder::new(data);
        let decompressed_len = snap::raw::decompress_len(data)?;
        let mut decompressed = Vec::with_capacity(decompressed_len);
        io::copy(&mut rdr, &mut decompressed)?;
        Ok(vec![decompressed])
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct Lz4 {}
impl Preprocessor for Lz4 {
    fn name(&self) -> &str {
        "lz4"
    }

    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use lz4::Decoder;
        let mut decoder = Decoder::new(data)?;
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(vec![decompressed])
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct Decompress {}
impl Preprocessor for Decompress {
    fn name(&self) -> &str {
        "decompress"
    }

    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let r = match data.get(0..6) {
            Some(&[0x1f, 0x8b, _, _, _, _]) => {
                use libflate::gzip::Decoder;
                let mut decoder = Decoder::new(data)?;
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed)?;
                decompressed
            }
            // ZLib magic headers
            Some(&[0x78, 0x01 | 0x5e | 0x9c | 0xda, _, _, _, _]) => {
                use libflate::zlib::Decoder;
                let mut decoder = Decoder::new(data)?;
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed)?;
                decompressed
            }
            Some(&[0xfd, b'7', b'z', b'X', b'Z', 0x00]) => {
                use xz2::read::XzDecoder as Decoder;
                let mut decoder = Decoder::new(data);
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed)?;
                decompressed
            }
            // Some(b"sNaPpY") => {
            Some(&[0xff, _, _, _, _, _]) => {
                use snap::read::FrameDecoder;
                let mut rdr = FrameDecoder::new(data);
                let decompressed_len = snap::raw::decompress_len(data)?;
                let mut decompressed = Vec::with_capacity(decompressed_len);
                io::copy(&mut rdr, &mut decompressed)?;
                decompressed
            }
            Some(&[0x04, 0x22, 0x4D, 0x18, _, _]) => {
                use lz4::Decoder;
                let mut decoder = Decoder::new(data)?;
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed)?;
                decompressed
            }
            // Zstd Magic : 0xFD2FB528 (but little endian)
            Some(&[0x28, 0xb5, 0x2f, 0xfd, _, _]) => zstd::decode_all(data)?,
            _ => data.to_vec(),
        };
        Ok(vec![r])
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

#[derive(Clone, Debug, Default)]
pub(crate) struct Zstd {}
impl Preprocessor for Zstd {
    fn name(&self) -> &str {
        "ztd"
    }
    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let decoded: Vec<u8> = zstd::decode_all(data)?;
        Ok(vec![decoded])
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::postprocessor::{self as post, join::Join, Postprocessor};
    use crate::preprocessor::{self as pre, Preprocessor};
    #[test]
    fn ingest_ts() -> Result<()> {
        let mut pre_p = pre::ExtractIngestTs {};
        let mut post_p = post::AttachIngresTs {};

        let data = vec![1_u8, 2, 3];

        let encoded = post_p.process(42, 23, &data)?.pop().unwrap();

        let mut in_ns = 0u64;
        let decoded = pre_p.process(&mut in_ns, &encoded)?.pop().unwrap();

        assert!(pre_p.finish(None)?.is_empty());

        assert_eq!(data, decoded);
        assert_eq!(in_ns, 42);

        // data too short
        assert!(pre_p.process(&mut in_ns, &[0_u8]).is_err());
        Ok(())
    }

    fn textual_prefix(len: usize) -> String {
        format!("{} {}", len, String::from_utf8(vec![b'O'; len]).unwrap())
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
                while giant_string.len() > 0 && chopped.len() < 4 {
                    // verify we are at a char boundary
                    let indices = giant_string.char_indices();
                    let num_chars = giant_string.chars().count();
                    if let Some((index, _)) = indices.skip(num_chars / 2).next() {
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
            let res: Vec<Vec<u8>> = datas.into_iter().map(|data| {
                pre_p.process(&mut in_ns, data.as_bytes()).unwrap()
            }).flatten().collect();
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
            let encoded = post_p.process(0, 0, &data).unwrap().pop().unwrap();
            let mut in_ns = 0_u64;
            let mut res = pre_p.process(&mut in_ns, &encoded).unwrap();
            assert_eq!(1, res.len());
            let payload = res.pop().unwrap();
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
            .map(|data| pre_p.process(&mut in_ns, data.as_bytes()).unwrap())
            .flatten()
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
        let mut res = pre_p.process(&mut in_ns, data.as_bytes()).unwrap();
        assert_eq!(1, res.len());
        let payload = res.pop().unwrap();
        assert_eq!(42, payload.len());
    }

    #[test]
    fn empty_textual_prefix() {
        let data = ("").as_bytes();
        let mut pre_p = pre::TextualLength::default();
        let mut post_p = post::TextualLength::default();
        let mut in_ns = 0_u64;
        let res = pre_p.process(&mut in_ns, data).unwrap();
        assert_eq!(0, res.len());

        let data_empty = vec![];
        let encoded = post_p.process(42, 23, &data_empty).unwrap().pop().unwrap();
        assert_eq!("0 ", str::from_utf8(&encoded).unwrap());
        let mut res2 = pre_p.process(&mut in_ns, &encoded).unwrap();
        assert_eq!(1, res2.len());
        let payload = res2.pop().unwrap();
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
        let id = String::from("test");
        let mut pps: Vec<Box<dyn Preprocessor>> = vec![Box::new(pre_p)];
        let recv = preprocess(pps.as_mut_slice(), &mut it, start.to_vec(), &id)?;
        assert!(recv.is_empty());
        let recv = preprocess(pps.as_mut_slice(), &mut it, end.to_vec(), &id)?;
        assert_eq!(recv[0], data);

        // incomplete data
        let processed = preprocess(pps.as_mut_slice(), &mut it, start.to_vec(), &id)?;
        assert!(processed.is_empty());
        // not emitted upon finish
        let finished = finish(pps.as_mut_slice(), &id)?;
        assert!(finished.is_empty());

        Ok(())
    }

    const LOOKUP_TABLE: [&str; 15] = [
        "split",
        "base64",
        "gzip",
        "zlib",
        "xz2",
        "snappy",
        "lz4",
        "decompress",
        "remove-empty",
        "gelf-chunking",
        "gelf-chunking-tcp",
        "ingest-ns",
        "length-prefixed",
        "textual-length-prefix",
        "zstd",
    ];

    #[test]
    fn test_lookup() -> Result<()> {
        for t in LOOKUP_TABLE.iter() {
            assert!(lookup(t).is_ok());
        }
        let t = "snot";
        assert!(lookup(&t).is_err());
        Ok(())
    }

    #[test]
    fn test_filter_empty() -> Result<()> {
        let mut pre = FilterEmpty::default();
        assert_eq!(Ok(vec![]), pre.process(&mut 0_u64, &vec![]));
        assert_eq!(Ok(vec![]), pre.finish(None));
        Ok(())
    }

    #[test]
    fn test_filter_null() -> Result<()> {
        let mut pre = FilterEmpty::default();
        assert_eq!(Ok(vec![]), pre.process(&mut 0_u64, &vec![]));
        assert_eq!(Ok(vec![]), pre.finish(None));
        Ok(())
    }

    macro_rules! assert_decompress {
        ($internal:expr, $which:ident, $magic:expr) => {
            // Assert pre and post processors have a sensible default() ctor
            let mut pre = crate::preprocessor::Decompress::default();
            let mut outbound = crate::postprocessor::$which::default();

            // Fake ingest_ns and egress_ns
            let mut ingest_ns = 0_u64;
            let egress_ns = 1_u64;

            let r = outbound.process(ingest_ns, egress_ns, $internal);
            let ext = &r?[0];
            let ext = ext.as_slice();
            // Assert actual encoded form is as expected ( magic code only )
            assert_eq!($magic, decode_magic(&ext));

            let r = pre.process(&mut ingest_ns, &ext);
            let out = &r?[0];
            let out = out.as_slice();
            // Assert actual decoded form is as expected
            assert_eq!(&$internal, &out);

            // empty finish, no leftovers
            assert!(pre.finish(None)?.is_empty());
        };
    }

    macro_rules! assert_simple_symmetric {
        ($internal:expr, $which:ident, $magic:expr) => {
            // Assert pre and post processors have a sensible default() ctor
            let mut pre = crate::preprocessor::$which::default();
            let mut post = crate::postprocessor::$which::default();

            // Fake ingest_ns and egress_ns
            let mut ingest_ns = 0_u64;
            let egress_ns = 1_u64;

            let r = post.process(ingest_ns, egress_ns, $internal);
            let ext = &r?[0];
            let ext = ext.as_slice();
            // Assert actual encoded form is as expected ( magic code only )
            assert_eq!($magic, decode_magic(&ext));

            let r = pre.process(&mut ingest_ns, &ext);
            let out = &r?[0];
            let out = out.as_slice();
            // Assert actual decoded form is as expected
            assert_eq!(&$internal, &out);
            // assert empty finish, no leftovers
            assert!(pre.finish(None)?.is_empty())
        };
    }

    fn decode_magic(data: &[u8]) -> &'static str {
        match data.get(0..6) {
            Some(&[0x1f, 0x8b, _, _, _, _]) => "gzip",
            Some(&[0x78, _, _, _, _, _]) => "zlib",
            Some(&[0xfd, b'7', b'z', _, _, _]) => "xz2",
            Some(b"sNaPpY") => "snap",
            Some(&[0xff, 0x6, 0x0, 0x0, _, _]) => "snap",
            Some(&[0x04, 0x22, 0x4d, 0x18, _, _]) => "lz4",
            Some(&[0x28, 0xb5, 0x2f, 0xfd, _, _]) => "zstd",
            _ => "fail/unknown",
        }
    }

    #[test]
    fn test_lines() -> Result<()> {
        let int = "snot\nbadger".as_bytes();
        let enc = "snot\nbadger\n".as_bytes(); // First event ( event per line )
        let out = "snot".as_bytes();

        let mut post = Join::default();
        let mut pre = Split::default();

        let mut ingest_ns = 0_u64;
        let egress_ns = 1_u64;

        let r = post.process(ingest_ns, egress_ns, int);
        assert!(r.is_ok(), "Expected Ok(...), Got: {r:?}");
        let ext = &r?[0];
        let ext = ext.as_slice();
        // Assert actual encoded form is as expected
        assert_eq!(enc, ext);

        let r = pre.process(&mut ingest_ns, &ext);
        let out2 = &r?[0];
        let out2 = out2.as_slice();
        // Assert actual decoded form is as expected
        assert_eq!(out, out2);

        // assert empty finish, no leftovers
        assert!(pre.finish(None)?.is_empty());
        Ok(())
    }

    #[test]
    fn test_lines_buffered() -> Result<()> {
        let input = "snot\nbadger\nwombat\ncapybara\nquagga".as_bytes();
        let mut pre = Split::new(b'\n', 1000, true);
        let mut ingest_ns = 0_u64;
        let mut res = pre.process(&mut ingest_ns, input)?;
        let splitted = input
            .split(|c| *c == b'\n')
            .map(|line| line.to_vec())
            .collect::<Vec<_>>();
        assert_eq!(splitted[..splitted.len() - 1].to_vec(), res);
        let mut finished = pre.finish(None)?;
        res.append(&mut finished);
        assert_eq!(splitted, res);
        Ok(())
    }

    macro_rules! assert_lines_no_buffer {
        ($inbound:expr, $outbound1:expr, $outbound2:expr, $case_number:expr, $separator:expr) => {
            let mut ingest_ns = 0_u64;
            let r = crate::preprocessor::Split::new($separator, 0, false)
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

    #[test]
    fn test_lines_no_buffer_no_maxlength() -> Result<()> {
        let test_data: [(&'static [u8], &'static [u8], &'static [u8], &'static str); 4] = [
            (b"snot\nbadger", b"snot", b"badger", "0"),
            (b"snot\n", b"snot", b"", "1"),
            (b"\nsnot", b"", b"snot", "2"),
            (b"\n", b"", b"", "3"),
        ];
        for case in &test_data {
            assert_lines_no_buffer!(case.0, case.1, case.2, case.3, b'\n');
        }

        Ok(())
    }

    #[test]
    fn test_carriage_return_no_buffer_no_maxlength() -> Result<()> {
        let test_data: [(&'static [u8], &'static [u8], &'static [u8], &'static str); 4] = [
            (b"snot\rbadger", b"snot", b"badger", "0"),
            (b"snot\r", b"snot", b"", "1"),
            (b"\rsnot", b"", b"snot", "2"),
            (b"\r", b"", b"", "3"),
        ];
        for case in &test_data {
            assert_lines_no_buffer!(case.0, case.1, case.2, case.3, b'\r');
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

        let r = pre.process(&mut ingest_ns, &ext);
        let out = &r?[0];
        let out = out.as_slice();
        // Assert actual decoded form is as expected
        assert_eq!(&int, &out);

        // assert empty finish, no leftovers
        assert!(pre.finish(None)?.is_empty());
        Ok(())
    }

    #[test]
    fn test_gzip() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric!(int, Gzip, "gzip");
        assert_decompress!(int, Gzip, "gzip");
        Ok(())
    }

    #[test]
    fn test_zlib() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric!(int, Zlib, "zlib");
        assert_decompress!(int, Zlib, "zlib");
        Ok(())
    }

    #[test]
    fn test_snappy() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric!(int, Snappy, "snap");
        assert_decompress!(int, Snappy, "snap");
        Ok(())
    }

    #[test]
    fn test_xz2() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric!(int, Xz2, "xz2");
        assert_decompress!(int, Xz2, "xz2");
        Ok(())
    }

    #[test]
    fn test_lz4() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric!(int, Lz4, "lz4");
        assert_decompress!(int, Lz4, "lz4");
        Ok(())
    }
    #[test]
    fn test_zstd() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric!(int, Zstd, "zstd");
        assert_decompress!(int, Zstd, "zstd");
        Ok(())
    }
}
