// Copyright 2020, The Tremor Team
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
pub(crate) use gelf::GELF;
pub(crate) mod lines;

use crate::errors::Result;
use crate::url::TremorURL;
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use bytes::buf::Buf;
use bytes::BytesMut;

use std::io::{self, Read};

//pub type Lines = lines::Lines;

/// A set of preprocessors
pub type Preprocessors = Vec<Box<dyn Preprocessor>>;

/// Preprocessor trait
pub trait Preprocessor: Sync + Send {
    /// Canonical name for this preprocessor
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str;
    /// process data
    ///
    /// # Errors
    ///
    /// * Errors if the data can not processed
    fn process(&mut self, ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>>;
}

/// Lookup a preprocessor implementation via its unique id
///
/// # Errors
///
///   * Errors if the preprocessor is not known
#[cfg(not(tarpaulin_include))]
pub fn lookup(name: &str) -> Result<Box<dyn Preprocessor>> {
    match name {
        // TODO once preprocessors allow configuration, remove multiple entries for lines here
        "lines" => Ok(Box::new(Lines::new('\n', 1_048_576, true))),
        "lines-null" => Ok(Box::new(Lines::new('\0', 1_048_576, true))),
        "lines-pipe" => Ok(Box::new(Lines::new('|', 1_048_576, true))),
        "lines-no-buffer" => Ok(Box::new(Lines::new('\n', 0, false))),
        "lines-cr-no-buffer" => Ok(Box::new(Lines::new('\r', 0, false))),
        "base64" => Ok(Box::new(Base64::default())),
        "gzip" => Ok(Box::new(Gzip::default())),
        "zlib" => Ok(Box::new(Zlib::default())),
        "xz2" => Ok(Box::new(Xz2::default())),
        "snappy" => Ok(Box::new(Snappy::default())),
        "lz4" => Ok(Box::new(Lz4::default())),
        "decompress" => Ok(Box::new(Decompress {})),
        "remove-empty" => Ok(Box::new(FilterEmpty::default())),
        "gelf-chunking" => Ok(Box::new(GELF::default())),
        "gelf-chunking-tcp" => Ok(Box::new(GELF::tcp())),
        "ingest-ns" => Ok(Box::new(ExtractIngresTs {})),
        "length-prefixed" => Ok(Box::new(LengthPrefix::default())),
        _ => Err(format!("Preprocessor '{}' not found.", name).into()),
    }
}

/// Given the slice of preprocessor names: Look them up and return them as `Preprocessors`.
///
/// # Errors
///
///   * If the preprocessor is not known.
pub fn make_preprocessors(preprocessors: &[String]) -> Result<Preprocessors> {
    preprocessors.iter().map(|n| lookup(&n)).collect()
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
    instance_id: &TremorURL,
) -> Result<Vec<Vec<u8>>> {
    let mut data = vec![data];
    let mut data1 = Vec::new();
    for pp in preprocessors {
        data1.clear();
        for (i, d) in data.iter().enumerate() {
            match pp.process(ingest_ns, d) {
                Ok(mut r) => data1.append(&mut r),
                Err(e) => {
                    error!("[{}] Preprocessor[{}] error {}", instance_id, i, e);
                    return Err(e);
                }
            }
        }
        std::mem::swap(&mut data, &mut data1);
    }
    Ok(data)
}

trait SliceTrim {
    fn trim(&self) -> &Self;
}

impl SliceTrim for [u8] {
    fn trim(&self) -> &[u8] {
        fn is_not_whitespace(c: u8) -> bool {
            c != b'\t' && c != b' '
        }

        if let Some(first) = self.iter().position(|v| is_not_whitespace(*v)) {
            if let Some(last) = self.iter().rposition(|v| is_not_whitespace(*v)) {
                &self[first..=last]
            } else {
                &self[first..]
            }
        } else {
            &[]
        }
    }
}

pub(crate) use lines::Lines;

#[derive(Default, Debug, Clone)]
pub(crate) struct FilterEmpty {}

impl Preprocessor for FilterEmpty {
    #[cfg(not(tarpaulin_include))]
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
pub(crate) struct Nulls {}
impl Preprocessor for Nulls {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "nulls"
    }

    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(data.split(|c| *c == b'\0').map(Vec::from).collect())
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct ExtractIngresTs {}
impl Preprocessor for ExtractIngresTs {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "extract-ingress-ts"
    }

    fn process(&mut self, ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use std::io::Cursor;
        *ingest_ns = Cursor::new(data).read_u64::<BigEndian>()?;
        Ok(vec![data[8..].to_vec()])
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct Base64 {}
impl Preprocessor for Base64 {
    #[cfg(not(tarpaulin_include))]
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
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "gzip"
    }

    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use libflate::gzip::MultiDecoder;
        let mut decoder = MultiDecoder::new(&data[..])?;
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(vec![decompressed])
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct Zlib {}
impl Preprocessor for Zlib {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "zlib"
    }

    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use libflate::zlib::Decoder;
        let mut decoder = Decoder::new(&data[..])?;
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(vec![decompressed])
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct Xz2 {}
impl Preprocessor for Xz2 {
    #[cfg(not(tarpaulin_include))]
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
    #[cfg(not(tarpaulin_include))]
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
    #[cfg(not(tarpaulin_include))]
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
    #[cfg(not(tarpaulin_include))]
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
            Some(&[0x78, 0x01, _, _, _, _])
            | Some(&[0x78, 0x5e, _, _, _, _])
            | Some(&[0x78, 0x9c, _, _, _, _])
            | Some(&[0x78, 0xda, _, _, _, _]) => {
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
    #[cfg(not(tarpaulin_include))]
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
#[cfg(test)]
mod test {
    use super::*;
    use crate::postprocessor::{self as post, Postprocessor};
    use crate::preprocessor::{self as pre, Preprocessor};
    #[test]
    fn ts() {
        let mut pre_p = pre::ExtractIngresTs {};
        let mut post_p = post::AttachIngresTs {};

        let data = vec![1_u8, 2, 3];

        let encoded = post_p.process(42, 23, &data).unwrap().pop().unwrap();

        let mut in_ns = 0u64;
        let decoded = pre_p.process(&mut in_ns, &encoded).unwrap().pop().unwrap();

        assert_eq!(data, decoded);
        assert_eq!(in_ns, 42);
    }

    #[test]
    fn length_prefix() -> Result<()> {
        let mut it = 0;

        let pre_p = pre::LengthPrefix::default();
        let mut post_p = post::LengthPrefix::default();

        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let wire = post_p.process(0, 0, &data)?;
        let (start, end) = wire[0].split_at(7);
        let id = TremorURL::parse("/onramp/snot/00").unwrap();
        let mut pps: Vec<Box<dyn Preprocessor>> = vec![Box::new(pre_p)];
        let recv = preprocess(pps.as_mut_slice(), &mut it, start.to_vec(), &id)?;
        assert!(recv.is_empty());
        let recv = preprocess(pps.as_mut_slice(), &mut it, end.to_vec(), &id)?;
        assert_eq!(recv[0], data);
        Ok(())
    }

    const LOOKUP_TABLE: [&str; 15] = [
        "lines",
        "lines-null",
        "lines-pipe",
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
        Ok(())
    }

    #[test]
    fn test_filter_null() -> Result<()> {
        let mut pre = FilterEmpty::default();
        assert_eq!(Ok(vec![]), pre.process(&mut 0_u64, &vec![]));
        Ok(())
    }

    macro_rules! assert_decompress {
        ($internal:expr, $which:ident, $magic:expr) => {
            // Assert pre and post processors have a sensible default() ctor
            let mut inbound = crate::preprocessor::Decompress::default();
            let mut outbound = crate::postprocessor::$which::default();

            // Fake ingest_ns and egress_ns
            let mut ingest_ns = 0_u64;
            let egress_ns = 1_u64;

            let r = outbound.process(ingest_ns, egress_ns, $internal);
            let ext = &r?[0];
            let ext = ext.as_slice();
            // Assert actual encoded form is as expected ( magic code only )
            assert_eq!($magic, decode_magic(&ext));

            let r = inbound.process(&mut ingest_ns, &ext);
            let out = &r?[0];
            let out = out.as_slice();
            // Assert actual decoded form is as expected
            assert_eq!(&$internal, &out);
        };
    }
    macro_rules! assert_simple_symmetric {
        ($internal:expr, $which:ident, $magic:expr) => {
            // Assert pre and post processors have a sensible default() ctor
            let mut inbound = crate::preprocessor::$which::default();
            let mut outbound = crate::postprocessor::$which::default();

            // Fake ingest_ns and egress_ns
            let mut ingest_ns = 0_u64;
            let egress_ns = 1_u64;

            let r = outbound.process(ingest_ns, egress_ns, $internal);
            let ext = &r?[0];
            let ext = ext.as_slice();
            // Assert actual encoded form is as expected ( magic code only )
            assert_eq!($magic, decode_magic(&ext));

            let r = inbound.process(&mut ingest_ns, &ext);
            let out = &r?[0];
            let out = out.as_slice();
            // Assert actual decoded form is as expected
            assert_eq!(&$internal, &out);
        };
    }

    macro_rules! assert_symmetric {
        ($inbound:expr, $outbound:expr, $which:ident) => {
            // Assert pre and post processors have a sensible default() ctor
            let mut inbound = crate::preprocessor::$which::default();
            let mut outbound = crate::postprocessor::$which::default();

            // Fake ingest_ns and egress_ns
            let mut ingest_ns = 0_u64;
            let egress_ns = 1_u64;

            let enc = $outbound.clone();
            let r = outbound.process(ingest_ns, egress_ns, $inbound);
            let ext = &r?[0];
            let ext = ext.as_slice();
            // Assert actual encoded form is as expected
            assert_eq!(&enc, &ext);

            let r = inbound.process(&mut ingest_ns, &ext);
            let out = &r?[0];
            let out = out.as_slice();
            // Assert actual decoded form is as expected
            assert_eq!(&$inbound, &out);
        };
    }

    macro_rules! assert_not_symmetric {
        ($inbound:expr, $internal:expr, $outbound:expr, $which:ident) => {
            // Assert pre and post processors have a sensible default() ctor
            let mut inbound = crate::preprocessor::$which::default();
            let mut outbound = crate::postprocessor::$which::default();

            // Fake ingest_ns and egress_ns
            let mut ingest_ns = 0_u64;
            let egress_ns = 1_u64;

            let enc = $internal.clone();
            let r = outbound.process(ingest_ns, egress_ns, $inbound);
            let ext = &r?[0];
            let ext = ext.as_slice();
            // Assert actual encoded form is as expected
            assert_eq!(&enc, &ext);

            let r = inbound.process(&mut ingest_ns, &ext);
            let out = &r?[0];
            let out = out.as_slice();
            // Assert actual decoded form is as expected
            assert_eq!(&$outbound, &out);
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
            _ => "fail/unknown",
        }
    }

    #[test]
    fn test_lines() -> Result<()> {
        let int = "snot\nbadger".as_bytes();
        let enc = "snot\nbadger\n".as_bytes(); // First event ( event per line )
        let out = "snot".as_bytes();
        assert_not_symmetric!(int, enc, out, Lines);
        Ok(())
    }

    macro_rules! assert_no_buffer {
        ($inbound:expr, $outbound1:expr, $outbound2:expr, $case_number:expr, $separator:expr) => {
            let mut ingest_ns = 0_u64;
            let r = crate::preprocessor::Lines::new($separator, 0, false)
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
        let test_data = get_data_for_test_lines_no_buffer_no_maxlength();
        for case in &test_data {
            assert_no_buffer!(case.0, case.1, case.2, case.3, '\n');
        }

        Ok(())
    }

    fn get_data_for_test_lines_no_buffer_no_maxlength(
    ) -> [(&'static [u8], &'static [u8], &'static [u8], &'static str); 5] {
        [
            (b"snot\nbadger", b"snot", b"badger", "0"),
            (b"snot\nbadger\n", b"snot", b"badger", "1"),
            (b"snot\nbadger\n\n", b"snot", b"badger", "2"),
            (b"snot\n\nbadger", b"snot", b"badger", "3"),
            (b"\nsnot\nbadger", b"snot", b"badger", "4"),
        ]
    }

    #[test]
    fn test_carriage_return_no_buffer_no_maxlength() -> Result<()> {
        let test_data = get_data_for_test_carriage_return_no_buffer_no_maxlength();
        for case in &test_data {
            assert_no_buffer!(case.0, case.1, case.2, case.3, '\r');
        }

        Ok(())
    }

    fn get_data_for_test_carriage_return_no_buffer_no_maxlength(
    ) -> [(&'static [u8], &'static [u8], &'static [u8], &'static str); 5] {
        [
            (b"snot\rbadger", b"snot", b"badger", "0"),
            (b"snot\rbadger\r", b"snot", b"badger", "1"),
            (b"snot\rbadger\r\r", b"snot", b"badger", "2"),
            (b"snot\r\rbadger", b"snot", b"badger", "3"),
            (b"\rsnot\rbadger", b"snot", b"badger", "4"),
        ]
    }

    #[test]
    fn test_base64() -> Result<()> {
        let int = "snot badger".as_bytes();
        let enc = "c25vdCBiYWRnZXI=".as_bytes();
        assert_symmetric!(int, enc, Base64);
        Ok(())
    }

    #[test]
    fn test_gzip() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric!(int, Gzip, "gzip");
        assert_decompress!(int, Lz4, "lz4");
        Ok(())
    }

    #[test]
    fn test_zlib() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric!(int, Zlib, "zlib");
        assert_decompress!(int, Lz4, "lz4");
        Ok(())
    }

    #[test]
    fn test_snappy() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric!(int, Snappy, "snap");
        assert_decompress!(int, Lz4, "lz4");
        Ok(())
    }

    #[test]
    fn test_xz2() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric!(int, Xz2, "xz2");
        assert_decompress!(int, Lz4, "lz4");
        Ok(())
    }

    #[test]
    fn test_lz4() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric!(int, Lz4, "lz4");
        assert_decompress!(int, Lz4, "lz4");
        Ok(())
    }
}
