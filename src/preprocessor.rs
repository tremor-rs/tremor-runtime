// Copyright 2018-2020, Wayfair GmbH
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
pub mod lines;

use crate::errors::*;
use base64;
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use bytes::BytesMut;

pub type Lines = lines::Lines;

pub type Preprocessors = Vec<Box<dyn Preprocessor>>;
pub trait Preprocessor: Sync + Send {
    fn process(&mut self, ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>>;
}

// just a lookup
#[cfg_attr(tarpaulin, skip)]
pub fn lookup(name: &str) -> Result<Box<dyn Preprocessor>> {
    match name {
        // TODO once preprocessors allow configuration, remove multiple entries for lines here
        "lines" => Ok(Box::new(lines::Lines::new('\n', 1_048_576))),
        "lines-null" => Ok(Box::new(lines::Lines::new('\0', 1_048_576))),
        "lines-pipe" => Ok(Box::new(lines::Lines::new('|', 1_048_576))),
        "base64" => Ok(Box::new(Base64::default())),
        "gzip" => Ok(Box::new(Gzip::default())),
        "zlib" => Ok(Box::new(Zlib::default())),
        "xz2" => Ok(Box::new(Xz2::default())),
        "snappy" => Ok(Box::new(Snappy::default())),
        "lz4" => Ok(Box::new(Lz4::default())),
        "decompress" => Ok(Box::new(Decompress {})),
        "remove-empty" => Ok(Box::new(FilterEmpty::default())),
        "gelf-chunking" => Ok(Box::new(GELF::default())),
        "gelf-chunking-tcp" => Ok(Box::new(gelf::GELF::tcp())),
        "ingest-ns" => Ok(Box::new(ExtractIngresTs {})),
        "length-prefixerd" => Ok(Box::new(LengthPrefix::default())),
        _ => Err(format!("Preprocessor '{}' not found.", name).into()),
    }
}

trait SliceTrim {
    fn trim(&self) -> &Self;
}

impl SliceTrim for [u8] {
    #[allow(clippy::all)]
    fn trim(&self) -> &[u8] {
        fn is_not_whitespace(c: &u8) -> bool {
            *c != b'\t' && *c != b' '
        }

        if let Some(first) = self.iter().position(is_not_whitespace) {
            if let Some(last) = self.iter().rposition(is_not_whitespace) {
                &self[first..last + 1]
            } else {
                &self[first..]
            }
        } else {
            &[]
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct FilterEmpty {}

impl Preprocessor for FilterEmpty {
    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        if data.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![data.to_vec()])
        }
    }
}

#[derive(Clone, Default, Debug)]
pub struct Nulls {}
impl Preprocessor for Nulls {
    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(data.split(|c| *c == b'\0').map(Vec::from).collect())
    }
}

#[derive(Clone, Default, Debug)]
pub struct ExtractIngresTs {}
impl Preprocessor for ExtractIngresTs {
    fn process(&mut self, ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use std::io::Cursor;
        *ingest_ns = Cursor::new(data).read_u64::<BigEndian>()?;
        Ok(vec![data[8..].to_vec()])
    }
}

#[derive(Clone, Default, Debug)]
pub struct Base64 {}
impl Preprocessor for Base64 {
    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(vec![base64::decode(&data)?])
    }
}

#[derive(Clone, Default, Debug)]
pub struct Gzip {}
impl Preprocessor for Gzip {
    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use libflate::gzip::MultiDecoder;
        use std::io::Read;
        let mut decoder = MultiDecoder::new(&data[..])?;
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(vec![decompressed])
    }
}

#[derive(Clone, Default, Debug)]
pub struct Zlib {}
impl Preprocessor for Zlib {
    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use libflate::zlib::Decoder;
        use std::io::Read;
        let mut decoder = Decoder::new(&data[..])?;
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(vec![decompressed])
    }
}

#[derive(Clone, Default, Debug)]
pub struct Xz2 {}
impl Preprocessor for Xz2 {
    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use std::io::Read;
        use xz2::read::XzDecoder as Decoder;
        let mut decoder = Decoder::new(data);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(vec![decompressed])
    }
}

#[derive(Clone, Default, Debug)]
pub struct Snappy {}
impl Preprocessor for Snappy {
    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use snap::Reader;
        use std::io::Read;
        let mut reader = Reader::new(data);
        let mut decompressed = Vec::new();
        reader.read_to_end(&mut decompressed)?;
        Ok(vec![decompressed])
    }
}

#[derive(Clone, Default, Debug)]
pub struct Lz4 {}
impl Preprocessor for Lz4 {
    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use lz4::Decoder;
        use std::io::Read;
        let mut decoder = Decoder::new(data)?;
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(vec![decompressed])
    }
}

#[derive(Clone, Default, Debug)]
pub struct Decompress {}
impl Preprocessor for Decompress {
    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use std::io::Read;

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
                use snap::Decoder;
                let mut decoder = Decoder::new();
                let decompressed_len = snap::decompress_len(data)?;
                let mut decompressed = Vec::with_capacity(decompressed_len);
                decoder.decompress(data, &mut decompressed)?;
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
pub struct LengthPrefix {
    len: Option<usize>,
    buffer: BytesMut,
}
impl Preprocessor for LengthPrefix {
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
    fn length_prefix() -> Result<()> {
        let mut it = 0;

        let mut pre_p = pre::LengthPrefix::default();
        let mut post_p = post::LengthPrefix::default();

        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let wire = post_p.process(0, 0, &data)?;
        let (start, end) = wire[0].split_at(7);
        let recv = pre_p.process(&mut it, start)?;
        assert!(recv.is_empty());
        let recv = pre_p.process(&mut it, end)?;
        assert_eq!(recv[0], data);
        Ok(())
    }

    const LOOKUP_TABLE: [&'static str; 15] = [
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
        "length-prefixerd",
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
        assert_eq!(Ok(vec![]), pre.process(&mut 0u64, &vec![]));
        Ok(())
    }

    #[test]
    fn test_filter_null() -> Result<()> {
        let mut pre = FilterEmpty::default();
        assert_eq!(Ok(vec![]), pre.process(&mut 0u64, &vec![]));
        Ok(())
    }
}
