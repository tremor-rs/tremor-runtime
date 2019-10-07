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

mod gelf;
use crate::errors::*;
use base64;

use byteorder::{BigEndian, ReadBytesExt};

pub type Preprocessors = Vec<Box<dyn Preprocessor>>;
pub trait Preprocessor: Sync + Send {
    fn process(&mut self, ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>>;
}

#[deny(clippy::ptr_arg)]
pub fn lookup(name: &str) -> Result<Box<dyn Preprocessor>> {
    match name {
        "lines" => Ok(Box::new(Lines {})),
        "base64" => Ok(Box::new(Base64 {})),
        "gzip" => Ok(Box::new(Gzip {})),
        "decompress" => Ok(Box::new(Decompress {})),
        "remove-empty" => Ok(Box::new(FilterEmpty::default())),
        "gelf-chunking" => Ok(Box::new(gelf::GELF::default())),
        "gelf-chunking-tcp" => Ok(Box::new(gelf::GELF::tcp())),
        "ingest-ns" => Ok(Box::new(ExtractIngresTs {})),
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
struct FilterEmpty {}

impl Preprocessor for FilterEmpty {
    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        if data.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![data.to_vec()])
        }
    }
}

#[derive(Clone)]
struct Nulls {}
impl Preprocessor for Nulls {
    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(data.split(|c| *c == b'\0').map(Vec::from).collect())
    }
}

#[derive(Clone)]
struct Lines {}
impl Preprocessor for Lines {
    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(data.split(|c| *c == b'\n').map(Vec::from).collect())
    }
}

#[derive(Clone)]
struct ExtractIngresTs {}
impl Preprocessor for ExtractIngresTs {
    fn process(&mut self, ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use std::io::Cursor;
        *ingest_ns = Cursor::new(data).read_u64::<BigEndian>()?;
        Ok(vec![data[8..].to_vec()])
    }
}

#[derive(Clone)]
struct Base64 {}
impl Preprocessor for Base64 {
    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(vec![base64::decode(&data)?])
    }
}

#[derive(Clone)]
struct Gzip {}
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

#[derive(Clone)]
struct Decompress {}
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
            Some(b"sNaPpY") => {
                use snap::Decoder;
                let mut decoder = Decoder::new();
                decoder.decompress_vec(data)?
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
