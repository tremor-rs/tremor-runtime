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
mod lines;

use crate::codec::{self, Codec};
use crate::errors::*;
use base64;
use std::any::Any;

pub type Preprocessors = Vec<Box<dyn Preprocessor>>;
pub trait Preprocessor: Sync + Send {
    fn as_any(&self) -> &dyn Any;
    fn process(&mut self, ingest_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>>;
}

fn downcast<T: Preprocessor + 'static>(this: &dyn Preprocessor) -> Option<&T> {
    this.as_any().downcast_ref()
}

// NOTE required due to constraints imposed by actix to pass preproc's to actor state in
// so that they can be applied in rest request handlers
impl Clone for Box<dyn Preprocessor> {
    fn clone(&self) -> Self {
        if let Some(x) = downcast::<lines::Lines>(&**self) {
            return Box::new(x.clone());
        };
        if let Some(x) = downcast::<Influx>(&**self) {
            return Box::new(x.clone());
        };
        if let Some(x) = downcast::<Base64>(&**self) {
            return Box::new(x.clone());
        };
        if let Some(x) = downcast::<Decompress>(&**self) {
            return Box::new(x.clone());
        };
        if let Some(x) = downcast::<gelf::GELF>(&**self) {
            return Box::new(x.clone());
        };
        if let Some(x) = downcast::<Gzip>(&**self) {
            return Box::new(x.clone());
        };
        unreachable!("Unable to clone unsupported preprocessor type");
    }
}

#[deny(clippy::ptr_arg)]
pub fn lookup(name: &str) -> Result<Box<dyn Preprocessor>> {
    match name {
        // TODO once preprocessors allow configuration, remove multiple entries for lines here
        "lines" => Ok(Box::new(lines::Lines::new('\n'))),
        "lines-null" => Ok(Box::new(lines::Lines::new('\0'))),
        // "influx" => Ok(Box::new(Influx::default())),
        "base64" => Ok(Box::new(Base64 {})),
        "gzip" => Ok(Box::new(Gzip {})),
        "decompress" => Ok(Box::new(Decompress {})),
        "remove-empty" => Ok(Box::new(FilterEmpty::default())),
        "gelf-chunking" => Ok(Box::new(gelf::GELF::default())),
        "gelf-chunking-tcp" => Ok(Box::new(gelf::GELF::tcp())),
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
                unreachable!();
            }
        } else {
            &[]
        }
    }
}

#[derive(Default, Debug, Clone)]
struct FilterEmpty {}

impl Preprocessor for FilterEmpty {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn process(&mut self, _ingest_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        if data.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![data.to_vec()])
        }
    }
}

#[derive(Clone)]
struct Influx {
    codec: Box<codec::influx::Influx>,
    json: Box<codec::json::JSON>,
}

impl Influx {
    #[allow(dead_code)]
    fn default() -> Self {
        Influx {
            codec: Box::new(codec::influx::Influx {}),
            json: Box::new(codec::json::JSON {}),
        }
    }
}

impl Preprocessor for Influx {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn process(&mut self, ingest_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let data = data.trim();
        if data.is_empty() {
            Ok(vec![])
        } else {
            match self.codec.decode(data.to_vec(), ingest_ns) {
                Ok(Some(x)) => Ok(vec![self.json.encode(x).expect("could not encode")]),
                Ok(None) => Ok(vec![]),
                Err(e) => {
                    dbg!(("influx", &e));
                    Err(e)
                }
            }
        }
    }
}

#[derive(Clone)]
struct Base64 {}
impl Preprocessor for Base64 {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn process(&mut self, _ingest_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(vec![base64::decode(&data)?])
    }
}

#[derive(Clone)]
struct Gzip {}
impl Preprocessor for Gzip {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn process(&mut self, _ingest_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use libflate::gzip::MultiDecoder;
        use std::io::Read;
        let mut decoder =
            MultiDecoder::new(&data[..]).expect("could not create multi decoder for gzip");
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(vec![decompressed])
    }
}

#[derive(Clone)]
struct Decompress {}
impl Preprocessor for Decompress {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn process(&mut self, _ingest_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
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
