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
pub(crate) use gelf::GELF;

use crate::errors::*;
use base64;
use byteorder::{BigEndian, WriteBytesExt};
use std::default::Default;
pub type Postprocessors = Vec<Box<dyn Postprocessor>>;

pub trait Postprocessor: Send {
    fn process(&mut self, ingres_ns: u64, egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>>;
}

// just a lookup
#[cfg_attr(tarpaulin, skip)]
pub fn lookup(name: &str) -> Result<Box<dyn Postprocessor>> {
    match name {
        "lines" => Ok(Box::new(Lines {})),
        "base64" => Ok(Box::new(Base64 {})),
        "gzip" => Ok(Box::new(CompressGzip {})),
        "zlib" => Ok(Box::new(CompressZlib {})),
        "xz2" => Ok(Box::new(CompressXz2 {})),
        "snappy" => Ok(Box::new(CompressSnappy {})),
        "lz4" => Ok(Box::new(CompressLz4 {})),
        "ingest-ns" => Ok(Box::new(AttachIngresTS {})),
        "length-prefixerd" => Ok(Box::new(LengthPrefix::default())),
        "gelf-chunking" => Ok(Box::new(GELF::default())),
        _ => Err(format!("Postprocessor '{}' not found.", name).into()),
    }
}

pub(crate) struct Lines {}
impl Postprocessor for Lines {
    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let mut framed = data.to_vec(); // FIXME PERF TODO prefer to in-place extend
        framed.push(b'\n');
        Ok(vec![framed])
    }
}

pub(crate) struct Base64 {}
impl Postprocessor for Base64 {
    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(vec![base64::encode(&data).as_bytes().to_vec()])
    }
}

pub(crate) struct CompressGzip {}
impl Postprocessor for CompressGzip {
    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use libflate::gzip::Encoder;
        use std::io::Write;
        let mut encoder = Encoder::new(Vec::new())?;
        encoder.write_all(&data)?;
        Ok(vec![encoder.finish().into_result()?])
    }
}

pub(crate) struct CompressZlib {}
impl Postprocessor for CompressZlib {
    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use libflate::zlib::Encoder;
        use std::io::Write;
        let mut encoder = Encoder::new(Vec::new())?;
        encoder.write_all(&data)?;
        Ok(vec![encoder.finish().into_result()?])
    }
}

pub(crate) struct CompressXz2 {}
impl Postprocessor for CompressXz2 {
    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use std::io::Write;
        use xz2::write::XzEncoder as Encoder;
        let mut encoder = Encoder::new(Vec::new(), 9);
        encoder.write_all(&data)?;
        Ok(vec![encoder.finish()?])
    }
}

pub(crate) struct CompressSnappy {}
impl Postprocessor for CompressSnappy {
    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use snap::Encoder;
        let len: usize = data.len();
        let max_compress_len: usize = snap::max_compress_len(len);
        let mut compressed = Vec::with_capacity(max_compress_len);
        let mut encoder = Encoder::new();
        encoder.compress(data, compressed.as_mut())?;
        Ok(vec![compressed])
    }
}

pub(crate) struct CompressLz4 {}
impl Postprocessor for CompressLz4 {
    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use lz4::EncoderBuilder;
        use std::io::Write;
        let buffer = Vec::<u8>::new();
        let mut encoder = EncoderBuilder::new().level(4).build(buffer)?;
        encoder.write_all(&data)?;
        Ok(vec![encoder.finish().0])
    }
}

pub(crate) struct AttachIngresTS {}
impl Postprocessor for AttachIngresTS {
    fn process(&mut self, ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use std::io::Write;
        let mut res = Vec::with_capacity(data.len() + 8);
        res.write_u64::<BigEndian>(ingres_ns)?;
        res.write_all(&data)?;

        Ok(vec![res])
    }
}

#[derive(Clone, Default)]
pub(crate) struct LengthPrefix {}
impl Postprocessor for LengthPrefix {
    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use std::io::Write;
        let mut res = Vec::with_capacity(data.len() + 8);
        res.write_u64::<BigEndian>(data.len() as u64)?;
        res.write_all(&data)?;
        Ok(vec![res])
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn line() {
        let mut line = Lines {};
        let data: [u8; 0] = [];
        assert_eq!(Ok(vec![vec![b'\n']]), line.process(0, 0, &data));
        assert_eq!(
            Ok(vec![vec![b'f', b'o', b'o', b'b', b'\n']]),
            line.process(0, 0, b"foob")
        );
    }

    #[test]
    fn base64() {
        let mut post = Base64 {};
        let data: [u8; 0] = [];

        assert_eq!(Ok(vec![vec![]]), post.process(0, 0, &data));

        // FIXME throws invalid length but it should not
        // assert_eq!(Ok(vec![vec![b'C',b'g',b'=',b'=']]), post.process(0, "\n".as_bytes()));

        assert_eq!(Ok(vec![b"c25vdA==".to_vec()]), post.process(0, 0, b"snot"));
    }
}
