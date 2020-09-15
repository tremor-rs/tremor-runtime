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

use crate::errors::{Error, Result};
use byteorder::{BigEndian, WriteBytesExt};
use std::default::Default;
/// Set of Postprocessors
pub type Postprocessors = Vec<Box<dyn Postprocessor>>;
use std::io::Write;

/// Postprocessor trait
pub trait Postprocessor: Send {
    /// Canonical name of the postprocessor
    fn name(&self) -> String;
    /// process data
    ///
    /// # Errors
    ///
    ///   * Errors if the data could not be processed
    fn process(&mut self, ingres_ns: u64, egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>>;
}

/// Lookup a postprocessor via its unique id
/// # Errors
///
///   * Errors if the postprocessor is not known
#[cfg_attr(tarpaulin, skip)]
pub fn lookup(name: &str) -> Result<Box<dyn Postprocessor>> {
    match name {
        "lines" => Ok(Box::new(Lines::default())),
        "base64" => Ok(Box::new(Base64::default())),
        "gzip" => Ok(Box::new(Gzip::default())),
        "zlib" => Ok(Box::new(Zlib::default())),
        "xz2" => Ok(Box::new(Xz2::default())),
        "snappy" => Ok(Box::new(Snappy::default())),
        "lz4" => Ok(Box::new(Lz4::default())),
        "ingest-ns" => Ok(Box::new(AttachIngresTS {})),
        "length-prefixed" => Ok(Box::new(LengthPrefix::default())),
        "gelf-chunking" => Ok(Box::new(GELF::default())),
        _ => Err(format!("Postprocessor '{}' not found.", name).into()),
    }
}

#[derive(Default)]
pub(crate) struct Lines {}
impl Postprocessor for Lines {
    fn name(&self) -> String {
        "lines".to_string()
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        // padding capacity with 1 to account for the new line char we will be pushing
        let mut framed: Vec<u8> = Vec::with_capacity(data.len() + 1);
        framed.extend_from_slice(data);
        framed.push(b'\n');
        Ok(vec![framed])
    }
}

#[derive(Default)]
pub(crate) struct Base64 {}
impl Postprocessor for Base64 {
    fn name(&self) -> String {
        "base64".to_string()
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(vec![base64::encode(&data).as_bytes().to_vec()])
    }
}

#[derive(Default)]
pub(crate) struct Gzip {}
impl Postprocessor for Gzip {
    fn name(&self) -> String {
        "gzip".to_string()
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use libflate::gzip::Encoder;

        let mut encoder = Encoder::new(Vec::new())?;
        encoder.write_all(&data)?;
        Ok(vec![encoder.finish().into_result()?])
    }
}

#[derive(Default)]
pub(crate) struct Zlib {}
impl Postprocessor for Zlib {
    fn name(&self) -> String {
        "zlib".to_string()
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use libflate::zlib::Encoder;
        let mut encoder = Encoder::new(Vec::new())?;
        encoder.write_all(&data)?;
        Ok(vec![encoder.finish().into_result()?])
    }
}

#[derive(Default)]
pub(crate) struct Xz2 {}
impl Postprocessor for Xz2 {
    fn name(&self) -> String {
        "xz2".to_string()
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use xz2::write::XzEncoder as Encoder;
        let mut encoder = Encoder::new(Vec::new(), 9);
        encoder.write_all(&data)?;
        Ok(vec![encoder.finish()?])
    }
}

#[derive(Default)]
pub(crate) struct Snappy {}
impl Postprocessor for Snappy {
    fn name(&self) -> String {
        "snappy".to_string()
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use snap::write::FrameEncoder;
        let mut writer = FrameEncoder::new(vec![]);
        writer.write_all(data)?;
        let compressed = writer
            .into_inner()
            .map_err(|e| Error::from(format!("Snappy compression postprocessor error: {}", e)))?;
        Ok(vec![compressed])
    }
}

#[derive(Default)]
pub(crate) struct Lz4 {}
impl Postprocessor for Lz4 {
    fn name(&self) -> String {
        "lz4".to_string()
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use lz4::EncoderBuilder;
        let buffer = Vec::<u8>::new();
        let mut encoder = EncoderBuilder::new().level(4).build(buffer)?;
        encoder.write_all(&data)?;
        Ok(vec![encoder.finish().0])
    }
}

pub(crate) struct AttachIngresTS {}
impl Postprocessor for AttachIngresTS {
    fn name(&self) -> String {
        "attach-ingress-ts".to_string()
    }

    fn process(&mut self, ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let mut res = Vec::with_capacity(data.len() + 8);
        res.write_u64::<BigEndian>(ingres_ns)?;
        res.write_all(&data)?;

        Ok(vec![res])
    }
}

#[derive(Clone, Default)]
pub(crate) struct LengthPrefix {}
impl Postprocessor for LengthPrefix {
    fn name(&self) -> String {
        "length-prefix".to_string()
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
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

        assert_eq!(Ok(vec![b"Cg==".to_vec()]), post.process(0, 0, b"\n"));

        assert_eq!(Ok(vec![b"c25vdA==".to_vec()]), post.process(0, 0, b"snot"));
    }
}
