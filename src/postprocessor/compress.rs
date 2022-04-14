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

use super::Postprocessor;
use crate::errors::Result;
use simd_json::ValueAccess;
use std::{io::Write, str};
use tremor_script::Value;

#[derive(Default)]
struct Gzip {}
impl Postprocessor for Gzip {
    fn name(&self) -> &str {
        "gzip"
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use libflate::gzip::Encoder;

        let mut encoder = Encoder::new(Vec::new())?;
        encoder.write_all(data)?;
        Ok(vec![encoder.finish().into_result()?])
    }
}

#[derive(Default)]
struct Zlib {}
impl Postprocessor for Zlib {
    fn name(&self) -> &str {
        "zlib"
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use libflate::zlib::Encoder;
        let mut encoder = Encoder::new(Vec::new())?;
        encoder.write_all(data)?;
        Ok(vec![encoder.finish().into_result()?])
    }
}

#[derive(Default)]
struct Xz2 {}
impl Postprocessor for Xz2 {
    fn name(&self) -> &str {
        "xz2"
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use xz2::write::XzEncoder as Encoder;
        let mut encoder = Encoder::new(Vec::new(), 9);
        encoder.write_all(data)?;
        Ok(vec![encoder.finish()?])
    }
}

#[derive(Default)]
struct Snappy {}
impl Postprocessor for Snappy {
    fn name(&self) -> &str {
        "snappy"
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use snap::write::FrameEncoder;
        let mut writer = FrameEncoder::new(vec![]);
        writer.write_all(data)?;
        let compressed = writer
            .into_inner()
            .map_err(|e| format!("Snappy compression postprocessor error: {}", e))?;
        Ok(vec![compressed])
    }
}

#[derive(Default)]
struct Lz4 {}
impl Postprocessor for Lz4 {
    fn name(&self) -> &str {
        "lz4"
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use lz4::EncoderBuilder;
        let buffer = Vec::<u8>::new();
        let mut encoder = EncoderBuilder::new().level(4).build(buffer)?;
        encoder.write_all(data)?;
        Ok(vec![encoder.finish().0])
    }
}

#[derive(Clone, Default, Debug)]
struct Zstd {}
impl Postprocessor for Zstd {
    fn name(&self) -> &str {
        "zstd"
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        // Value of 0 indicates default level for encode.
        let compressed = zstd::encode_all(data, 0)?;
        Ok(vec![compressed])
    }
}

pub(crate) struct Compress {
    codec: Box<dyn Postprocessor>,
}
impl Compress {
    pub(crate) fn from_config(config: Option<&Value>) -> Result<Self> {
        let codec: Box<dyn Postprocessor> =
            match config.get_str("algorithm").ok_or("Missing algorithm")? {
                "gzip" => Box::new(Gzip::default()),
                "zlib" => Box::new(Zlib::default()),
                "xz2" => Box::new(Xz2::default()),
                "snappy" => Box::new(Snappy::default()),
                "lz4" => Box::new(Lz4::default()),
                "zstd" => Box::new(Zstd::default()),
                other => return Err(format!("Unknown compression algorithm: {other}").into()),
            };
        Ok(Compress { codec })
    }
}
impl Postprocessor for Compress {
    fn name(&self) -> &str {
        "compress"
    }
    fn process(&mut self, ingres_ns: u64, egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        self.codec.process(ingres_ns, egress_ns, data)
    }
}
