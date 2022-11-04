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
use std::{
    io::Write,
    str::{self, FromStr},
};
use tremor_script::Value;

#[derive(Debug, PartialEq)]
enum Algorithm {
    Gzip,
    Zlib,
    Xz2,
    Zstd,
    Snappy,
    Lz4,
}

impl FromStr for Algorithm {
    type Err = crate::errors::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "gzip" => Ok(Algorithm::Gzip),
            "zlib" => Ok(Algorithm::Zlib),
            "xz2" => Ok(Algorithm::Xz2),
            "snappy" => Ok(Algorithm::Snappy),
            "lz4" => Ok(Algorithm::Lz4),
            "zstd" => Ok(Algorithm::Zstd),
            other => Err(format!("Unknown compression algorithm: {other}").into()),
        }
    }
}

impl Algorithm {
    pub fn into_postprocessor(self, config: Option<&Value>) -> Result<Box<dyn Postprocessor>> {
        if let Some(compression_level) = config.get_i64("level") {
            match self {
                Algorithm::Xz2 => Xz2::with_config(compression_level),
                Algorithm::Zstd => Zstd::with_config(compression_level),
                Algorithm::Lz4 => Lz4::with_config(compression_level),
                _ => Err("compression level not supported for given algorithm".into()),
            }
        } else {
            let codec: Box<dyn Postprocessor> = match self {
                Algorithm::Gzip => Box::new(Gzip::default()),
                Algorithm::Zlib => Box::new(Zlib::default()),
                Algorithm::Xz2 => Box::new(Xz2::default()),
                Algorithm::Zstd => Box::new(Zstd::default()),
                Algorithm::Snappy => Box::new(Snappy::default()),
                Algorithm::Lz4 => Box::new(Lz4::default()),
            };
            Ok(codec)
        }
    }
}

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

struct Xz2 {
    compression_level: u32,
}
impl Xz2 {
    fn with_config(level: i64) -> Result<Box<dyn Postprocessor>> {
        if !(0..=9).contains(&level) {
            return Err(format!(
                "Xz2 supports compression level between 0 and 9 but {level} was given"
            )
            .into());
        }

        Ok(Box::new(Self {
            compression_level: level.try_into()?,
        }))
    }
}
impl Postprocessor for Xz2 {
    fn name(&self) -> &str {
        "xz2"
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use xz2::write::XzEncoder as Encoder;
        let mut encoder = Encoder::new(Vec::new(), self.compression_level);
        encoder.write_all(data)?;
        Ok(vec![encoder.finish()?])
    }
}
impl Default for Xz2 {
    fn default() -> Self {
        Self {
            compression_level: 9,
        }
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

struct Lz4 {
    compression_level: u32,
}
impl Lz4 {
    pub fn with_config(level: i64) -> Result<Box<dyn Postprocessor>> {
        if level < 0 {
            return Err("Lz4 compression level cannot be less than 0".into());
        }

        Ok(Box::new(Self {
            compression_level: level.try_into()?,
        }))
    }
}
impl Postprocessor for Lz4 {
    fn name(&self) -> &str {
        "lz4"
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use lz4::EncoderBuilder;
        let buffer = Vec::<u8>::new();
        let mut encoder = EncoderBuilder::new()
            .level(self.compression_level)
            .build(buffer)?;
        encoder.write_all(data)?;
        Ok(vec![encoder.finish().0])
    }
}
impl Default for Lz4 {
    fn default() -> Self {
        Self {
            compression_level: 4,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct Zstd {
    compression_level: i32,
}
impl Zstd {
    pub fn with_config(level: i64) -> Result<Box<dyn Postprocessor>> {
        if !(-7..=22).contains(&level) {
            return Err(format!(
                "Zstd supports compression level between -7 and 22 but {level} was given"
            )
            .into());
        }

        Ok(Box::new(Self {
            compression_level: level.try_into()?,
        }))
    }
}
impl Postprocessor for Zstd {
    fn name(&self) -> &str {
        "zstd"
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        // Value of 0 indicates default level for encode.
        let compressed = zstd::encode_all(data, self.compression_level)?;
        Ok(vec![compressed])
    }
}
pub(crate) struct Compress {
    codec: Box<dyn Postprocessor>,
}
impl Compress {
    pub(crate) fn from_config(config: Option<&Value>) -> Result<Self> {
        let algorithm_str = config.get_str("algorithm").ok_or("Missing algorithm")?;
        match Algorithm::from_str(algorithm_str) {
            Ok(algorithm) => match algorithm.into_postprocessor(config) {
                Ok(codec) => Ok(Compress { codec }),
                Err(msg) => Err(msg),
            },
            Err(msg) => Err(msg),
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_str_to_algorithm() -> Result<()> {
        let algorithm = Algorithm::from_str("gzip")?;
        assert_eq!(algorithm, Algorithm::Gzip);
        let algorithm = Algorithm::from_str("xz2")?;
        assert_eq!(algorithm, Algorithm::Xz2);
        let algorithm = Algorithm::from_str("snappy")?;
        assert_eq!(algorithm, Algorithm::Snappy);
        let algorithm = Algorithm::from_str("zlib")?;
        assert_eq!(algorithm, Algorithm::Zlib);
        let algorithm = Algorithm::from_str("zstd")?;
        assert_eq!(algorithm, Algorithm::Zstd);

        let _algorithm = Algorithm::from_str("gzi");
        assert!(matches!(crate::errors::Error, _algorithm));
        let _algorithm = Algorithm::from_str("xz");
        assert!(matches!(crate::errors::Error, _algorithm));
        let _algorithm = Algorithm::from_str("snapp");
        assert!(matches!(crate::errors::Error, _algorithm));
        let _algorithm = Algorithm::from_str("zli");
        assert!(matches!(crate::errors::Error, _algorithm));
        let _algorithm = Algorithm::from_str("zst");
        assert!(matches!(crate::errors::Error, _algorithm));

        Ok(())
    }

    #[test]
    fn test_test_xz2() -> Result<()> {
        let _algorithm = Xz2::with_config(-1);
        assert!(matches!(crate::errors::Error, _algorithm));
        let _algorithm = Xz2::with_config(10);
        assert!(matches!(crate::errors::Error, _algorithm));

        for level in 0..=9 {
            let _algorithm = Xz2::with_config(level)?;
        }

        Ok(())
    }

    #[test]
    fn test_test_lz4() {
        let _algorithm = Lz4::with_config(-1);
        assert!(matches!(crate::errors::Error, _algorithm));
    }

    #[test]
    fn test_test_zstd() -> Result<()> {
        let _algorithm = Zstd::with_config(-8);
        assert!(matches!(crate::errors::Error, _algorithm));

        let _algorithm = Zstd::with_config(23);
        assert!(matches!(crate::errors::Error, _algorithm));

        for level in -7..=22 {
            let _algorithm = Zstd::with_config(level)?;
        }

        Ok(())
    }
}
