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

//! Compresses event data.
//!
//! Unlike decompression processors, the compression algorithm must be selected. Optionally the compression level can be specified for algotithms which support it (example Zstandard with level 0). The following compression post-processors are supported. Each format can be configured as a postprocessor.
//!
//! Supported formats:
//!
//! | Name     | Algorithm / Format                                                                 |
//! |----------|------------------------------------------------------------------------------------|
//! | `gzip`   | `GZip`                                                                             |
//! | `zlib`   | `ZLib`                                                                             |
//! | `xz2`    | `Xz2` level 9 (default)                                                            |
//! | `snappy` | `Snappy`                                                                           |
//! | `lz4`    | `Lz` level 4 compression (default)                                                 |
//! | `zstd`   | [`Zstandard`](https://datatracker.ietf.org/doc/html/rfc8878) (defaults to level 0) |
//! | `br`     | `Brotli`                                                                           |
//!
//! Example configuration:
//!
//! Xz compression example with compression level:
//!
//! ```tremor
//! postprocessors = [
//!   {
//!     "name": "compress",
//!     "config": {
//!       "algotithm":"xz2",
//!       "level": 3
//!     }
//!   }
//! ]
//! ```
//!
//! Xz compression example without compression level defaults to level 9:
//!
//! ```tremor
//! postprocessors = [
//!   {
//!     "name": "compress",
//!     "config": {
//!       "algorithm":"xz2"
//!     }
//!   }
//! ]
//! ```
//!
//! Xz compression when wrong compression level is specified gives an `Err`.

use super::Stateless;
use std::{
    io::{Cursor, Write},
    str::{self, FromStr},
};
use tremor_value::Value;
use value_trait::prelude::*;

#[derive(Debug, PartialEq)]
enum Algorithm {
    Gzip,
    Zlib,
    Xz2,
    Zstd,
    Snappy,
    Lz4,
    Brotli,
}

/// Compression postprocessor errors
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Unknown compression algorithm
    #[error("Unknown compression algorithm: {0}")]
    UnknownAlgorithm(String),
    /// Compression level not supported for given algorithm
    #[error("Compression level not supported for given algorithm")]
    CompressionLevelNotSupported,
    /// Missing algorithm
    #[error("Missing algorithm")]
    MissingAlgorithm,
    /// Xz2 Error
    #[error(transparent)]
    Xz2(#[from] Xz2Error),
    /// Lz4 Error
    #[error(transparent)]
    Lz4(#[from] Lz4Error),
    /// Zstd Error
    #[error(transparent)]
    Zstd(#[from] ZstdError),
    /// IO Error
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl FromStr for Algorithm {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        match s {
            "gzip" => Ok(Algorithm::Gzip),
            "zlib" => Ok(Algorithm::Zlib),
            "xz2" => Ok(Algorithm::Xz2),
            "snappy" => Ok(Algorithm::Snappy),
            "lz4" => Ok(Algorithm::Lz4),
            "zstd" => Ok(Algorithm::Zstd),
            "br" => Ok(Algorithm::Brotli),
            other => Err(Error::UnknownAlgorithm(other.to_string())),
        }
    }
}

impl Algorithm {
    pub fn into_postprocessor(self, config: Option<&Value>) -> Result<Box<dyn Stateless>, Error> {
        if let Some(compression_level) = config.get_i64("level") {
            match self {
                Algorithm::Xz2 => Ok(Xz2::with_config(compression_level)?),
                Algorithm::Zstd => Ok(Zstd::with_config(compression_level)?),
                Algorithm::Lz4 => Ok(Lz4::with_config(compression_level)?),
                _ => Err(Error::CompressionLevelNotSupported),
            }
        } else {
            let codec: Box<dyn Stateless> = match self {
                Algorithm::Gzip => Box::<Gzip>::default(),
                Algorithm::Zlib => Box::<Zlib>::default(),
                Algorithm::Xz2 => Box::<Xz2>::default(),
                Algorithm::Zstd => Box::<Zstd>::default(),
                Algorithm::Snappy => Box::<Snappy>::default(),
                Algorithm::Lz4 => Box::<Lz4>::default(),
                Algorithm::Brotli => Box::<Brotli>::default(),
            };
            Ok(codec)
        }
    }
}

#[derive(Default)]
struct Gzip {}
impl Stateless for Gzip {
    fn name(&self) -> &str {
        "gzip"
    }

    fn process(&self, data: &[u8]) -> anyhow::Result<Vec<Vec<u8>>> {
        use libflate::gzip::Encoder;

        let mut encoder = Encoder::new(Vec::new())?;
        encoder.write_all(data)?;
        Ok(vec![encoder.finish().into_result()?])
    }
}

#[derive(Default)]
struct Brotli {
    params: brotli::enc::BrotliEncoderParams,
}
impl Stateless for Brotli {
    fn name(&self) -> &str {
        "br"
    }

    fn process(&self, data: &[u8]) -> anyhow::Result<Vec<Vec<u8>>> {
        // Heuristic because it's nice to avoid some allocations
        let mut res = Vec::with_capacity(data.len() / 10);
        let mut c = Cursor::new(data);
        brotli::BrotliCompress(&mut c, &mut res, &self.params)?;
        Ok(vec![res])
    }
}

#[derive(Default)]
struct Zlib {}
impl Stateless for Zlib {
    fn name(&self) -> &str {
        "zlib"
    }

    fn process(&self, data: &[u8]) -> anyhow::Result<Vec<Vec<u8>>> {
        use libflate::zlib::Encoder;
        let mut encoder = Encoder::new(Vec::new())?;
        encoder.write_all(data)?;
        Ok(vec![encoder.finish().into_result()?])
    }
}

struct Xz2 {
    compression_level: u32,
}

/// Xz2 Error
#[derive(Debug, thiserror::Error)]
pub enum Xz2Error {
    /// Invalid compressioh level
    #[error("Xz2 supports compression level between 0 and 9 but {0} was given")]
    InvalidCompressionLevel(i64),
}
impl Xz2 {
    fn with_config(level: i64) -> Result<Box<dyn Stateless>, Xz2Error> {
        if !(0..=9).contains(&level) {
            return Err(Xz2Error::InvalidCompressionLevel(level));
        }

        Ok(Box::new(Self {
            compression_level: level
                .try_into()
                .map_err(|_| Xz2Error::InvalidCompressionLevel(level))?,
        }))
    }
}
impl Stateless for Xz2 {
    fn name(&self) -> &str {
        "xz2"
    }

    fn process(&self, data: &[u8]) -> anyhow::Result<Vec<Vec<u8>>> {
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
impl Stateless for Snappy {
    fn name(&self) -> &str {
        "snappy"
    }

    fn process(&self, data: &[u8]) -> anyhow::Result<Vec<Vec<u8>>> {
        use snap::write::FrameEncoder;
        let mut writer = FrameEncoder::new(vec![]);
        writer.write_all(data)?;
        let compressed = writer.into_inner()?;
        Ok(vec![compressed])
    }
}

struct Lz4 {
    compression_level: u32,
}
/// lz4 Error
#[derive(Debug, thiserror::Error)]
pub enum Lz4Error {
    /// Invalid compressioh level
    #[error("Lz4 compression level cannot be less than 0 but {0} was given")]
    InvalidCompressionLevel(i64),
}
impl Lz4 {
    pub fn with_config(level: i64) -> Result<Box<dyn Stateless>, Lz4Error> {
        if level < 0 {
            return Err(Lz4Error::InvalidCompressionLevel(level));
        }

        Ok(Box::new(Self {
            compression_level: level
                .try_into()
                .map_err(|_| Lz4Error::InvalidCompressionLevel(level))?,
        }))
    }
}
impl Stateless for Lz4 {
    fn name(&self) -> &str {
        "lz4"
    }

    fn process(&self, data: &[u8]) -> anyhow::Result<Vec<Vec<u8>>> {
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
/// zstd Error
#[derive(Debug, thiserror::Error)]
pub enum ZstdError {
    /// Invalid compressioh level
    #[error("Zstd supports compression level between -7 and 22 but {0} was given")]
    InvalidCompressionLevel(i64),
}

impl Zstd {
    pub fn with_config(level: i64) -> Result<Box<dyn Stateless>, ZstdError> {
        if !(-7..=22).contains(&level) {
            return Err(ZstdError::InvalidCompressionLevel(level));
        }

        Ok(Box::new(Self {
            compression_level: level
                .try_into()
                .map_err(|_| ZstdError::InvalidCompressionLevel(level))?,
        }))
    }
}
impl Stateless for Zstd {
    fn name(&self) -> &str {
        "zstd"
    }

    fn process(&self, data: &[u8]) -> anyhow::Result<Vec<Vec<u8>>> {
        // Value of 0 indicates default level for encode.
        let compressed = zstd::encode_all(data, self.compression_level)?;
        Ok(vec![compressed])
    }
}
pub(crate) struct Compress {
    codec: Box<dyn Stateless>,
}
impl Compress {
    pub(crate) fn from_config(config: Option<&Value>) -> Result<Self, super::Error> {
        let algorithm_str = config
            .get_str("algorithm")
            .ok_or(super::Error::InvalidConfig(
                "compress",
                Error::MissingAlgorithm.into(),
            ))?;
        match Algorithm::from_str(algorithm_str) {
            Ok(algorithm) => match algorithm.into_postprocessor(config) {
                Ok(codec) => Ok(Compress { codec }),
                Err(msg) => Err(super::Error::InvalidConfig("compress", msg.into())),
            },
            Err(msg) => Err(super::Error::InvalidConfig("compress", msg.into())),
        }
    }
}
impl Stateless for Compress {
    fn name(&self) -> &str {
        "compress"
    }
    fn process(&self, data: &[u8]) -> anyhow::Result<Vec<Vec<u8>>> {
        self.codec.process(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_str_to_algorithm() -> anyhow::Result<()> {
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

        let algorithm = Algorithm::from_str("gzi");
        assert!(algorithm.is_err());
        let algorithm = Algorithm::from_str("xz");
        assert!(algorithm.is_err());
        let algorithm = Algorithm::from_str("snapp");
        assert!(algorithm.is_err());
        let algorithm = Algorithm::from_str("zli");
        assert!(algorithm.is_err());
        let algorithm = Algorithm::from_str("zst");
        assert!(algorithm.is_err());

        Ok(())
    }

    #[test]
    fn test_test_xz2() -> anyhow::Result<()> {
        let algorithm = Xz2::with_config(-1);
        assert!(algorithm.is_err());
        let algorithm = Xz2::with_config(10);
        assert!(algorithm.is_err());

        for level in 0..=9 {
            let _algorithm = Xz2::with_config(level)?;
        }

        Ok(())
    }

    #[test]
    fn test_test_lz4() {
        let algorithm = Lz4::with_config(-1);
        assert!(algorithm.is_err());
    }

    #[test]
    fn test_test_zstd() -> anyhow::Result<()> {
        let algorithm = Zstd::with_config(-8);
        assert!(algorithm.is_err());

        let algorithm = Zstd::with_config(23);
        assert!(algorithm.is_err());

        for level in -7..=22 {
            let _algorithm = Zstd::with_config(level)?;
        }

        Ok(())
    }
}
