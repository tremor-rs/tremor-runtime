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

//! Stream Compresses event data stream.
//!
//! Unlike decompression processors, the compression algorithm must be selected. Optionally the compression level can be specified for algotithms which support it (example Zstandard with level 0). The following compression post-processors are supported. Each format can be configured as a postprocessor.
//!
//! In contrast to the Compression preprocessor, which compresses the entire event data as a single blob the stream-compress postprocessor compresses the event data stream of events until the stream is marked as finished.
//! Supported formats:
//!
//! | Name     | Algorithm / Format                                                                 |
//! |----------|------------------------------------------------------------------------------------|
//! | `xz2`    | `Xz2` level 9 (default)                                                            |
//! | `zstd`   | [`Zstandard`](https://datatracker.ietf.org/doc/html/rfc8878) (defaults to level 0) |
//!
//! Example configuration:
//!
//! Xz compression example with compression level:
//!
//! ```tremor
//! postprocessors = [
//!   {
//!     "name": "stream-compress",
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
//!     "name": "stream-compress",
//!     "config": {
//!       "algorithm":"xz2"
//!     }
//!   }
//! ]
//! ```
//!
//! Xz compression when wrong compression level is specified gives an `Err`.

use super::Postprocessor;
use ringbuf::traits::Observer;
use ringbuf_blocking::{traits::Split, wrap::BlockingWrap, BlockingHeapRb};
use std::{
    io::{Read as _, Write as _},
    str::FromStr,
    sync::Arc,
    vec,
};
use tremor_value::Value;
use value_trait::prelude::*;

#[derive(Debug, PartialEq)]
enum Algorithm {
    // Gzip,
    // Zlib,
    Xz2,
    Zstd,
    // Snappy,
    // Lz4,
    // Brotli,
}

/// Compression postprocessor errors
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Unknown compression algorithm
    #[error("Unknown compression algorithm: {0}")]
    UnknownAlgorithm(String),
    // /// Compression level not supported for given algorithm
    // #[error("Compression level not supported for given algorithm")]
    // CompressionLevelNotSupported,
    /// Missing algorithm
    #[error("Missing algorithm")]
    MissingAlgorithm,
    /// Xz2 Error
    #[error(transparent)]
    Xz2(#[from] Xz2Error),
    // /// Lz4 Error
    // #[error(transparent)]
    // Lz4(#[from] Lz4Error),
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
            // "gzip" => Ok(Algorithm::Gzip),
            // "zlib" => Ok(Algorithm::Zlib),
            "xz2" => Ok(Algorithm::Xz2),
            // "snappy" => Ok(Algorithm::Snappy),
            // "lz4" => Ok(Algorithm::Lz4),
            "zstd" => Ok(Algorithm::Zstd),
            // "br" => Ok(Algorithm::Brotli),
            other => Err(Error::UnknownAlgorithm(other.to_string())),
        }
    }
}

impl Algorithm {
    pub fn into_postprocessor(
        self,
        config: Option<&Value>,
    ) -> Result<Box<dyn Postprocessor>, Error> {
        if let Some(compression_level) = config.get_i64("level") {
            match self {
                Algorithm::Xz2 => Ok(Xz2::with_config(compression_level)?),
                Algorithm::Zstd => Ok(Zstd::with_config(compression_level)?),
                // Algorithm::Lz4 => Ok(Lz4::with_config(compression_level)?),
                // _ => Err(Error::CompressionLevelNotSupported),
            }
        } else {
            let codec: Box<dyn Postprocessor> = match self {
                // Algorithm::Gzip => Box::<Gzip>::default(),
                // Algorithm::Zlib => Box::<Zlib>::default(),
                Algorithm::Xz2 => Xz2::with_default_config()?,
                Algorithm::Zstd => Zstd::with_default_config()?,
                // Algorithm::Snappy => Box::<Snappy>::default(),
                // Algorithm::Lz4 => Box::<Lz4>::default(),
                // Algorithm::Brotli => Box::<Brotli>::default(),
            };
            Ok(codec)
        }
    }
}

pub(crate) struct Compress {
    codec: Box<dyn Postprocessor>,
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
impl Postprocessor for Compress {
    fn name(&self) -> &str {
        "compress"
    }
    fn process(
        &mut self,
        ingres_ns: u64,
        egress_ns: u64,
        data: &[u8],
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        self.codec.process(ingres_ns, egress_ns, data)
    }

    fn finish(&mut self, data: Option<&[u8]>) -> anyhow::Result<Vec<Vec<u8>>> {
        self.codec.finish(data)
    }
}
type Reader = BlockingWrap<Arc<BlockingHeapRb<u8>>, false, true>;
type XzEncoder = xz2::write::XzEncoder<BlockingWrap<Arc<BlockingHeapRb<u8>>, true, false>>;

struct Xz2 {
    compression_level: u32,
    reader: Option<Reader>,
    encoder: Option<XzEncoder>,
}
impl Drop for Xz2 {
    fn drop(&mut self) {
        debug_assert!(
            self.encoder.is_none(),
            "Xz2 was not finalized before drop, this will result in incomplete data"
        );
    }
}

/// Xz2 Error
#[derive(Debug, thiserror::Error)]
pub enum Xz2Error {
    /// Invalid compressioh level
    #[error("Xz2 supports compression level between 0 and 9 but {0} was given")]
    InvalidCompressionLevel(i64),

    /// Try From Error
    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),
}

impl Xz2 {
    const DEFAULT_COMPRESSION_LEVEL: i64 = 6;
    const CAPACITY: usize = 1024 * 1024;

    fn with_default_config() -> Result<Box<dyn Postprocessor>, Xz2Error> {
        Self::with_config(Self::DEFAULT_COMPRESSION_LEVEL)
    }

    fn encoder(&mut self) -> &mut XzEncoder {
        // we need to do this in two steps to avoid double borrow of encoder
        if self.encoder.is_some() {
            if let Some(encoder) = self.encoder.as_mut() {
                return encoder;
            }
            unreachable!("encoder is some but encoder is none")
        }
        let rb = BlockingHeapRb::<u8>::new(Self::CAPACITY);
        let (writer, reader) = rb.split();

        let encoder = xz2::write::XzEncoder::new(writer, self.compression_level);
        self.reader = Some(reader);
        self.encoder.insert(encoder)
    }
    fn reader(&mut self) -> &mut Reader {
        // we need to do this in two steps to avoid double borrow of reader
        if self.reader.is_some() {
            if let Some(encoder) = self.reader.as_mut() {
                return encoder;
            }
            unreachable!("encoder is some but encoder is none")
        }
        let rb = BlockingHeapRb::<u8>::new(Self::CAPACITY);
        let (writer, reader) = rb.split();

        let encoder = xz2::write::XzEncoder::new(writer, self.compression_level);
        self.encoder = Some(encoder);
        self.reader.insert(reader)
    }

    fn with_config(level: i64) -> Result<Box<dyn Postprocessor>, Xz2Error> {
        if !(0..=9).contains(&level) {
            return Err(Xz2Error::InvalidCompressionLevel(level));
        }
        let compression_level = u32::try_from(level)?;

        Ok(Box::new(Self {
            compression_level,
            reader: None,
            encoder: None,
        }))
    }
}

impl Postprocessor for Xz2 {
    fn name(&self) -> &str {
        "streaming-xz2"
    }

    fn process(
        &mut self,
        _ingres_ns: u64,
        _egress_ns: u64,
        data: &[u8],
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        // Value of 0 indicates default level for encode.
        self.encoder().write_all(data)?;
        if self.reader().is_empty() {
            return Ok(Vec::new());
        }
        let mut compressed = vec![0; 1024];
        let mut res = Vec::new();
        loop {
            let read = self.reader().read(&mut compressed)?;
            if read < 1024 {
                compressed.truncate(read);
                break;
            }
            res.push(compressed[..read].to_vec());
        }
        res.push(compressed);
        Ok(res)
    }

    fn finish(&mut self, data: Option<&[u8]>) -> anyhow::Result<Vec<Vec<u8>>> {
        if let Some(data) = data {
            self.encoder().write_all(data)?;
        }
        if let Some(encoder) = self.encoder.take() {
            let mut w = encoder.finish()?;
            w.flush()?;
        };

        let mut out = Vec::new();
        if let Some(mut reader) = self.reader.take() {
            reader.read_to_end(&mut out)?;
        }
        Ok(vec![out])
    }
}

type ZstdEncoder = zstd::Encoder<'static, BlockingWrap<Arc<BlockingHeapRb<u8>>, true, false>>;
struct Zstd {
    compression_level: i32,
    encoder: ZstdEncoder,
    reader: Reader,
    finalized: bool,
}

impl std::fmt::Debug for Zstd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Zstd")
            .field("compression_level", &self.compression_level)
            .finish_non_exhaustive()
    }
}

/// zstd Error
#[derive(Debug, thiserror::Error)]
pub enum ZstdError {
    /// Invalid compressioh level
    #[error("Zstd supports compression level between -7 and 22 but {0} was given")]
    InvalidCompressionLevel(i64),

    /// Zstd error
    #[error(transparent)]
    Zstd(#[from] std::io::Error),
}

impl Drop for Zstd {
    fn drop(&mut self) {
        debug_assert!(
            self.finalized,
            "Zstd was not finalized before drop, this will result in incomplete data"
        );
    }
}

impl Zstd {
    const CAPACITY: usize = 1024 * 1024;

    fn with_default_config() -> Result<Box<dyn Postprocessor>, ZstdError> {
        Self::with_config(i64::from(zstd::DEFAULT_COMPRESSION_LEVEL))
    }

    pub fn with_config(level: i64) -> Result<Box<dyn Postprocessor>, ZstdError> {
        if !(-7..=22).contains(&level) {
            return Err(ZstdError::InvalidCompressionLevel(level));
        }

        let compression_level = level
            .try_into()
            .map_err(|_| ZstdError::InvalidCompressionLevel(level))?;

        let rb = BlockingHeapRb::<u8>::new(Self::CAPACITY);
        let (writer, reader) = rb.split();
        let encoder = zstd::Encoder::new(writer, compression_level)?;
        Ok(Box::new(Self {
            compression_level,
            encoder,
            reader,
            finalized: true,
        }))
    }
}
impl Postprocessor for Zstd {
    fn name(&self) -> &str {
        "streaming-zstd"
    }

    fn process(
        &mut self,
        _ingres_ns: u64,
        _egress_ns: u64,
        data: &[u8],
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        self.finalized = false;
        // Value of 0 indicates default level for encode.
        self.encoder.write_all(data)?;
        if self.reader.is_empty() {
            return Ok(Vec::new());
        }
        let mut compressed = vec![0; 1024];
        let mut res = Vec::new();
        loop {
            let read = self.reader.read(&mut compressed)?;
            if read < 1024 {
                compressed.truncate(read);
                break;
            }

            res.push(compressed[..read].to_vec());
        }
        res.push(compressed);
        Ok(res)
    }

    fn finish(&mut self, data: Option<&[u8]>) -> anyhow::Result<Vec<Vec<u8>>> {
        if let Some(data) = data {
            self.encoder.write_all(data)?;
        }
        let rb = BlockingHeapRb::<u8>::new(Self::CAPACITY);
        let (writer, new_reader) = rb.split();
        // swap out encoder and finish it
        let mut new_encoder = zstd::Encoder::new(writer, self.compression_level)?;
        std::mem::swap(&mut self.encoder, &mut new_encoder);
        let mut w = new_encoder.finish()?;
        w.flush()?;
        drop(w);

        let mut out = Vec::new();
        self.reader.read_to_end(&mut out)?;
        self.reader = new_reader;
        self.finalized = true;
        Ok(vec![out])
    }
}

mod tests {

    use super::*;
    // #[test]
    // fn name() {
    //     let post = Compress {
    //         codec: Box::<Gzip>::default(),
    //     };
    //     assert_eq!(post.name(), "compress");

    //     let post = Compress {
    //         codec: Box::<Brotli>::default(),
    //     };
    //     assert_eq!(post.name(), "compress");

    //     let post = Compress {
    //         codec: Box::<Zlib>::default(),
    //     };
    //     assert_eq!(post.name(), "compress");

    //     let post = Compress {
    //         codec: Box::<Xz2>::default(),
    //     };
    //     assert_eq!(post.name(), "compress");

    //     let post = Compress {
    //         codec: Box::<Snappy>::default(),
    //     };
    //     assert_eq!(post.name(), "compress");

    //     let post = Compress {
    //         codec: Box::<Lz4>::default(),
    //     };
    //     assert_eq!(post.name(), "compress");

    //     let post = Compress {
    //         codec: Box::<Zstd>::default(),
    //     };
    //     assert_eq!(post.name(), "compress");
    // }
    // #[test]
    // fn test_str_to_algorithm() -> anyhow::Result<()> {
    //     let algorithm = Algorithm::from_str("gzip")?;
    //     assert_eq!(algorithm, Algorithm::Gzip);
    //     let algorithm = Algorithm::from_str("xz2")?;
    //     assert_eq!(algorithm, Algorithm::Xz2);
    //     let algorithm = Algorithm::from_str("snappy")?;
    //     assert_eq!(algorithm, Algorithm::Snappy);
    //     let algorithm = Algorithm::from_str("zlib")?;
    //     assert_eq!(algorithm, Algorithm::Zlib);
    //     let algorithm = Algorithm::from_str("zstd")?;
    //     assert_eq!(algorithm, Algorithm::Zstd);

    //     let algorithm = Algorithm::from_str("gzi");
    //     assert!(algorithm.is_err());
    //     let algorithm = Algorithm::from_str("xz");
    //     assert!(algorithm.is_err());
    //     let algorithm = Algorithm::from_str("snapp");
    //     assert!(algorithm.is_err());
    //     let algorithm = Algorithm::from_str("zli");
    //     assert!(algorithm.is_err());
    //     let algorithm = Algorithm::from_str("zst");
    //     assert!(algorithm.is_err());

    //     Ok(())
    // }

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

    // #[test]
    // fn test_test_lz4() {
    //     let algorithm = Lz4::with_config(-1);
    //     assert!(algorithm.is_err());
    // }

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
    #[test]
    fn test_stream_zstd() -> anyhow::Result<()> {
        let mut algorithm = Zstd::with_default_config()?;

        let res = algorithm.process(0, 0, b"hello")?;
        let res2 = algorithm.process(0, 0, b" badger")?;
        let res3 = algorithm.finish(Some(b" world"))?;

        let data: Vec<u8> = res.into_iter().chain(res2).chain(res3).flatten().collect();

        assert_eq!(zstd::decode_all(data.as_slice())?, b"hello badger world");

        Ok(())
    }
    #[test]
    fn test_stream_xz2() -> anyhow::Result<()> {
        let mut algorithm = Xz2::with_default_config()?;

        let res = algorithm.process(0, 0, b"hello")?;
        let res2 = algorithm.process(0, 0, b" badger")?;
        let res3 = algorithm.finish(Some(b" world"))?;

        drop(algorithm);

        let data: Vec<u8> = res.into_iter().chain(res2).chain(res3).flatten().collect();
        let mut decoder = xz2::read::XzDecoder::new(data.as_slice());
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        assert_eq!(decompressed, b"hello badger world");
        Ok(())
    }
    #[test]
    fn test_ringbuf() -> anyhow::Result<()> {
        let rb = BlockingHeapRb::<u8>::new(1024);
        let (mut writer, mut reader) = rb.split();
        let data = b"hello world";
        writer.write_all(data)?;
        writer.flush()?;
        drop(writer);

        let mut out = Vec::new();
        reader.read_to_end(&mut out)?;
        assert_eq!(&out[..], data);
        Ok(())
    }
}
