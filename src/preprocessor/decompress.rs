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

//!Decompresses a data stream. It is assumed that each message reaching the decompressor is a complete compressed entity.
//!
//!The compression algorithm is detected automatically from the supported formats. If it can't be detected, the assumption is that the data was decompressed and will be sent on. Errors then can be transparently handled in the codec.
//!
//!Supported formats:
//!
//!## gzip
//!
//!Decompress GZ compressed payload.
//!
//!## lz4
//!
//!Decompress Lz4 compressed payload.
//!
//!## snappy
//!
//!Decompress framed snappy compressed payload (does not support raw snappy).
//!
//!## xz
//!
//!Decompress Xz2 (7z) compressed payload.
//!
//!## xz2
//!
//!Decompress xz and LZMA compressed payload.
//!
//!## zstd
//!
//!Decompress [Zstandard](https://datatracker.ietf.org/doc/html/rfc8878) compressed payload.
//!
//!## zlib
//!
//!Decompress Zlib (deflate) compressed payload.

use super::Preprocessor;
use crate::Result;
use simd_json::ValueAccess;
use std::io::{self, Read};
use tremor_value::Value;

#[derive(Clone, Default, Debug)]
struct Gzip {}
impl Preprocessor for Gzip {
    fn name(&self) -> &str {
        "gzip"
    }

    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use libflate::gzip::MultiDecoder;
        let mut decoder = MultiDecoder::new(data)?;
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(vec![decompressed])
    }
}

#[derive(Clone, Default, Debug)]
struct Zlib {}
impl Preprocessor for Zlib {
    fn name(&self) -> &str {
        "zlib"
    }

    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use libflate::zlib::Decoder;
        let mut decoder = Decoder::new(data)?;
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(vec![decompressed])
    }
}

#[derive(Clone, Default, Debug)]
struct Xz2 {}
impl Preprocessor for Xz2 {
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
struct Snappy {}
impl Preprocessor for Snappy {
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
struct Lz4 {}
impl Preprocessor for Lz4 {
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

#[derive(Clone, Debug, Default)]
struct Zstd {}
impl Preprocessor for Zstd {
    fn name(&self) -> &str {
        "ztd"
    }
    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let decoded: Vec<u8> = zstd::decode_all(data)?;
        Ok(vec![decoded])
    }
}

#[derive(Clone, Default, Debug)]
struct Fingerprinted {}
impl Preprocessor for Fingerprinted {
    fn name(&self) -> &str {
        "autodetect"
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
            Some(&[0x78, 0x01 | 0x5e | 0x9c | 0xda, _, _, _, _]) => {
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
            Some(b"sNaPpY" | &[0xff, _, _, _, _, _]) => {
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
            // Zstd Magic : 0xFD2FB528 (but little endian)
            Some(&[0x28, 0xb5, 0x2f, 0xfd, _, _]) => zstd::decode_all(data)?,
            _ => data.to_vec(),
        };
        Ok(vec![r])
    }
}

pub(crate) struct Decompress {
    codec: Box<dyn Preprocessor>,
}
impl Decompress {
    pub(crate) fn from_config(config: Option<&Value>) -> Result<Self> {
        let codec: Box<dyn Preprocessor> = match config.get_str("algorithm") {
            Some("gzip") => Box::new(Gzip::default()),
            Some("zlib") => Box::new(Zlib::default()),
            Some("xz2") => Box::new(Xz2::default()),
            Some("snappy") => Box::new(Snappy::default()),
            Some("lz4") => Box::new(Lz4::default()),
            Some("zstd") => Box::new(Zstd::default()),
            Some("autodetect") | None => Box::new(Fingerprinted::default()),
            Some(other) => return Err(format!("Unknown decompression algorithm: {other}").into()),
        };
        Ok(Decompress { codec })
    }
}
impl Preprocessor for Decompress {
    fn name(&self) -> &str {
        "decompress"
    }
    fn process(&mut self, ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        self.codec.process(ingest_ns, data)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::postprocessor::{self as post, Postprocessor};
    use tremor_value::literal;

    fn decode_magic(data: &[u8]) -> &'static str {
        match data.get(0..6) {
            Some(&[0x1f, 0x8b, _, _, _, _]) => "gzip",
            Some(&[0x78, _, _, _, _, _]) => "zlib",
            Some(&[0xfd, b'7', b'z', _, _, _]) => "xz2",
            Some(b"sNaPpY" | &[0xff, 0x6, 0x0, 0x0, _, _]) => "snappy",
            Some(&[0x04, 0x22, 0x4d, 0x18, _, _]) => "lz4",
            Some(&[0x28, 0xb5, 0x2f, 0xfd, _, _]) => "zstd",
            _ => "fail/unknown",
        }
    } // Assert pre and post processors have a sensible default() ctor

    fn assert_simple_symmetric(internal: &[u8], algo: &str) -> Result<()> {
        let config = literal!({ "algorithm": algo });
        let mut pre = super::Decompress::from_config(Some(&config))?;
        let mut post = post::compress::Compress::from_config(Some(&config))?;

        // Fake ingest_ns and egress_ns
        let mut ingest_ns = 0_u64;
        let egress_ns = 1_u64;

        let r = post.process(ingest_ns, egress_ns, internal);
        let ext = &r?[0];
        let ext = ext.as_slice();
        // Assert actual encoded form is as expected ( magic code only )
        assert_eq!(algo, decode_magic(ext));

        let r = pre.process(&mut ingest_ns, ext);
        let out = &r?[0];
        let out = out.as_slice();
        // Assert actual decoded form is as expected
        assert_eq!(&internal, &out);
        // assert empty finish, no leftovers
        assert!(pre.finish(None)?.is_empty());
        Ok(())
    }

    fn assert_fingerprinted_symmetric(internal: &[u8], algo: &str) -> Result<()> {
        let config_pre = literal!({ "algorithm": "autodetect" });
        let mut pre = super::Decompress::from_config(Some(&config_pre))?;

        let config_post = literal!({ "algorithm": algo });
        let mut post = post::compress::Compress::from_config(Some(&config_post))?;

        // Fake ingest_ns and egress_ns
        let mut ingest_ns = 0_u64;
        let egress_ns = 1_u64;

        let r = post.process(ingest_ns, egress_ns, internal);
        let ext = &r?[0];
        let ext = ext.as_slice();

        let r = pre.process(&mut ingest_ns, ext);
        let out = &r?[0];
        let out = out.as_slice();
        // Assert actual decoded form is as expected
        assert_eq!(&internal, &out);
        // assert empty finish, no leftovers
        assert!(pre.finish(None)?.is_empty());
        Ok(())
    }

    #[test]
    fn test_gzip() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric(int, "gzip")?;
        Ok(())
    }

    #[test]
    fn test_gzip_fingerprinted() -> Result<()> {
        let int = "snot".as_bytes();
        assert_fingerprinted_symmetric(int, "gzip")?;
        Ok(())
    }

    #[test]
    fn test_zlib() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric(int, "zlib")?;
        Ok(())
    }

    #[test]
    fn test_zlib_fingerprinted() -> Result<()> {
        let int = "snot".as_bytes();
        assert_fingerprinted_symmetric(int, "zlib")?;
        Ok(())
    }

    #[test]
    fn test_snappy() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric(int, "snappy")?;
        Ok(())
    }

    #[test]
    fn test_snappy_fingerprinted() -> Result<()> {
        let int = "snot".as_bytes();
        assert_fingerprinted_symmetric(int, "snappy")?;
        Ok(())
    }

    #[test]
    fn test_xz2() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric(int, "xz2")?;
        Ok(())
    }

    #[test]
    fn test_xz2_fingerprinted() -> Result<()> {
        let int = "snot".as_bytes();
        assert_fingerprinted_symmetric(int, "xz2")?;
        Ok(())
    }

    #[test]
    fn test_lz4() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric(int, "lz4")?;
        Ok(())
    }

    #[test]
    fn test_lz4_fingerprinted() -> Result<()> {
        let int = "snot".as_bytes();
        assert_fingerprinted_symmetric(int, "lz4")?;
        Ok(())
    }

    #[test]
    fn test_zstd() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric(int, "zstd")?;
        Ok(())
    }

    #[test]
    fn test_zstd_fingerprinted() -> Result<()> {
        let int = "snot".as_bytes();
        assert_fingerprinted_symmetric(int, "zstd")?;
        Ok(())
    }
}
