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

pub(crate) mod base64;
mod chunk;
pub(crate) mod collect;
pub(crate) mod compress;
pub(crate) mod gelf_chunking;
pub(crate) mod ingest_ns;
pub(crate) mod length_prefixed;
pub(crate) mod separate;
pub(crate) mod textual_length_prefixed;

use log::error;
use tremor_common::time::nanotime;
/// Set of Postprocessors
pub type Postprocessors = Vec<Box<dyn Postprocessor>>;
use std::{mem, str};

/// postprocessor error
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Postprocessor not found
    #[error("{0} Postprocessor not found.")]
    NotFound(String),
    /// Invalid Config
    #[error("{0} Invalid config: {1}")]
    InvalidConfig(&'static str, anyhow::Error),
    /// Missing Config
    #[error("{0} Missing config")]
    MissingConfig(&'static str),
}

/// Configuration for a postprocessor
pub type Config = tremor_config::NameWithConfig;

/// Postprocessor trait
pub trait Postprocessor: Send + Sync {
    /// Canonical name of the postprocessor
    fn name(&self) -> &str;
    /// process data
    ///
    /// # Errors
    ///
    ///   * Errors if the data could not be processed
    fn process(
        &mut self,
        ingres_ns: u64,
        egress_ns: u64,
        data: &[u8],
    ) -> anyhow::Result<Vec<Vec<u8>>>;

    /// Finish execution of this postprocessor.
    ///
    /// `data` is the result of the previous preprocessors `finish` execution if any,
    /// otherwise it is an empty slice.
    ///
    /// # Errors
    ///   * if the postprocessor could not be finished correctly
    fn finish(&mut self, _data: Option<&[u8]>) -> anyhow::Result<Vec<Vec<u8>>> {
        Ok(vec![])
    }
}

/// Lookup a postprocessor via its config
///
/// # Errors
///
///   * Errors if the postprocessor is not known

pub fn lookup_with_config(config: &Config) -> anyhow::Result<Box<dyn Postprocessor>> {
    match config.name.as_str() {
        "chunk" => Ok(Box::new(chunk::Chunk::from_config(config.config.as_ref())?)),
        "collect" => Ok(Box::new(collect::Postprocessor::from_config(
            &config.config,
        )?)),
        "compress" => Ok(Box::new(compress::Compress::from_config(
            config.config.as_ref(),
        )?)),
        "separate" => Ok(Box::new(separate::Separate::from_config(&config.config)?)),
        "base64" => Ok(Box::<base64::Base64>::default()),
        "ingest-ns" => Ok(Box::<ingest_ns::IngestNs>::default()),
        "length-prefixed" => Ok(Box::<length_prefixed::LengthPrefixed>::default()),
        "gelf-chunking" => Ok(Box::<gelf_chunking::Gelf>::default()),
        "textual-length-prefixed" => {
            Ok(Box::<textual_length_prefixed::TextualLengthPrefixed>::default())
        }
        name => Err(Error::NotFound(name.to_string()).into()),
    }
}

/// Lookup a postprocessor implementation via its unique name.
/// Only for backwards compatibility.
///
/// # Errors
///   * if the postprocessor with `name` is not known
pub fn lookup(name: &str) -> anyhow::Result<Box<dyn Postprocessor>> {
    lookup_with_config(&Config::from(name))
}

/// Given the slice of postprocessor names: Lookup each of them and return them as `Postprocessors`
///
/// # Errors
///
///   * If any postprocessor is not known.
pub fn make_postprocessors(postprocessors: &[Config]) -> anyhow::Result<Postprocessors> {
    postprocessors.iter().map(lookup_with_config).collect()
}

/// canonical way to process encoded data passed from a `Codec`
///
/// # Errors
///
///   * If a `Postprocessor` fails
pub fn postprocess(
    postprocessors: &mut [Box<dyn Postprocessor>], // We are borrowing a dyn box as we don't want to pass ownership.
    ingres_ns: u64,
    data: Vec<u8>,
    _alias: &str,
) -> anyhow::Result<Vec<Vec<u8>>> {
    let egress_ns = nanotime();
    let mut data = vec![data];
    let mut data1 = Vec::new();

    for pp in postprocessors {
        data1.clear();
        for d in &data {
            let mut r = pp.process(ingres_ns, egress_ns, d)?;
            data1.append(&mut r);
        }
        mem::swap(&mut data, &mut data1);
    }

    Ok(data)
}

/// Canonical way to finish postprocessors up
///
/// # Errors
///
/// * If a postprocessor failed
pub fn finish(
    postprocessors: &mut [Box<dyn Postprocessor>],
    alias: &str,
) -> anyhow::Result<Vec<Vec<u8>>> {
    if let Some((head, tail)) = postprocessors.split_first_mut() {
        let mut data = match head.finish(None) {
            Ok(d) => d,
            Err(e) => {
                error!(
                    "[Connector::{alias}] Postprocessor '{}' finish error: {e}",
                    head.name()
                );
                return Err(e);
            }
        };
        let mut data1 = Vec::new();
        for pp in tail {
            data1.clear();
            for d in &data {
                match pp.finish(Some(d)) {
                    Ok(mut r) => data1.append(&mut r),
                    Err(e) => {
                        error!(
                            "[Connector::{alias}] Postprocessor '{}' finish error: {e}",
                            pp.name()
                        );
                        return Err(e);
                    }
                }
            }
            std::mem::swap(&mut data, &mut data1);
        }
        Ok(data)
    } else {
        Ok(vec![])
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tremor_config::NameWithConfig;
    use tremor_value::literal;

    const LOOKUP_TABLE: [&str; 6] = [
        "separate",
        "base64",
        "gelf-chunking",
        "ingest-ns",
        "length-prefixed",
        "textual-length-prefixed",
    ];
    const COMPRESSION: [&str; 6] = ["gzip", "zlib", "xz2", "snappy", "lz4", "zstd"];

    #[test]
    fn test_lookup() {
        for t in &LOOKUP_TABLE {
            assert!(lookup(t).is_ok());
        }
        let t = "snot";
        assert!(lookup(t).is_err());

        assert!(lookup("bad_lookup").is_err());
    }

    #[test]
    fn test_lookup_compression() -> anyhow::Result<()> {
        for c in COMPRESSION {
            let config = literal!({"name": "compress", "config":{"algorithm": c}});
            let config = NameWithConfig::try_from(&config)?;
            assert!(lookup_with_config(&config).is_ok());
        }
        let config = literal!({"name": "compress", "config":{"algorithm": "snot"}});
        let config = NameWithConfig::try_from(&config)?;
        assert!(lookup_with_config(&config).is_err());
        Ok(())
    }

    #[test]
    fn base64() -> anyhow::Result<()> {
        let mut post = base64::Base64 {};
        let data: [u8; 0] = [];

        assert_eq!(post.process(0, 0, &data).ok(), Some(vec![vec![]]));

        assert_eq!(post.process(0, 0, b"\n").ok(), Some(vec![b"Cg==".to_vec()]));

        assert_eq!(
            post.process(0, 0, b"snot").ok(),
            Some(vec![b"c25vdA==".to_vec()])
        );

        assert!(post.finish(None)?.is_empty());
        Ok(())
    }

    #[test]
    fn textual_length_prefix_postp() -> anyhow::Result<()> {
        let mut post = textual_length_prefixed::TextualLengthPrefixed {};
        let data = vec![1_u8, 2, 3];
        let encoded = post.process(42, 23, &data)?.pop().unwrap_or_default();
        assert_eq!("3 \u{1}\u{2}\u{3}", str::from_utf8(&encoded)?);
        assert!(post.finish(None)?.is_empty());
        Ok(())
    }

    #[derive(Default)]
    struct Reverse {}

    impl Postprocessor for Reverse {
        fn name(&self) -> &str {
            "reverse"
        }

        fn process(
            &mut self,
            _ingres_ns: u64,
            _egress_ns: u64,
            data: &[u8],
        ) -> anyhow::Result<Vec<Vec<u8>>> {
            let mut data = data.to_vec();
            data.reverse();
            Ok(vec![data])
        }

        fn finish(&mut self, data: Option<&[u8]>) -> anyhow::Result<Vec<Vec<u8>>> {
            if let Some(data) = data {
                let mut data = data.to_vec();
                data.reverse();
                Ok(vec![data])
            } else {
                Ok(vec![])
            }
        }
    }

    #[derive(Default)]
    struct BadProcessor {}

    impl Postprocessor for BadProcessor {
        fn name(&self) -> &str {
            "nah-proc"
        }

        fn process(
            &mut self,
            _ingres_ns: u64,
            _egress_ns: u64,
            _data: &[u8],
        ) -> anyhow::Result<Vec<Vec<u8>>> {
            Err(anyhow::format_err!("nah"))
        }

        fn finish(&mut self, _data: Option<&[u8]>) -> anyhow::Result<Vec<Vec<u8>>> {
            Ok(vec![])
        }
    }

    #[derive(Default)]
    struct BadFinisher {}

    impl Postprocessor for BadFinisher {
        fn name(&self) -> &str {
            "reverse"
        }

        fn process(
            &mut self,
            _ingres_ns: u64,
            _egress_ns: u64,
            _data: &[u8],
        ) -> anyhow::Result<Vec<Vec<u8>>> {
            Ok(vec![b"snot".to_vec()]) // NOTE has to be non-empty for finish err to trigger in tests
        }

        fn finish(&mut self, _data: Option<&[u8]>) -> anyhow::Result<Vec<Vec<u8>>> {
            Err(anyhow::format_err!("nah"))
        }
    }

    #[test]
    fn postprocess_ok() -> anyhow::Result<()> {
        let mut posties: Vec<Box<dyn Postprocessor>> = vec![
            Box::<Reverse>::default(),
            Box::<separate::Separate>::default(),
        ];

        let data: Vec<u8> = b"\ntons\nregdab\n".to_vec();
        let encoded = postprocess(&mut posties, 42, data, "test")?;
        assert_eq!(1, encoded.len());
        assert_eq!("\nbadger\nsnot\n\n", str::from_utf8(&encoded[0])?);

        let encoded = finish(&mut posties, "test")?;
        assert_eq!(0, encoded.len());

        let mut seitsop: Vec<Box<dyn Postprocessor>> = vec![
            Box::<separate::Separate>::default(),
            Box::<Reverse>::default(),
        ];

        let data: Vec<u8> = b"\ntons\nregdab\n".to_vec();
        let encoded = postprocess(&mut seitsop, 42, data, "test")?;
        assert_eq!(1, encoded.len());
        assert_eq!("\n\nbadger\nsnot\n", str::from_utf8(&encoded[0])?);

        let encoded = finish(&mut seitsop, "test")?;
        assert_eq!(0, encoded.len());

        Ok(())
    }

    #[test]
    fn appease_coverage_gods() {
        let mut a = Box::<BadProcessor>::default();
        let mut b = Box::<BadFinisher>::default();
        let mut c = Box::<Reverse>::default();

        a.name();
        b.name();
        c.name();

        assert!(a.process(0, 0, &[]).is_err());
        assert!(b.process(0, 0, &[]).is_ok());
        assert!(c.process(0, 0, &[]).is_ok());

        let data: Vec<u8> = b"donotcare".to_vec();
        assert!(a.finish(Some(&data)).is_ok());
        assert!(b.finish(Some(&data)).is_err());
        assert!(c.finish(Some(&data)).is_ok());
    }

    #[test]
    fn postprocess_process_err() {
        let mut seitsop: Vec<Box<dyn Postprocessor>> = vec![
            Box::<separate::Separate>::default(),
            Box::<Reverse>::default(),
            Box::<BadProcessor>::default(),
        ];

        let data: Vec<u8> = b"\ntons\nregdab\n".to_vec();
        let encoded = postprocess(&mut seitsop, 42, data, "test");
        assert!(encoded.is_err());
    }

    #[test]
    fn postprocess_finish_err() {
        let mut seitsop: Vec<Box<dyn Postprocessor>> =
            vec![Box::<BadFinisher>::default(), Box::<BadFinisher>::default()];

        let encoded = finish(&mut seitsop, "test");
        assert!(encoded.is_err());
    }
}
