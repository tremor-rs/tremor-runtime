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

mod compress;
mod gelf;
pub(crate) mod separate;

use crate::config::Postprocessor as PostprocessorConfig;
use crate::errors::Result;
use byteorder::{BigEndian, WriteBytesExt};
pub(crate) use gelf::Gelf;
use std::default::Default;
use tremor_common::time::nanotime;
/// Set of Postprocessors
pub type Postprocessors = Vec<Box<dyn Postprocessor>>;
pub(crate) use compress::Compress;
use std::{io::Write, mem, str};

trait PostprocessorState {}
/// Postprocessor trait
pub trait Postprocessor: Send + Sync {
    /// Canonical name of the postprocessor
    fn name(&self) -> &str;
    /// process data
    ///
    /// # Errors
    ///
    ///   * Errors if the data could not be processed
    fn process(&mut self, ingres_ns: u64, egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>>;

    /// Finish execution of this postprocessor.
    ///
    /// `data` is the result of the previous preprocessors `finish` execution if any,
    /// otherwise it is an empty slice.
    ///
    /// # Errors
    ///   * if the postprocessor could not be finished correctly
    fn finish(&mut self, _data: Option<&[u8]>) -> Result<Vec<Vec<u8>>> {
        Ok(vec![])
    }
}

/// Lookup a postprocessor via its config
///
/// # Errors
///
///   * Errors if the postprocessor is not known

pub fn lookup_with_config(config: &PostprocessorConfig) -> Result<Box<dyn Postprocessor>> {
    match config.name.as_str() {
        "compress" => Ok(Box::new(Compress::from_config(config.config.as_ref())?)),
        "separate" => Ok(Box::new(separate::Separate::from_config(&config.config)?)),
        "base64" => Ok(Box::new(Base64::default())),
        "ingest-ns" => Ok(Box::new(AttachIngresTs {})),
        "length-prefixed" => Ok(Box::new(LengthPrefix::default())),
        "gelf-chunking" => Ok(Box::new(Gelf::default())),
        "textual-length-prefix" => Ok(Box::new(TextualLength::default())),
        name => Err(format!("Postprocessor '{}' not found.", name).into()),
    }
}

/// Lookup a postprocessor implementation via its unique name.
/// Only for backwards compatibility.
///
/// # Errors
///   * if the postprocessor with `name` is not known
pub fn lookup(name: &str) -> Result<Box<dyn Postprocessor>> {
    lookup_with_config(&PostprocessorConfig::from(name))
}

/// Given the slice of postprocessor names: Lookup each of them and return them as `Postprocessors`
///
/// # Errors
///
///   * If any postprocessor is not known.
pub fn make_postprocessors(postprocessors: &[PostprocessorConfig]) -> Result<Postprocessors> {
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
    alias: &str,
) -> Result<Vec<Vec<u8>>> {
    let egress_ns = nanotime();
    let mut data = vec![data];
    let mut data1 = Vec::new();

    for pp in postprocessors {
        data1.clear();
        for d in &data {
            let mut r = pp
                .process(ingres_ns, egress_ns, d)
                .map_err(|e| format!("[Connector::{alias}] Postprocessor error {e}"))?;
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
pub fn finish(postprocessors: &mut [Box<dyn Postprocessor>], alias: &str) -> Result<Vec<Vec<u8>>> {
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

#[derive(Default)]
pub(crate) struct Base64 {}
impl Postprocessor for Base64 {
    fn name(&self) -> &str {
        "base64"
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(vec![base64::encode(data).as_bytes().to_vec()])
    }
}

pub(crate) struct AttachIngresTs {}
impl Postprocessor for AttachIngresTs {
    fn name(&self) -> &str {
        "attach-ingress-ts"
    }

    fn process(&mut self, ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let mut res = Vec::with_capacity(data.len() + 8);
        res.write_u64::<BigEndian>(ingres_ns)?;
        res.write_all(data)?;

        Ok(vec![res])
    }
}

#[derive(Clone, Default)]
pub(crate) struct LengthPrefix {}
impl Postprocessor for LengthPrefix {
    fn name(&self) -> &str {
        "length-prefix"
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let mut res = Vec::with_capacity(data.len() + 8);
        res.write_u64::<BigEndian>(data.len() as u64)?;
        res.write_all(data)?;
        Ok(vec![res])
    }
}

#[derive(Clone, Default)]
pub(crate) struct TextualLength {}
impl Postprocessor for TextualLength {
    fn name(&self) -> &str {
        "textual-length-prefix"
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let size = data.len();
        let mut digits: Vec<u8> = size.to_string().into_bytes();
        let mut res = Vec::with_capacity(digits.len() + 1 + size);
        res.append(&mut digits);
        res.push(32);
        res.write_all(data)?;
        Ok(vec![res])
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config::NameWithConfig;
    use tremor_value::literal;

    const LOOKUP_TABLE: [&str; 6] = [
        "separate",
        "base64",
        "gelf-chunking",
        "ingest-ns",
        "length-prefixed",
        "textual-length-prefix",
    ];
    const COMPRESSION: [&str; 6] = ["gzip", "zlib", "xz2", "snappy", "lz4", "zstd"];

    #[test]
    fn test_lookup() {
        for t in &LOOKUP_TABLE {
            dbg!(t);
            assert!(lookup(t).is_ok());
        }
        let t = "snot";
        assert!(lookup(t).is_err());
    }

    #[test]
    fn test_lookup_compression() -> Result<()> {
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
    fn base64() -> Result<()> {
        let mut post = Base64 {};
        let data: [u8; 0] = [];

        assert_eq!(Ok(vec![vec![]]), post.process(0, 0, &data));

        assert_eq!(Ok(vec![b"Cg==".to_vec()]), post.process(0, 0, b"\n"));

        assert_eq!(Ok(vec![b"c25vdA==".to_vec()]), post.process(0, 0, b"snot"));

        assert!(post.finish(None)?.is_empty());
        Ok(())
    }

    #[test]
    fn textual_length_prefix_postp() -> Result<()> {
        let mut post = TextualLength {};
        let data = vec![1_u8, 2, 3];
        let encoded = post.process(42, 23, &data)?.pop().unwrap_or_default();
        assert_eq!("3 \u{1}\u{2}\u{3}", str::from_utf8(&encoded)?);
        assert!(post.finish(None)?.is_empty());
        Ok(())
    }
}
