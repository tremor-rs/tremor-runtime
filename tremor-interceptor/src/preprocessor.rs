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

#[cfg(feature = "base64")]
mod base64;
#[cfg(feature = "compression")]
mod decompress;
#[cfg(feature = "gelf")]
pub(crate) mod gelf_chunking;

#[cfg(feature = "length-prefix")]
mod length_prefixed;

#[cfg(feature = "length-prefix")]
mod textual_length_prefixed;

pub(crate) mod ingest_ns;
mod remove_empty;
pub(crate) mod separate;
pub(crate) mod prelude {
    pub use super::Preprocessor;
    pub use tremor_value::Value;
    pub use value_trait::prelude::*;
}
use self::prelude::*;
use log::error;
use tremor_common::alias::Connector as Alias;

/// postprocessor error
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Preprocessor not found
    #[error("{0} Preprocessor not found.")]
    NotFound(String),
    /// Invalid Config
    #[error("{0} Invalid config: {1}")]
    InvalidConfig(&'static str, anyhow::Error),
    /// Missing Config
    #[error("{0} Missing config")]
    MissingConfig(&'static str),
}
/// Configuration for a preprocessor
pub type Config = tremor_config::NameWithConfig;

//pub type Lines = lines::Lines;

/// A set of preprocessors
pub type Preprocessors = Vec<Box<dyn Preprocessor>>;

/// Preprocessor trait
pub trait Preprocessor: Sync + Send {
    /// Canonical name for this preprocessor
    fn name(&self) -> &str;
    /// process data
    ///
    /// # Errors
    ///
    /// * Errors if the data can not processed
    fn process(
        &mut self,
        ingest_ns: &mut u64,
        data: &[u8],
        meta: Value<'static>,
    ) -> anyhow::Result<Vec<(Vec<u8>, Value<'static>)>>;

    /// Finish processing data and emit anything that might be left.
    /// Takes a `data` buffer of input data, that is potentially empty,
    /// especially if this is the first preprocessor in a chain.
    ///
    /// # Errors
    ///
    /// * if finishing fails for some reason lol
    fn finish(
        &mut self,
        _data: Option<&[u8]>,
        _meta: Option<Value<'static>>,
    ) -> anyhow::Result<Vec<(Vec<u8>, Value<'static>)>> {
        Ok(vec![])
    }
}

/// Lookup a preprocessor implementation via its configuration
///
/// # Errors
///
///   * Errors if the preprocessor is not known
pub fn lookup_with_config(config: &Config) -> Result<Box<dyn Preprocessor>, Error> {
    match config.name.as_str() {
        "remove-empty" => Ok(Box::<remove_empty::RemoveEmpty>::default()),
        "separate" => Ok(Box::new(separate::Separate::from_config(&config.config)?)),
        "ingest-ns" => Ok(Box::<ingest_ns::ExtractIngestTs>::default()),
        #[cfg(feature = "base64")]
        "base64" => Ok(Box::<base64::Base64>::default()),
        #[cfg(feature = "compression")]
        "decompress" => Ok(Box::new(decompress::Decompress::from_config(
            config.config.as_ref(),
        )?)),
        #[cfg(feature = "gelf")]
        "gelf-chunking" => Ok(Box::<gelf_chunking::GelfChunking>::default()),
        #[cfg(feature = "length-prefix")]
        "length-prefixed" => Ok(Box::<length_prefixed::LengthPrefixed>::default()),
        #[cfg(feature = "length-prefix")]
        "textual-length-prefixed" => {
            Ok(Box::<textual_length_prefixed::TextualLengthPrefixed>::default())
        }
        name => Err(Error::NotFound(name.to_string())),
    }
}

/// Lookup a preprocessor implementation via its unique id
///
/// # Errors
///
/// * if the preprocessor with `name` is not known
pub fn lookup(name: &str) -> Result<Box<dyn Preprocessor>, Error> {
    lookup_with_config(&Config::from(name))
}

/// Given the slice of preprocessor names: Look them up and return them as `Preprocessors`.
///
/// # Errors
///
///   * If the preprocessor is not known.
pub fn make_preprocessors(preprocessors: &[Config]) -> Result<Preprocessors, Error> {
    preprocessors.iter().map(lookup_with_config).collect()
}

/// Canonical way to preprocess data before it is fed to a codec for decoding.
///
/// Preprocessors might split up the given data in multiple chunks. Each of those
/// chunks must be seperately decoded by a `Codec`.
///
/// # Errors
///
///   * If a preprocessor failed
pub fn preprocess(
    preprocessors: &mut [Box<dyn Preprocessor>],
    ingest_ns: &mut u64,
    data: Vec<u8>,
    meta: Value<'static>,
    alias: &Alias,
) -> anyhow::Result<Vec<(Vec<u8>, Value<'static>)>> {
    let mut data = vec![(data, meta)];
    let mut data1 = Vec::new();
    for pp in preprocessors {
        for (i, (d, m)) in data.drain(..).enumerate() {
            match pp.process(ingest_ns, &d, m) {
                Ok(mut r) => data1.append(&mut r),
                Err(e) => {
                    error!("[Connector::{alias}] Preprocessor [{i}] error: {e}");
                    return Err(e);
                }
            }
        }
        std::mem::swap(&mut data, &mut data1);
    }
    Ok(data)
}

fn finish_and_error(
    alias: &Alias,
    pp: &mut dyn Preprocessor,
    data: Option<&[u8]>,
    meta: Option<Value<'static>>,
) -> anyhow::Result<Vec<(Vec<u8>, Value<'static>)>> {
    let res = pp.finish(data, meta);
    if let Err(e) = &res {
        error!(
            "[Connector::{alias}] Preprocessor '{}' finish error: {e}",
            pp.name()
        );
    }
    res
}
/// Canonical way to finish preprocessors up
///
/// # Errors
///
/// * If a preprocessor failed
pub fn finish(
    preprocessors: &mut [Box<dyn Preprocessor>],
    alias: &Alias,
) -> anyhow::Result<Vec<(Vec<u8>, Value<'static>)>> {
    if let Some((head, tail)) = preprocessors.split_first_mut() {
        let mut data = finish_and_error(alias, head.as_mut(), None, None)?;
        let mut data1 = Vec::new();
        for pp in tail {
            if data.is_empty() {
                let mut r = finish_and_error(alias, pp.as_mut(), None, None)?;
                data1.append(&mut r);
            }
            for (d, m) in data.drain(..) {
                let mut r = finish_and_error(alias, pp.as_mut(), Some(&d), Some(m))?;
                data1.append(&mut r);
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
    #![allow(clippy::ignored_unit_patterns)]
    use super::*;

    #[test]
    fn test_lookup_separate() {
        assert!(lookup("separate").is_ok());
    }
    #[test]
    fn test_lookup_remove_empty() {
        assert!(lookup("remove-empty").is_ok());
    }
    #[test]
    fn test_lookup_ingest_ns() {
        assert!(lookup("ingest-ns").is_ok());
    }
    #[test]
    fn test_lookup_errors() {
        let t = "snot";
        assert!(lookup(t).is_err());

        assert!(lookup("bad_lookup").is_err());
    }

    #[cfg(feature = "gelf")]
    #[test]
    fn test_lookup_gelf() {
        assert!(lookup("gelf-chunking").is_ok());
    }

    #[cfg(feature = "compression")]
    #[test]
    fn test_lookup_compression() {
        assert!(lookup("decompress").is_ok());
    }

    #[cfg(feature = "base64")]
    #[test]
    fn test_lookup_base64() {
        assert!(lookup("base64").is_ok());
    }

    #[cfg(feature = "length-prefix")]
    #[test]
    fn test_lookup_length_prefix() {
        assert!(lookup("length-prefixed").is_ok());
        assert!(lookup("textual-length-prefixed").is_ok());
    }
    #[test]
    fn test_filter_empty() {
        let mut pre = remove_empty::RemoveEmpty::default();
        assert_eq!(
            pre.process(&mut 0_u64, &[], Value::object()).ok(),
            Some(vec![])
        );
        assert_eq!(pre.finish(None, None).ok(), Some(vec![]));
    }

    #[test]
    fn test_filter_null() {
        let mut pre = remove_empty::RemoveEmpty::default();
        assert_eq!(
            pre.process(&mut 0_u64, &[], Value::object()).ok(),
            Some(vec![])
        );
        assert_eq!(pre.finish(None, None).ok(), Some(vec![]));
    }

    struct BadPreprocessor {}
    impl Preprocessor for BadPreprocessor {
        fn name(&self) -> &'static str {
            "chucky"
        }

        fn process(
            &mut self,
            _ingest_ns: &mut u64,
            _data: &[u8],
            _meta: Value<'static>,
        ) -> anyhow::Result<Vec<(Vec<u8>, Value<'static>)>> {
            Err(anyhow::format_err!("chucky"))
        }
        fn finish(
            &mut self,
            _data: Option<&[u8]>,
            _meta: Option<Value<'static>>,
        ) -> anyhow::Result<Vec<(Vec<u8>, Value<'static>)>> {
            Ok(vec![])
        }
    }

    struct BadFinisher {}
    impl Preprocessor for BadFinisher {
        fn name(&self) -> &'static str {
            "chucky"
        }

        fn process(
            &mut self,
            _ingest_ns: &mut u64,
            _data: &[u8],
            _meta: Value<'static>,
        ) -> anyhow::Result<Vec<(Vec<u8>, Value<'static>)>> {
            Ok(vec![])
        }

        fn finish(
            &mut self,
            _data: Option<&[u8]>,
            _meta: Option<Value<'static>>,
        ) -> anyhow::Result<Vec<(Vec<u8>, Value<'static>)>> {
            Err(anyhow::format_err!("chucky revange"))
        }
    }

    struct NoOp {}
    impl Preprocessor for NoOp {
        fn name(&self) -> &'static str {
            "nily"
        }

        fn process(
            &mut self,
            _ingest_ns: &mut u64,
            _data: &[u8],
            meta: Value<'static>,
        ) -> anyhow::Result<Vec<(Vec<u8>, Value<'static>)>> {
            Ok(vec![(b"non".to_vec(), meta)])
        }
        fn finish(
            &mut self,
            _data: Option<&[u8]>,
            meta: Option<Value<'static>>,
        ) -> anyhow::Result<Vec<(Vec<u8>, Value<'static>)>> {
            Ok(vec![(b"nein".to_vec(), meta.unwrap_or_else(Value::object))])
        }
    }

    #[test]
    fn badly_behaved_process() {
        let mut pre = Box::new(BadPreprocessor {});
        assert_eq!("chucky", pre.name());

        let mut ingest_ns = 0_u64;
        let r = pre.process(&mut ingest_ns, b"foo", Value::object());
        assert!(r.is_err());

        let r = pre.finish(Some(b"foo"), Some(Value::object()));
        assert!(r.is_ok());
    }

    #[test]
    fn badly_behaved_finish() {
        let mut pre = Box::new(BadFinisher {});
        assert_eq!("chucky", pre.name());

        let mut ingest_ns = 0_u64;
        let r = pre.process(&mut ingest_ns, b"foo", Value::object());
        assert!(r.is_ok());

        let r = pre.finish(Some(b"foo"), Some(Value::object()));
        assert!(r.is_err());
    }

    #[test]
    fn single_pre_process_head_ok() {
        let pre = Box::new(BadPreprocessor {});
        let alias = tremor_common::alias::Connector::new(
            tremor_common::alias::Flow::new("chucky"),
            "chucky".to_string(),
        );
        let mut ingest_ns = 0_u64;
        let r = preprocess(
            &mut [pre],
            &mut ingest_ns,
            b"foo".to_vec(),
            Value::object(),
            &alias,
        );
        assert!(r.is_err());
    }

    #[test]
    fn single_pre_process_tail_err() {
        let noop = Box::new(NoOp {});
        assert_eq!("nily", noop.name());
        let pre = Box::new(BadPreprocessor {});
        let alias = tremor_common::alias::Connector::new(
            tremor_common::alias::Flow::new("chucky"),
            "chucky".to_string(),
        );
        let mut ingest_ns = 0_u64;
        let r = preprocess(
            &mut [noop, pre],
            &mut ingest_ns,
            b"foo".to_vec(),
            Value::object(),
            &alias,
        );
        assert!(r.is_err());
    }

    #[test]
    fn single_pre_finish_ok() {
        let pre = Box::new(BadPreprocessor {});
        let alias = tremor_common::alias::Connector::new(
            tremor_common::alias::Flow::new("chucky"),
            "chucky".to_string(),
        );
        let r = finish(&mut [pre], &alias);
        assert!(r.is_ok());
    }

    #[test]
    fn direct_pre_finish_err() {
        let mut pre = Box::new(BadFinisher {});
        let r = pre.finish(Some(b"foo"), Some(Value::object()));
        assert!(r.is_err());
    }

    #[test]
    fn preprocess_finish_head_fail() {
        let alias = tremor_common::alias::Connector::new(
            tremor_common::alias::Flow::new("chucky"),
            "chucky".to_string(),
        );
        let pre = Box::new(BadFinisher {});
        let r = finish(&mut [pre], &alias);
        assert!(r.is_err());
    }

    #[test]
    fn preprocess_finish_tail_fail() {
        let alias = tremor_common::alias::Connector::new(
            tremor_common::alias::Flow::new("chucky"),
            "chucky".to_string(),
        );
        let noop = Box::new(NoOp {});
        let pre = Box::new(BadFinisher {});
        let r = finish(&mut [noop, pre], &alias);
        assert!(r.is_err());
    }

    #[test]
    fn preprocess_finish_multi_ok() {
        let alias = tremor_common::alias::Connector::new(
            tremor_common::alias::Flow::new("xyz"),
            "xyz".to_string(),
        );
        let noop1 = Box::new(NoOp {});
        let noop2 = Box::new(NoOp {});
        let noop3 = Box::new(NoOp {});
        let r = finish(&mut [noop1, noop2, noop3], &alias);
        assert!(r.is_ok());
    }
}
