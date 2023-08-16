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

mod base64;
mod decompress;
pub(crate) mod gelf_chunking;
mod ingest_ns;
mod length_prefixed;
mod remove_empty;
pub(crate) mod separate;
mod textual_length_prefixed;
pub(crate) mod prelude {
    pub use super::Preprocessor;
    pub use crate::errors::Result;
    pub use tremor_value::Value;
    pub use value_trait::Builder;
}
use self::prelude::*;
use crate::{config::Preprocessor as PreprocessorConfig, connectors::Alias, errors::Result};

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
    ) -> Result<Vec<(Vec<u8>, Value<'static>)>>;

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
    ) -> Result<Vec<(Vec<u8>, Value<'static>)>> {
        Ok(vec![])
    }
}

/// Lookup a preprocessor implementation via its configuration
///
/// # Errors
///
///   * Errors if the preprocessor is not known
pub fn lookup_with_config(config: &PreprocessorConfig) -> Result<Box<dyn Preprocessor>> {
    match config.name.as_str() {
        "separate" => Ok(Box::new(separate::Separate::from_config(&config.config)?)),
        "base64" => Ok(Box::<base64::Base64>::default()),
        "decompress" => Ok(Box::new(decompress::Decompress::from_config(
            config.config.as_ref(),
        )?)),
        "remove-empty" => Ok(Box::<remove_empty::RemoveEmpty>::default()),
        "gelf-chunking" => Ok(Box::<gelf_chunking::GelfChunking>::default()),
        "ingest-ns" => Ok(Box::<ingest_ns::ExtractIngestTs>::default()),
        "length-prefixed" => Ok(Box::<length_prefixed::LengthPrefixed>::default()),
        "textual-length-prefixed" => {
            Ok(Box::<textual_length_prefixed::TextualLengthPrefixed>::default())
        }
        name => Err(format!("Preprocessor '{name}' not found.").into()),
    }
}

/// Lookup a preprocessor implementation via its unique id
///
/// # Errors
///
/// * if the preprocessor with `name` is not known
pub fn lookup(name: &str) -> Result<Box<dyn Preprocessor>> {
    lookup_with_config(&PreprocessorConfig::from(name))
}

/// Given the slice of preprocessor names: Look them up and return them as `Preprocessors`.
///
/// # Errors
///
///   * If the preprocessor is not known.
pub fn make_preprocessors(preprocessors: &[PreprocessorConfig]) -> Result<Preprocessors> {
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
) -> Result<Vec<(Vec<u8>, Value<'static>)>> {
    let mut data = vec![(data, meta)];
    let mut data1 = Vec::new();
    for pp in preprocessors {
        for (i, (d, m)) in data.drain(..).enumerate() {
            match pp.process(ingest_ns, &d, m) {
                // FIXME: can we avoid this clone?
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

/// Canonical way to finish preprocessors up
///
/// # Errors
///
/// * If a preprocessor failed
pub fn finish(
    preprocessors: &mut [Box<dyn Preprocessor>],
    alias: &Alias,
) -> Result<Vec<(Vec<u8>, Value<'static>)>> {
    if let Some((head, tail)) = preprocessors.split_first_mut() {
        let mut data = match head.finish(None, None) {
            Ok(d) => d,
            Err(e) => {
                error!(
                    "[Connector::{alias}] Preprocessor '{}' finish error: {e}",
                    head.name()
                );
                return Err(e);
            }
        };
        let mut data1 = Vec::new();
        for pp in tail {
            for (d, m) in data.drain(..) {
                // FIXME: can we make this owned?
                match pp.finish(Some(&d), Some(m)) {
                    Ok(mut r) => data1.append(&mut r),
                    Err(e) => {
                        error!(
                            "[Connector::{alias}] Preprocessor '{}' finish error: {e}",
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
    #![allow(clippy::ignored_unit_patterns)]
    use super::*;
    use crate::postprocessor::{self as post, separate::Separate as SeparatePost, Postprocessor};
    use crate::Result;

    #[test]
    fn ingest_ts() -> Result<()> {
        let mut pre_p = ingest_ns::ExtractIngestTs {};
        let mut post_p = post::ingest_ns::IngestNs {};

        let data = vec![1_u8, 2, 3];

        let encoded = post_p.process(42, 23, &data)?.pop().ok_or("no data")?;

        let mut in_ns = 0u64;
        let decoded = pre_p
            .process(&mut in_ns, &encoded, Value::object())?
            .pop()
            .ok_or("no data")?
            .0;

        assert!(pre_p.finish(None, None)?.is_empty());

        assert_eq!(data, decoded);
        assert_eq!(in_ns, 42);

        // data too short
        assert!(pre_p.process(&mut in_ns, &[0_u8], Value::object()).is_err());
        Ok(())
    }

    fn textual_prefix(len: usize) -> String {
        format!("{len} {}", String::from_utf8_lossy(&vec![b'O'; len]))
    }

    use proptest::prelude::*;

    // generate multiple chopped length-prefixed strings
    fn multiple_textual_lengths(max_elements: usize) -> BoxedStrategy<(Vec<usize>, Vec<String>)> {
        proptest::collection::vec(".+", 1..max_elements) // generator for Vec<String> of arbitrary strings, maximum length of vector: `max_elements`
            .prop_map(|ss| {
                let s: (Vec<usize>, Vec<String>) = ss
                    .into_iter()
                    .map(|s| (s.len(), format!("{} {s}", s.len()))) // for each string, extract the length, and create a textual length prefix
                    .unzip();
                s
            })
            .prop_map(|tuple| (tuple.0, tuple.1.join(""))) // generator for a tuple of 1. the sizes of the length prefixed strings, 2. the concatenated length prefixed strings as one giant string
            .prop_map(|tuple| {
                // here we chop the big string into up to 4 bits
                let mut chopped = Vec::with_capacity(4);
                let mut giant_string: String = tuple.1.clone();
                while !giant_string.is_empty() && chopped.len() < 4 {
                    // verify we are at a char boundary
                    let mut indices = giant_string.char_indices();
                    let num_chars = giant_string.chars().count();
                    if let Some((index, _)) = indices.nth(num_chars / 2) {
                        let mut splitted = giant_string.split_off(index);
                        std::mem::swap(&mut splitted, &mut giant_string);
                        chopped.push(splitted);
                    } else {
                        break;
                    }
                }
                chopped.push(giant_string);
                (tuple.0, chopped)
            })
            .boxed()
    }

    proptest! {
        #[test]
        fn textual_length_prefix_prop((lengths, datas) in multiple_textual_lengths(5)) {
            let mut pre_p = textual_length_prefixed::TextualLengthPrefixed::default();
            let mut in_ns = 0_u64;
            let res: Vec<_> = datas.into_iter().flat_map(|data| {
                pre_p.process(&mut in_ns, data.as_bytes(), Value::object()).unwrap_or_default()
            }).collect();
            assert_eq!(lengths.len(), res.len());
            for (processed, expected_len) in res.iter().zip(lengths) {
                assert_eq!(expected_len, processed.0.len());
            }
        }

        #[test]
        fn textual_length_pre_post(length in 1..100_usize) {
            let data = vec![1_u8; length];
            let mut pre_p = textual_length_prefixed::TextualLengthPrefixed::default();
            let mut post_p = post::textual_length_prefixed::TextualLengthPrefixed::default();
            let encoded = post_p.process(0, 0, &data).unwrap_or_default().pop().unwrap_or_default();
            let mut in_ns = 0_u64;
            let mut res = pre_p.process(&mut in_ns, &encoded, Value::object()).unwrap_or_default();
            assert_eq!(1, res.len());
            let payload = res.pop().unwrap_or_default().0;
            assert_eq!(length, payload.len());
        }
    }

    #[test]
    fn textual_prefix_length_loop() {
        let datas = vec![
            "24 \'?\u{d617e}ѨR\u{202e}\u{f8f7c}\u{ede29}\u{ac784}36 ?{¥?MȺ\r\u{bac41}9\u{5bbbb}\r\u{1c46c}\u{4ba79}¥\u{7f}*?:\u{0}$i",
            "60 %\u{a825a}\u{a4269}\u{39e0c}\u{b3e21}<ì\u{f6c20}ѨÛ`HW\u{9523f}V",
            "\u{3}\u{605fe}%Fq\u{89b5e}\u{93780}Q3",
            "¥?\u{feff}9",
            " \'�2\u{4269b}",
        ];
        let lengths: Vec<usize> = vec![24, 36, 60, 9];
        let mut pre_p = textual_length_prefixed::TextualLengthPrefixed::default();
        let mut in_ns = 0_u64;
        let res: Vec<_> = datas
            .into_iter()
            .flat_map(|data| {
                pre_p
                    .process(&mut in_ns, data.as_bytes(), Value::object())
                    .unwrap_or_default()
            })
            .collect();
        assert_eq!(lengths.len(), res.len());
        for (processed, expected_len) in res.iter().zip(lengths) {
            assert_eq!(expected_len, processed.0.len());
        }
    }

    #[test]
    fn textual_length_prefix() {
        let mut pre_p = textual_length_prefixed::TextualLengthPrefixed::default();
        let data = textual_prefix(42);
        let mut in_ns = 0_u64;
        let mut res = pre_p
            .process(&mut in_ns, data.as_bytes(), Value::object())
            .unwrap_or_default();
        assert_eq!(1, res.len());
        let payload = res.pop().unwrap_or_default().0;
        assert_eq!(42, payload.len());
    }

    #[test]
    fn empty_textual_prefix() {
        let data = ("").as_bytes();
        let mut pre_p = textual_length_prefixed::TextualLengthPrefixed::default();
        let mut post_p = post::textual_length_prefixed::TextualLengthPrefixed::default();
        let mut in_ns = 0_u64;
        let res = pre_p
            .process(&mut in_ns, data, Value::object())
            .unwrap_or_default();
        assert_eq!(0, res.len());

        let data_empty = vec![];
        let encoded = post_p
            .process(42, 23, &data_empty)
            .unwrap_or_default()
            .pop()
            .unwrap_or_default();
        assert_eq!("0 ", String::from_utf8_lossy(&encoded));
        let mut res2 = pre_p
            .process(&mut in_ns, &encoded, Value::object())
            .unwrap_or_default();
        assert_eq!(1, res2.len());
        let payload = res2.pop().unwrap_or_default().0;
        assert_eq!(0, payload.len());
    }

    #[test]
    fn length_prefix() -> Result<()> {
        let mut it = 0;

        let pre_p = length_prefixed::LengthPrefixed::default();
        let mut post_p = post::length_prefixed::LengthPrefixed::default();

        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let wire = post_p.process(0, 0, &data)?;
        let (start, end) = wire[0].split_at(7);
        let alias = Alias::new("test", "test");
        let mut pps: Vec<Box<dyn Preprocessor>> = vec![Box::new(pre_p)];
        let recv = preprocess(
            pps.as_mut_slice(),
            &mut it,
            start.to_vec(),
            Value::object(),
            &alias,
        )?;
        assert!(recv.is_empty());
        let recv = preprocess(
            pps.as_mut_slice(),
            &mut it,
            end.to_vec(),
            Value::object(),
            &alias,
        )?;
        assert_eq!(recv[0].0, data);

        // incomplete data
        let processed = preprocess(
            pps.as_mut_slice(),
            &mut it,
            start.to_vec(),
            Value::object(),
            &alias,
        )?;
        assert!(processed.is_empty());
        // not emitted upon finish
        let finished = finish(pps.as_mut_slice(), &alias)?;
        assert!(finished.is_empty());

        Ok(())
    }

    const LOOKUP_TABLE: [&str; 8] = [
        "separate",
        "base64",
        "decompress",
        "remove-empty",
        "gelf-chunking",
        "ingest-ns",
        "length-prefixed",
        "textual-length-prefixed",
    ];

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
    fn test_filter_empty() {
        let mut pre = remove_empty::RemoveEmpty::default();
        assert_eq!(Ok(vec![]), pre.process(&mut 0_u64, &[], Value::object()));
        assert_eq!(Ok(vec![]), pre.finish(None, None));
    }

    #[test]
    fn test_filter_null() {
        let mut pre = remove_empty::RemoveEmpty::default();
        assert_eq!(Ok(vec![]), pre.process(&mut 0_u64, &[], Value::object()));
        assert_eq!(Ok(vec![]), pre.finish(None, None));
    }

    #[test]
    fn test_lines() -> Result<()> {
        let int = "snot\nbadger".as_bytes();
        let enc = "snot\nbadger\n".as_bytes(); // First event ( event per line )
        let out = "snot".as_bytes();

        let mut post = SeparatePost::default();
        let mut pre = separate::Separate::default();

        let mut ingest_ns = 0_u64;
        let egress_ns = 1_u64;

        let r = post.process(ingest_ns, egress_ns, int);
        assert!(r.is_ok(), "Expected Ok(...), Got: {r:?}");
        let ext = &r?[0];
        let ext = ext.as_slice();
        // Assert actual encoded form is as expected
        assert_eq!(enc, ext);

        let r = pre.process(&mut ingest_ns, ext, Value::object());
        let out2 = &r?[0].0;
        let out2 = out2.as_slice();
        // Assert actual decoded form is as expected
        assert_eq!(out, out2);

        // assert empty finish, no leftovers
        assert!(pre.finish(None, None)?.is_empty());
        Ok(())
    }

    #[test]
    fn test_separate_buffered() -> Result<()> {
        let input = "snot\nbadger\nwombat\ncapybara\nquagga".as_bytes();
        let mut pre = separate::Separate::new(b'\n', 1000, true);
        let mut ingest_ns = 0_u64;
        let mut res = pre.process(&mut ingest_ns, input, Value::object())?;
        let splitted = input
            .split(|c| *c == b'\n')
            .map(|v| (v.to_vec(), Value::object()))
            .collect::<Vec<_>>();
        assert_eq!(splitted[..splitted.len() - 1].to_vec(), res);
        let mut finished = pre.finish(None, None)?;
        res.append(&mut finished);
        assert_eq!(splitted, res);
        Ok(())
    }

    macro_rules! assert_separate_no_buffer {
        ($inbound:expr, $outbound1:expr, $outbound2:expr, $case_number:expr, $separator:expr) => {
            let mut ingest_ns = 0_u64;
            let r = separate::Separate::new($separator, 0, false).process(
                &mut ingest_ns,
                $inbound,
                Value::object(),
            );

            let out = &r?;
            // Assert preprocessor output is as expected
            assert!(
                2 == out.len(),
                "Test case : {} => expected output = {}, actual output = {}",
                $case_number,
                "2",
                out.len()
            );
            assert!(
                $outbound1 == out[0].0.as_slice(),
                "Test case : {} => expected output = \"{}\", actual output = \"{}\"",
                $case_number,
                std::str::from_utf8($outbound1).unwrap(),
                std::str::from_utf8(out[0].0.as_slice()).unwrap()
            );
            assert!(
                $outbound2 == out[1].0.as_slice(),
                "Test case : {} => expected output = \"{}\", actual output = \"{}\"",
                $case_number,
                std::str::from_utf8($outbound2).unwrap(),
                std::str::from_utf8(out[1].0.as_slice()).unwrap()
            );
        };
    }

    #[allow(clippy::type_complexity)]
    #[test]
    fn test_separate_no_buffer_no_maxlength() -> Result<()> {
        let test_data: [(&'static [u8], &'static [u8], &'static [u8], &'static str); 4] = [
            (b"snot\nbadger", b"snot", b"badger", "0"),
            (b"snot\n", b"snot", b"", "1"),
            (b"\nsnot", b"", b"snot", "2"),
            (b"\n", b"", b"", "3"),
        ];
        for case in &test_data {
            assert_separate_no_buffer!(case.0, case.1, case.2, case.3, b'\n');
        }

        Ok(())
    }

    #[allow(clippy::type_complexity)]
    #[test]
    fn test_carriage_return_no_buffer_no_maxlength() -> Result<()> {
        let test_data: [(&'static [u8], &'static [u8], &'static [u8], &'static str); 4] = [
            (b"snot\rbadger", b"snot", b"badger", "0"),
            (b"snot\r", b"snot", b"", "1"),
            (b"\rsnot", b"", b"snot", "2"),
            (b"\r", b"", b"", "3"),
        ];
        for case in &test_data {
            assert_separate_no_buffer!(case.0, case.1, case.2, case.3, b'\r');
        }

        Ok(())
    }

    #[test]
    fn test_base64() -> Result<()> {
        let int = "snot badger".as_bytes();
        let enc = "c25vdCBiYWRnZXI=".as_bytes();

        let mut pre = base64::Base64::default();
        let mut post = post::base64::Base64::default();

        // Fake ingest_ns and egress_ns
        let mut ingest_ns = 0_u64;
        let egress_ns = 1_u64;

        let r = post.process(ingest_ns, egress_ns, int);
        let ext = &r?[0];
        let ext = ext.as_slice();
        // Assert actual encoded form is as expected
        assert_eq!(&enc, &ext);

        let r = pre.process(&mut ingest_ns, ext, Value::object());
        let out = &r?[0].0;
        let out = out.as_slice();
        // Assert actual decoded form is as expected
        assert_eq!(&int, &out);

        // assert empty finish, no leftovers
        assert!(pre.finish(None, None)?.is_empty());
        Ok(())
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
        ) -> Result<Vec<(Vec<u8>, Value<'static>)>> {
            Err("chucky".into())
        }
        fn finish(
            &mut self,
            _data: Option<&[u8]>,
            _meta: Option<Value<'static>>,
        ) -> Result<Vec<(Vec<u8>, Value<'static>)>> {
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
        ) -> Result<Vec<(Vec<u8>, Value<'static>)>> {
            Ok(vec![])
        }

        fn finish(
            &mut self,
            _data: Option<&[u8]>,
            _meta: Option<Value<'static>>,
        ) -> Result<Vec<(Vec<u8>, Value<'static>)>> {
            Err("chucky revenge".into())
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
        ) -> Result<Vec<(Vec<u8>, Value<'static>)>> {
            Ok(vec![(b"non".to_vec(), meta)])
        }
        fn finish(
            &mut self,
            _data: Option<&[u8]>,
            meta: Option<Value<'static>>,
        ) -> Result<Vec<(Vec<u8>, Value<'static>)>> {
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
        let alias = crate::connectors::Alias::new(
            crate::system::flow::Alias::new("chucky"),
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
        let alias = crate::connectors::Alias::new(
            crate::system::flow::Alias::new("chucky"),
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
        let alias = crate::connectors::Alias::new(
            crate::system::flow::Alias::new("chucky"),
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
        let alias = crate::connectors::Alias::new(
            crate::system::flow::Alias::new("chucky"),
            "chucky".to_string(),
        );
        let pre = Box::new(BadFinisher {});
        let r = finish(&mut [pre], &alias);
        assert!(r.is_err());
    }

    #[test]
    fn preprocess_finish_tail_fail() {
        let alias = crate::connectors::Alias::new(
            crate::system::flow::Alias::new("chucky"),
            "chucky".to_string(),
        );
        let noop = Box::new(NoOp {});
        let pre = Box::new(BadFinisher {});
        let r = finish(&mut [noop, pre], &alias);
        assert!(r.is_err());
    }

    #[test]
    fn preprocess_finish_multi_ok() {
        let alias = crate::connectors::Alias::new(
            crate::system::flow::Alias::new("xyz"),
            "xyz".to_string(),
        );
        let noop1 = Box::new(NoOp {});
        let noop2 = Box::new(NoOp {});
        let noop3 = Box::new(NoOp {});
        let r = finish(&mut [noop1, noop2, noop3], &alias);
        assert!(r.is_ok());
    }
}
