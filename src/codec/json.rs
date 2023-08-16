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

//! The `json` codec supports the Javascript Object Notation format.
//!
//! Specification: [JSON](https://json.org).
//!
//! Deserialization supports minified and fat JSON. Duplicate keys are not preserved, consecutive duplicate keys overwrite previous ones.
//!
//! Serialization supports minified JSON only.
//!
//! The codec can be configured with a mode, either `sorted` or `unsorted`. The default is `unsorted` as it is singnificantly faster, `sorted` json is only needed in testing situations where the key order in maps matters for compairson.

use super::prelude::*;
use std::{cmp::max, marker::PhantomData};
use tremor_script::utils::sorted_serialize;
use tremor_value::AlignedBuf;

/// Sorting for JSON
pub trait Sorting: Sync + Send + Copy + Clone + 'static {
    const SORTED: bool;
}

/// Sorted
#[derive(Clone, Copy, Debug)]
pub struct Sorted {}
impl Sorting for Sorted {
    const SORTED: bool = true;
}

/// Unsorted
#[derive(Clone, Copy, Debug)]
pub struct Unsorted {}
impl Sorting for Unsorted {
    const SORTED: bool = false;
}

pub struct Json<S: Sorting> {
    _phantom: PhantomData<S>,
    input_buffer: AlignedBuf,
    string_buffer: Vec<u8>,
    data_buf: Vec<u8>,
}

impl<S: Sorting> Clone for Json<S> {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl<S: Sorting> Default for Json<S> {
    fn default() -> Self {
        Self {
            _phantom: PhantomData,
            input_buffer: AlignedBuf::with_capacity(1024),
            string_buffer: vec![0u8; 1024],
            data_buf: Vec::new(),
        }
    }
}

pub(crate) fn from_config(config: Option<&Value>) -> Result<Box<dyn Codec>> {
    match config.get_str("mode") {
        Some("sorted") => Ok(Box::<Json<Sorted>>::default()),
        None | Some("unsorted") => Ok(Box::<Json<Unsorted>>::default()),
        Some(mode) => Err(format!(
            "Unknown json codec mode: {mode}, can only be one of `sorted` or `unsorted`",
        )
        .into()),
    }
}

impl<S: Sorting> Codec for Json<S> {
    fn name(&self) -> &str {
        if S::SORTED {
            "sorted-json"
        } else {
            "json"
        }
    }

    fn mime_types(&self) -> Vec<&'static str> {
        vec!["application/json"]
        // TODO: application/json-seq for one json doc per line?
    }

    fn decode<'input>(
        &mut self,
        data: &'input mut [u8],
        _ingest_ns: u64,
        meta: Value<'input>,
    ) -> Result<Option<(Value<'input>, Value<'input>)>> {
        // The input buffer will be automatically grown if required
        if self.string_buffer.capacity() < data.len() {
            let new_len = max(self.string_buffer.capacity(), data.len()) * 2;
            self.string_buffer.resize(new_len, 0);
        }
        tremor_value::parse_to_value_with_buffers(
            data,
            &mut self.input_buffer,
            &mut self.string_buffer,
        )
        .map(|v| Some((v, meta)))
        .map_err(Error::from)
    }
    fn encode(&mut self, data: &Value) -> Result<Vec<u8>> {
        if S::SORTED {
            Ok(sorted_serialize(data)?.into_bytes())
        } else {
            data.write(&mut self.data_buf)?;
            let v = self.data_buf.clone();
            self.data_buf.clear();
            Ok(v)
        }
    }

    fn boxed_clone(&self) -> Box<dyn Codec> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tremor_value::literal;

    #[test]
    fn decode() -> Result<()> {
        let mut codec: Json<Unsorted> = Json {
            input_buffer: AlignedBuf::with_capacity(0),
            string_buffer: Vec::new(),
            ..Default::default()
        };
        let expected = literal!({ "snot": "badger" });

        let mut data = br#"{ "snot": "badger" }"#.to_vec();
        let output = codec
            .decode(&mut data, 42, Value::object())?
            .expect("no data");
        assert_eq!(output.0, expected);

        let mut codec = codec.clone();

        let mut data = br#"{ "snot": "badger" }"#.to_vec();
        let output = codec
            .decode(&mut data, 42, Value::object())?
            .expect("no data");
        assert_eq!(output.0, expected);

        Ok(())
    }

    #[test]
    fn test_json_codec() -> Result<()> {
        let seed = literal!({ "snot": "badger" });

        let mut codec = Json::<Unsorted>::default();

        let mut as_raw = codec.encode(&seed)?;
        assert!(codec
            .decode(as_raw.as_mut_slice(), 0, Value::object())?
            .is_some());

        Ok(())
    }
    #[test]
    fn test_json_codec_sorted() -> Result<()> {
        let seed = literal!({ "snot": "badger" });

        let mut codec = Json::<Sorted>::default();

        let mut as_raw = codec.encode(&seed)?;
        assert!(codec
            .decode(as_raw.as_mut_slice(), 0, Value::object())?
            .is_some());

        Ok(())
    }

    #[test]
    fn duplicate_keys_unsorted() -> Result<()> {
        let mut input = r#"{"key": 1, "key":2}"#.as_bytes().to_vec();
        let mut codec = Json::<Unsorted>::default();
        let res = codec
            .decode(input.as_mut_slice(), 0, Value::object())?
            .expect("no data");
        assert_eq!(literal!({"key": 2}), res.0); // duplicate keys are deduplicated with last-key-wins strategy

        let serialized = codec.encode(&res.0)?;
        assert_eq!(r#"{"key":2}"#.as_bytes(), serialized.as_slice());

        Ok(())
    }

    #[test]
    fn duplicate_keys_sorted() -> Result<()> {
        let mut input = r#"{"key": 1, "key":2}"#.as_bytes().to_vec();
        let mut codec = Json::<Sorted>::default();
        let res = codec
            .decode(input.as_mut_slice(), 0, Value::object())?
            .expect("no data");
        assert_eq!(literal!({"key": 2}), res.0); // duplicate keys are deduplicated with last-key-wins strategy
        let serialized = codec.encode(&res.0)?;
        assert_eq!(r#"{"key":2}"#.as_bytes(), serialized.as_slice());

        Ok(())
    }

    #[test]
    fn duplicate_keys_into_static() -> Result<()> {
        let mut input = r#"{"key": 1, "key":2}"#.as_bytes().to_vec();
        let mut codec = Json::<Unsorted>::default();
        let res = codec
            .decode(input.as_mut_slice(), 0, Value::object())?
            .expect("no data");
        assert_eq!(literal!({"key": 2}), res.0); // duplicate keys are deduplicated with last-key-wins strategy
        let serialized = codec.encode(&res.0)?;
        assert_eq!(r#"{"key":2}"#.as_bytes(), serialized.as_slice());

        Ok(())
    }
}
