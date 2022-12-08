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

use std::{cmp::max, marker::PhantomData};

use super::prelude::*;
use tremor_script::utils::sorted_serialize;
use tremor_value::AlignedBuf;

/// Sorting for JSON
pub trait Sorting: Sync + Send + Copy + Clone + 'static {
    const SORTED: bool;
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
}

impl<S: Sorting> Clone for Json<S> {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl<S: Sorting> Default for Json<S> {
    fn default() -> Self {
        Self {
            _phantom: PhantomData::default(),
            input_buffer: AlignedBuf::with_capacity(1024),
            string_buffer: vec![0u8; 1024],
        }
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
    ) -> Result<Option<Value<'input>>> {
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
        .map(Some)
        .map_err(Error::from)
    }
    fn encode(&self, data: &Value) -> Result<Vec<u8>> {
        if S::SORTED {
            Ok(sorted_serialize(data)?.into_bytes())
        } else {
            let mut v = Vec::with_capacity(1024);
            self.encode_into(data, &mut v)?;
            Ok(v)
        }
    }
    fn encode_into(&self, data: &Value, dst: &mut Vec<u8>) -> Result<()> {
        data.write(dst)?;
        Ok(())
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
        let output = codec.decode(&mut data, 42)?.unwrap_or_default();
        assert_eq!(output, expected);

        let mut codec = codec.clone();

        let mut data = br#"{ "snot": "badger" }"#.to_vec();
        let output = codec.decode(&mut data, 42)?.unwrap_or_default();
        assert_eq!(output, expected);

        Ok(())
    }

    #[test]
    fn test_json_codec() -> Result<()> {
        let seed = literal!({ "snot": "badger" });

        let mut codec = Json::<Unsorted>::default();

        let mut as_raw = codec.encode(&seed)?;
        assert!(codec.decode(as_raw.as_mut_slice(), 0)?.is_some());

        Ok(())
    }
    #[test]
    fn test_json_codec_sorted() -> Result<()> {
        let seed = literal!({ "snot": "badger" });

        let mut codec = Json::<crate::codec::json_sorted::Sorted>::default();

        let mut as_raw = codec.encode(&seed)?;
        assert!(codec.decode(as_raw.as_mut_slice(), 0)?.is_some());

        Ok(())
    }

    #[test]
    fn encode_into() -> Result<()> {
        let value = literal!({"snot": ["badger", null, false, 1.5, 42]});
        let codec: Box<dyn Codec> = Box::new(Json::<Unsorted>::default());
        println!("{codec} {codec:?}"); // for coverage

        let mut buf = vec![];
        codec.encode_into(&value, &mut buf)?;
        assert_eq!(
            "{\"snot\":[\"badger\",null,false,1.5,42]}".to_string(),
            String::from_utf8_lossy(&buf)
        );
        Ok(())
    }

    #[test]
    fn duplicate_keys_unsorted() -> Result<()> {
        let mut input = r#"{"key": 1, "key":2}"#.as_bytes().to_vec();
        let mut codec = Json::<Unsorted>::default();
        let res = codec.decode(input.as_mut_slice(), 0)?;
        assert_eq!(Some(literal!({"key": 2})), res); // duplicate keys are deduplicated with last-key-wins strategy
        let value = res.expect("No value");
        let serialized = codec.encode(&value)?;
        assert_eq!(r#"{"key":2}"#.as_bytes(), serialized.as_slice());

        Ok(())
    }

    #[test]
    fn duplicate_keys_sorted() -> Result<()> {
        let mut input = r#"{"key": 1, "key":2}"#.as_bytes().to_vec();
        let mut codec = Json::<crate::codec::json_sorted::Sorted>::default();
        let res = codec.decode(input.as_mut_slice(), 0)?;
        assert_eq!(Some(literal!({"key": 2})), res); // duplicate keys are deduplicated with last-key-wins strategy
        let value = res.expect("No value");
        let serialized = codec.encode(&value)?;
        assert_eq!(r#"{"key":2}"#.as_bytes(), serialized.as_slice());

        Ok(())
    }

    #[test]
    fn duplicate_keys_into_static() -> Result<()> {
        let mut input = r#"{"key": 1, "key":2}"#.as_bytes().to_vec();
        let mut codec = Json::<Unsorted>::default();
        let res = codec.decode(input.as_mut_slice(), 0)?;
        assert_eq!(Some(literal!({"key": 2})), res); // duplicate keys are deduplicated with last-key-wins strategy
        let value = res.expect("No value");
        let serialized = codec.encode(&value)?;
        assert_eq!(r#"{"key":2}"#.as_bytes(), serialized.as_slice());

        Ok(())
    }
}
