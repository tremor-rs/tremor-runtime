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
/// Sorted
#[derive(Clone, Copy, Debug)]
pub struct Sorted {}
impl Sorting for Sorted {
    const SORTED: bool = true;
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
            string_buffer: Vec::with_capacity(1024),
        }
    }
}

impl<S: Sorting> Codec for Json<S> {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        if S::SORTED {
            "sorted-json"
        } else {
            "json"
        }
    }

    #[cfg(not(tarpaulin_include))]
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
            self.string_buffer.reserve(new_len);
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

    #[cfg(not(tarpaulin_include))]
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
        let output = codec.decode(&mut data, 42)?.unwrap();
        assert_eq!(output, expected);

        let mut codec = codec.clone();

        let mut data = br#"{ "snot": "badger" }"#.to_vec();
        let output = codec.decode(&mut data, 42)?.unwrap();
        assert_eq!(output, expected);

        Ok(())
    }

    #[test]
    fn test_json_codec() -> Result<()> {
        let seed = literal!({ "snot": "badger" });

        let mut codec = Json::<Unsorted>::default();

        let mut as_raw = codec.encode(&seed)?;
        let as_json = codec.decode(as_raw.as_mut_slice(), 0);

        let _ = dbg!(as_json);

        Ok(())
    }
    #[test]
    fn test_json_codec_sorted() -> Result<()> {
        let seed = literal!({ "snot": "badger" });

        let mut codec = Json::<Sorted>::default();

        let mut as_raw = codec.encode(&seed)?;
        let as_json = codec.decode(as_raw.as_mut_slice(), 0);

        let _ = dbg!(as_json);

        Ok(())
    }
}
