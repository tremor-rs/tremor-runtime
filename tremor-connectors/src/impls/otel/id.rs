// Copyright 2020-2022, The Tremor Team
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

#![allow(dead_code)]

use super::common::{self, Error};
use ::rand::Rng;
use std::fmt::Write;
use tremor_common::rand;
use tremor_value::prelude::*;

pub(crate) fn random_span_id_bytes(ingest_ns_seed: u64) -> Vec<u8> {
    let mut rng = tremor_common::rand::make_prng(ingest_ns_seed);
    let span_id: Vec<u8> = (0..8).map(|_| rng.gen_range(0_u8..=255_u8)).collect();
    span_id
}

pub(crate) fn random_span_id_string(ingest_ns_seed: u64) -> String {
    rand::octet_string(8, ingest_ns_seed)
}

pub(crate) fn random_span_id_array(ingest_ns_seed: u64) -> Value<'static> {
    random_span_id_bytes(ingest_ns_seed)
        .into_iter()
        .map(Value::from)
        .collect()
}

pub(crate) fn random_span_id_value(ingest_ns_seed: u64) -> Value<'static> {
    Value::from(random_span_id_string(ingest_ns_seed))
}

pub(crate) fn random_trace_id_bytes(ingest_ns_seed: u64) -> Vec<u8> {
    let mut rng = tremor_common::rand::make_prng(ingest_ns_seed);
    let span_id: Vec<u8> = (0..16).map(|_| rng.gen_range(0_u8..=255_u8)).collect();
    span_id
}

pub(crate) fn random_trace_id_string(ingest_ns_seed: u64) -> String {
    tremor_common::rand::octet_string(16, ingest_ns_seed)
}

pub(crate) fn random_trace_id_value(ingest_ns_seed: u64) -> Value<'static> {
    Value::from(random_trace_id_string(ingest_ns_seed))
}

pub(crate) fn random_trace_id_array(ingest_ns_seed: u64) -> Value<'static> {
    random_trace_id_bytes(ingest_ns_seed)
        .into_iter()
        .map(Value::from)
        .collect()
}

pub(crate) fn hex_parent_span_id_to_pb(data: Option<&Value<'_>>) -> Result<Vec<u8>, common::Error> {
    hex_id_to_pb("parent span", data, 8, true)
}

pub(crate) fn hex_span_id_to_pb(data: Option<&Value<'_>>) -> Result<Vec<u8>, common::Error> {
    hex_id_to_pb("span", data, 8, false)
}

pub(crate) fn hex_id_to_json(data: &[u8]) -> Value<'static> {
    Value::from(data.iter().fold(String::new(), |mut o, b| {
        // ALLOW: if we can't allocate it's worse, we'd have the same problem with format
        let _ = write!(o, "{b:02x}");
        o
    }))
}

pub(crate) fn hex_trace_id_to_pb(data: Option<&Value<'_>>) -> Result<Vec<u8>, common::Error> {
    hex_id_to_pb("trace", data, 16, false)
}

fn hex_id_to_pb(
    kind: &str,
    data: Option<&Value<'_>>,
    len_bytes: usize,
    allow_empty: bool,
) -> Result<Vec<u8>, common::Error> {
    let data = if let Some(s) = data.as_str() {
        hex::decode(s)?
    } else if let Some(json) = data.and_then(Value::as_bytes) {
        json.to_vec()
    } else if let Some(arr) = data.as_array() {
        arr.iter()
            .map(Value::as_u8)
            .collect::<Option<Vec<u8>>>()
            .ok_or_else(|| Error::InvalidArrayContent(kind.to_string()))?
    } else {
        return Err(Error::FaildToConvert(kind.to_string()));
    };
    if (allow_empty && data.is_empty()) || data.len() == len_bytes {
        Ok(data)
    } else {
        Err(Error::InvalidLength(kind.to_string()))
    }
}

#[cfg(test)]
pub mod test {
    #![allow(clippy::ignored_unit_patterns)]
    use super::*;

    use proptest::prelude::*;
    use proptest::proptest;

    #[test]
    fn test_utilities() -> anyhow::Result<()> {
        let nanos = tremor_common::time::nanotime();
        let span_bytes = random_span_id_bytes(nanos);
        let trace_bytes = random_trace_id_bytes(nanos);

        let span_json = hex_id_to_json(&span_bytes);
        let trace_json = hex_id_to_json(&trace_bytes);

        let span_pb = hex_span_id_to_pb(Some(&span_json))?;
        let trace_pb = hex_trace_id_to_pb(Some(&trace_json))?;

        assert_eq!(span_bytes, span_pb);
        assert_eq!(trace_bytes, trace_pb);

        let span_array = random_span_id_array(nanos);
        let trace_array = random_trace_id_array(nanos);

        let span_pb = hex_span_id_to_pb(Some(&span_array))?;
        let trace_pb = hex_trace_id_to_pb(Some(&trace_array))?;

        let span_json = hex_id_to_json(&span_pb);
        let trace_json = hex_id_to_json(&trace_pb);

        let span_pb2 = hex_span_id_to_pb(Some(&span_json))?;
        let trace_pb2 = hex_trace_id_to_pb(Some(&trace_json))?;

        assert_eq!(span_pb2, span_pb);
        assert_eq!(trace_pb2, trace_pb);

        Ok(())
    }

    proptest! {

        #[test]
        fn prop_id_span(
            arb_hexen in prop::collection::vec("[a-f0-9]{16}", 16..=16)
        ) {
            for expected in arb_hexen {
                let bytes = hex::decode(expected).unwrap_or_default();

                let json = Value::Bytes(bytes.clone().into());
                let pb = hex_span_id_to_pb(Some(&json))?;
                prop_assert_eq!(bytes.clone(), pb);
            }
        }

        #[test]
        fn prop_id_trace(
            arb_hexen in prop::collection::vec("[a-f0-9]{32}", 32..=32)
        ) {
            for expected in arb_hexen {
                let bytes = hex::decode(expected).unwrap_or_default();

                let json = Value::Bytes(bytes.clone().into());
                let pb = hex_trace_id_to_pb(Some(&json))?;
                prop_assert_eq!(bytes.clone(), pb);
            }
        }
    }
}
