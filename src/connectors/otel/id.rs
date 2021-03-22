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

use crate::errors::Result;
use rand::Rng;
use simd_json::StaticNode;
use tremor_script::Value;

pub(crate) fn random_span_id_bytes() -> Vec<u8> {
    let mut rng = tremor_common::rand::make_prng();
    let span_id: Vec<u8> = (0..8).map(|_| rng.gen_range(0_u8..=255_u8)).collect();
    span_id
}

pub(crate) fn random_span_id_string() -> String {
    let mut rng = tremor_common::rand::make_prng();
    let span_id: String = (0..8)
        .map(|_| rng.gen_range(0_u8..=255_u8))
        .map(|b| format!("{:02x}", b))
        .collect();
    span_id
}

pub(crate) fn random_span_id_array<'event>() -> Value<'event> {
    Value::Array(
        random_span_id_bytes()
            .iter()
            .map(|b| Value::Static(StaticNode::U64(u64::from(*b))))
            .collect(),
    )
}

pub(crate) fn random_span_id_value<'event>() -> Value<'event> {
    Value::String(random_span_id_string().into())
}

pub(crate) fn random_trace_id_bytes() -> Vec<u8> {
    let mut rng = tremor_common::rand::make_prng();
    let span_id: Vec<u8> = (0..16).map(|_| rng.gen_range(0_u8..=255_u8)).collect();
    span_id
}

pub(crate) fn random_trace_id_string() -> String {
    let mut rng = tremor_common::rand::make_prng();
    let span_id: String = (0..16)
        .map(|_| rng.gen_range(0_u8..=255_u8))
        .map(|b| format!("{:02x}", b))
        .collect();
    span_id
}

pub(crate) fn random_trace_id_value<'event>() -> Value<'event> {
    Value::String(random_trace_id_string().into())
}

fn hex_to_bytes(str_bytes: &str) -> Option<Vec<u8>> {
    if str_bytes.len() % 2 == 0 {
        (0..str_bytes.len())
            .step_by(2)
            .map(|i| {
                str_bytes
                    .get(i..i + 2)
                    .and_then(|sub| u8::from_str_radix(sub, 16).ok())
            })
            .collect()
    } else {
        None
    }
}

pub(crate) fn random_trace_id_array<'event>() -> Value<'event> {
    Value::Array(
        random_trace_id_bytes()
            .iter()
            .map(|b| Value::Static(StaticNode::U64(u64::from(*b))))
            .collect(),
    )
}

pub(crate) fn hex_parent_span_id_to_pb(data: Option<&Value<'_>>) -> Result<Vec<u8>> {
    hex_id_to_pb("parent span", data, 8, true)
}

pub(crate) fn hex_span_id_to_pb(data: Option<&Value<'_>>) -> Result<Vec<u8>> {
    hex_id_to_pb("span", data, 8, false)
}

pub(crate) fn hex_span_id_to_json<'event>(data: &[u8]) -> Value<'event> {
    let hex: String = data.iter().map(|b| format!("{:02x}", b)).collect();
    Value::String(hex.into())
}

pub(crate) fn hex_trace_id_to_pb(data: Option<&Value<'_>>) -> Result<Vec<u8>> {
    hex_id_to_pb("trace", data, 16, false)
}

pub(crate) fn hex_trace_id_to_json<'event>(data: &[u8]) -> Value<'event> {
    let hex: String = data.iter().map(|b| format!("{:02x}", b)).collect();
    Value::String(hex.into())
}

fn hex_id_to_pb(
    kind: &str,
    data: Option<&Value<'_>>,
    len_bytes: usize,
    allow_empty: bool,
) -> Result<Vec<u8>> {
    if let Some(Value::String(json)) = data {
        let pb = json.to_string();
        let pb = hex_to_bytes(&pb);

        if let Some(pb) = pb {
            if (allow_empty && pb.is_empty()) || pb.len() == len_bytes {
                Ok(pb)
            } else {
                Err(format!(
                    "Invalid {} id ( wrong string length 1) - cannot convert to pb",
                    kind
                )
                .into())
            }
        } else {
            Err(format!("Invalid hex encoded string - cannot convert {} to pb", kind).into())
        }
    } else if let Some(Value::Bytes(json)) = data {
        let data = json.to_vec();
        if (allow_empty && data.is_empty()) || data.len() == len_bytes {
            Ok(data)
        } else {
            Err(format!(
                "Invalid {} id ( wrong byte length ) - cannot convert to pb",
                kind
            )
            .into())
        }
    } else if let Some(Value::Array(arr)) = data {
        let mut data = Vec::new();
        for i in arr {
            if let Value::Static(StaticNode::I64(i)) = i {
                if *i > 255 {
                    return Err(format!(
                        "Invalid {} id ( wrong array element ) - values must be 0 <= [..., v={} , ...] <= 255 - cannot convert to pb",
                        kind,
                        *i
                    )
                    .into());
                }
                #[allow(clippy::cast_possible_truncation)]
                #[allow(clippy::cast_sign_loss)]
                data.push(*i as u8)
            } else if let Value::Static(StaticNode::U64(i)) = i {
                if *i > 255 {
                    return Err(format!(
                        "Invalid {} id ( wrong array element ) - values must be 0 <= [..., v={} , ...] <= 255 - cannot convert to pb",
                        kind,
                        *i
                    )
                    .into());
                }
                #[allow(clippy::cast_possible_truncation)]
                #[allow(clippy::cast_sign_loss)]
                data.push(*i as u8)
            }
        }

        if (allow_empty && data.is_empty()) || data.len() == len_bytes {
            Ok(data)
        } else {
            Err(format!(
                "Invalid {} id ( wrong array length ) - cannot convert to pb",
                kind
            )
            .into())
        }
    } else {
        Err(format!("Cannot convert json value to otel pb {} id", kind).into())
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    use proptest::prelude::*;
    use proptest::proptest;

    pub(crate) fn pb_span_id_to_json(pb: &[u8]) -> Value {
        let hex: String = pb.iter().map(|b| format!("{:02x}", b)).collect();
        Value::String(hex.into())
    }

    pub(crate) fn json_span_id_to_pb(json: Option<&Value<'_>>) -> Result<Vec<u8>> {
        hex_span_id_to_pb(json)
    }

    pub(crate) fn pb_trace_id_to_json(pb: &[u8]) -> Value {
        let hex: String = pb.iter().map(|b| format!("{:02x}", b)).collect();
        Value::String(hex.into())
    }

    pub(crate) fn json_trace_id_to_pb(json: Option<&Value<'_>>) -> Result<Vec<u8>> {
        hex_trace_id_to_pb(json)
    }

    #[test]
    fn test_utilities() -> Result<()> {
        let span_bytes = random_span_id_bytes();
        let trace_bytes = random_trace_id_bytes();

        let span_json = pb_span_id_to_json(&span_bytes);
        let trace_json = pb_trace_id_to_json(&trace_bytes);

        let span_pb = json_span_id_to_pb(Some(&span_json))?;
        let trace_pb = json_trace_id_to_pb(Some(&trace_json))?;

        assert_eq!(span_bytes, span_pb);
        assert_eq!(trace_bytes, trace_pb);

        let span_array = random_span_id_array();
        let trace_array = random_trace_id_array();

        let span_pb = json_span_id_to_pb(Some(&span_array))?;
        let trace_pb = json_trace_id_to_pb(Some(&trace_array))?;

        let span_json = pb_span_id_to_json(&span_pb);
        let trace_json = pb_span_id_to_json(&trace_pb);

        let span_pb2 = json_span_id_to_pb(Some(&span_json))?;
        let trace_pb2 = json_trace_id_to_pb(Some(&trace_json))?;

        assert_eq!(span_pb2, span_pb);
        assert_eq!(trace_pb2, trace_pb);

        Ok(())
    }

    #[test]
    fn bad_hex() {
        assert!(hex_to_bytes("snot").is_none());
    }

    proptest! {

        #[test]
        fn prop_id_span(
            arb_hexen in prop::collection::vec("[a-f0-9]{16}", 16..=16)
        ) {
            for expected in arb_hexen {
                let bytes = hex_to_bytes(&expected).unwrap();

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
                let bytes = hex_to_bytes(&expected).unwrap();

                let json = Value::Bytes(bytes.clone().into());
                let pb = hex_trace_id_to_pb(Some(&json))?;
                prop_assert_eq!(bytes.clone(), pb);
            }
        }
    }
}
