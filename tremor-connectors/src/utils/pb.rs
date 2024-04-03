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

use crate::prelude::*;
use simd_json::StaticNode;
use std::collections::BTreeMap;
use tremor_common::base64::{Engine, BASE64};
use tremor_otelapis::opentelemetry::proto::metrics::v1;
use value_trait::TryTypeError;

pub(crate) fn maybe_string_to_pb(data: Option<&Value<'_>>) -> Result<String, TryTypeError> {
    if data.is_none() {
        Ok(String::new())
    } else {
        data.try_as_str().map(ToString::to_string)
    }
}

pub(crate) trait FromValue: Sized {
    fn maybe_from_value(data: Option<&Value<'_>>) -> Result<Option<Self>, TryTypeError> {
        data.map(|d| Self::from_value(d)).transpose()
    }
    fn from_value(data: &Value<'_>) -> Result<Self, TryTypeError>;
}

impl FromValue for v1::exemplar::Value {
    fn from_value(data: &Value<'_>) -> Result<Self, TryTypeError> {
        Ok(v1::exemplar::Value::AsDouble(data.try_as_f64()?))
    }
}

impl FromValue for v1::number_data_point::Value {
    fn from_value(data: &Value<'_>) -> Result<Self, TryTypeError> {
        Ok(v1::number_data_point::Value::AsDouble(data.try_as_f64()?))
    }
}

pub(crate) fn maybe_from_value<T: FromValue>(
    data: Option<&Value<'_>>,
) -> Result<Option<T>, TryTypeError> {
    T::maybe_from_value(data)
}

pub(crate) fn f64_repeated_to_pb(json: Option<&Value<'_>>) -> Result<Vec<f64>, TryTypeError> {
    json.try_as_array()?
        .iter()
        .map(Some)
        .map(|v| v.try_as_f64())
        .collect()
}

pub(crate) fn u64_repeated_to_pb(json: Option<&Value<'_>>) -> Result<Vec<u64>, TryTypeError> {
    json.try_as_array()?
        .iter()
        .map(Some)
        .map(|v| v.try_as_u64())
        .collect()
}

pub(crate) fn value_to_prost_value(json: &Value) -> Result<prost_types::Value, TryTypeError> {
    use prost_types::value::Kind;
    Ok(match json {
        Value::Static(StaticNode::Null) => prost_types::Value {
            kind: Some(Kind::NullValue(0)),
        },
        Value::Static(StaticNode::Bool(v)) => prost_types::Value {
            kind: Some(Kind::BoolValue(*v)),
        },
        #[allow(clippy::cast_precision_loss)]
        Value::Static(StaticNode::I64(v)) => prost_types::Value {
            kind: Some(Kind::NumberValue(*v as f64)),
        },
        #[allow(clippy::cast_precision_loss)]
        Value::Static(StaticNode::U64(v)) => prost_types::Value {
            kind: Some(Kind::NumberValue(*v as f64)),
        },
        Value::Static(StaticNode::F64(v)) => prost_types::Value {
            kind: Some(Kind::NumberValue(*v)),
        },
        Value::String(v) => prost_types::Value {
            kind: Some(Kind::StringValue(v.to_string())),
        },
        Value::Array(v) => {
            let mut arr: Vec<prost_types::Value> = vec![];
            for val in v {
                arr.push(value_to_prost_value(val)?);
            }
            prost_types::Value {
                kind: Some(Kind::ListValue(prost_types::ListValue { values: arr })),
            }
        }
        Value::Object(v) => {
            let mut fields = BTreeMap::new();
            for (key, val) in v.iter() {
                fields.insert(key.to_string(), value_to_prost_value(val)?);
            }
            prost_types::Value {
                kind: Some(Kind::StructValue(prost_types::Struct { fields })),
            }
        }
        Value::Bytes(v) => {
            let encoded = BASE64.encode(v);
            prost_types::Value {
                kind: Some(Kind::StringValue(encoded)),
            }
        }
    })
}

pub(crate) fn value_to_prost_struct(json: &Value<'_>) -> Result<prost_types::Struct, TryTypeError> {
    use prost_types::value::Kind;

    if json.is_object() {
        if let prost_types::Value {
            kind: Some(Kind::StructValue(s)),
        } = value_to_prost_value(json)?
        {
            return Ok(s);
        }
    }
    Err(TryTypeError {
        expected: ValueType::Object,
        got: json.value_type(),
    })
}

#[cfg(test)]
mod test {
    #![allow(clippy::ignored_unit_patterns)]
    use super::*;
    use proptest::proptest;
    use proptest::{bits::u64, prelude::*};
    use std::f64;
    use tremor_value::literal;

    // NOTE This is incomplete with respect to possible mappings of json values
    // to basic builtin protocol buffer types, but sufficient for the needs of
    // the OpenTelemetry bindings.
    //
    // These utility functions do not offer any coercion or casting and expect
    // valid and appropriate/relevant json values for conversion to respective
    // protocol buffer values.
    //
    // If this disposition changes - the tests below and this comment should also
    // change. Warning to our future selves!

    prop_compose! {
        fn fveci()(vec in prop::collection::vec(prop::num::f64::POSITIVE | prop::num::f64::NEGATIVE, 1..3))
                        (index in 0..vec.len(), vec in Just(vec))
                        -> (Vec<f64>, usize) {
           (vec, index)
       }
    }

    prop_compose! {
        fn uveci()(vec in prop::collection::vec(prop::num::u64::ANY, 1..3))
                        (index in 0..vec.len(), vec in Just(vec))
                        -> (Vec<u64>, usize) {
           (vec, index)
       }
    }

    proptest! {
        #[test]
        fn prop_string(arb_string in ".*") {
            let json = Value::from(arb_string.clone());
            let pb = maybe_string_to_pb(Some(&json))?;
            prop_assert_eq!(&arb_string, &pb);
            prop_assert_eq!(pb.len(), arb_string.len());
        }




        #[test]
        fn prop_pb_f64_repeated((vec, _index) in fveci()) {
            let json: Value = literal!(vec.clone());
            let pb = f64_repeated_to_pb(Some(&json))?;
            prop_assert_eq!(&vec, &pb);
            prop_assert_eq!(pb.len(), vec.len());
        }

        #[test]
        fn prop_pb_u64_repeated((vec, _index) in uveci()) {
            let json: Value = literal!(vec.clone());
            let pb = u64_repeated_to_pb(Some(&json))?;
            prop_assert_eq!(&vec, &pb);
            prop_assert_eq!(pb.len(), vec.len());
        }
    }

    #[test]
    fn bad_string() {
        // NOTE no coercion, no casting
        assert!(maybe_string_to_pb(Some(&Value::from(false))).is_err());

        // NOTE We allow None for string and map to pb default of "" ( empty string )
        assert_eq!(maybe_string_to_pb(None).ok(), Some(String::new()));
    }

    #[test]
    fn bad_repeated_numerics() {
        assert!(f64_repeated_to_pb(Some(&Value::from(vec![true]))).is_err());
        assert!(u64_repeated_to_pb(Some(&Value::from(vec![true]))).is_err());
    }

    #[test]
    fn bad_json_inputs() {
        // NOTE All of these functions are designed to process the output of
        // tremor-value/simd-json `object.get("field-name")` or other *valid*
        // calls that return `Option<&Value>`.
        //
        // As such attempting to convert a non-existent value is an
        // error by construction
        assert!(f64_repeated_to_pb(None).is_err());
        assert!(u64_repeated_to_pb(None).is_err());
    }

    #[test]
    fn prost_value_mappings() -> anyhow::Result<()> {
        use prost_types::value::Kind;
        let v = value_to_prost_value(&literal!(null))?;
        assert_eq!(Some(Kind::NullValue(0)), v.kind);

        let v = value_to_prost_value(&literal!(true))?;
        assert_eq!(Some(Kind::BoolValue(true)), v.kind);

        let v = value_to_prost_value(&literal!(1i64))?;
        assert_eq!(Some(Kind::NumberValue(1f64)), v.kind);

        let v = value_to_prost_value(&literal!(1u64))?;
        assert_eq!(Some(Kind::NumberValue(1f64)), v.kind);

        let v = value_to_prost_value(&literal!(1f64))?;
        assert_eq!(Some(Kind::NumberValue(1f64)), v.kind);

        let v = value_to_prost_value(&literal!("snot"))?;
        assert_eq!(Some(Kind::StringValue("snot".to_string())), v.kind);

        let v = literal!([1u64, 2u64, 3u64]);
        let v = value_to_prost_value(&v)?;
        assert_eq!(
            prost_types::Value {
                kind: Some(Kind::ListValue(prost_types::ListValue {
                    values: vec![
                        value_to_prost_value(&literal!(1u64))?,
                        value_to_prost_value(&literal!(2u64))?,
                        value_to_prost_value(&literal!(3u64))?,
                    ]
                }))
            },
            v
        );

        let v = literal!({ "snot": "badger"});
        let v = value_to_prost_value(&v)?;
        let mut fields = BTreeMap::new();
        fields.insert(
            "snot".to_string(),
            value_to_prost_value(&literal!("badger"))?,
        );
        assert_eq!(
            prost_types::Value {
                kind: Some(Kind::StructValue(prost_types::Struct { fields }))
            },
            v
        );

        let v: beef::Cow<[u8]> = beef::Cow::owned("snot".as_bytes().to_vec());
        let v = Value::Bytes(v);
        let v = value_to_prost_value(&v)?;
        assert_eq!(Some(Kind::StringValue("c25vdA==".to_string())), v.kind);

        Ok(())
    }

    #[test]
    fn prost_struct_mapping() {
        let v = literal!({ "snot": "badger"});
        let v = value_to_prost_struct(&v);
        assert!(v.is_ok());

        let v = literal!(null);
        let v = value_to_prost_struct(&v);
        assert!(v.is_err());
    }
}
