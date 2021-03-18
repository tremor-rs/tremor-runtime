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
use simd_json::StaticNode;
use tremor_value::Value;

pub(crate) fn maybe_string_to_pb(data: Option<&Value<'_>>) -> Result<String> {
    if let Some(Value::String(s)) = data {
        Ok(s.to_string())
    } else {
        Err("Expected a string to convert from json to pb".into())
    }
}

pub(crate) fn maybe_int_to_pbu64(data: Option<&Value<'_>>) -> Result<u64> {
    if let Some(&Value::Static(StaticNode::U64(i))) = data {
        Ok(i)
    } else if let Some(&Value::Static(StaticNode::I64(i))) = data {
        #[allow(clippy::clippy::cast_sign_loss)]
        Ok(i as u64)
    } else {
        Err("Expected an json u64 to convert to pb u64".into())
    }
}

pub(crate) fn maybe_int_to_pbi32(data: Option<&Value<'_>>) -> Result<i32> {
    if let Some(&Value::Static(StaticNode::I64(i))) = data {
        #[allow(clippy::clippy::cast_sign_loss)]
        #[allow(clippy::cast_possible_truncation)]
        Ok(i as i32)
    } else {
        Err("Expected an json i64 to convert to pb i32".into())
    }
}

pub(crate) fn maybe_int_to_pbi64(data: Option<&Value<'_>>) -> Result<i64> {
    if let Some(&Value::Static(StaticNode::I64(i))) = data {
        Ok(i)
    } else {
        Err("Expected an json i64 to convert to pb i64".into())
    }
}

pub(crate) fn maybe_int_to_pbu32(data: Option<&Value<'_>>) -> Result<u32> {
    if let Some(&Value::Static(StaticNode::U64(i))) = data {
        // json values originating from non-otel ( non otel sources )
        // may reasonably be u64
        if i > u64::from(u32::MAX) {
            return Err("Truncation error converting int value to pb.u32".into());
        }
        #[allow(clippy::clippy::cast_sign_loss)]
        #[allow(clippy::cast_possible_truncation)]
        Ok(i as u32)
    } else if let Some(&Value::Static(StaticNode::I64(i))) = data {
        // NOTE: automatic conversion to json via simd-json:json! macro
        // will default to i64 type which needs strict checking
        // for correct mapping to pb!
        if i > i64::from(u32::MAX) {
            return Err("Truncation error converting int value to pb.u32".into());
        }
        if i < 0 {
            return Err("Sign loss of negative signed int value to pb.u32".into());
        }
        #[allow(clippy::clippy::cast_sign_loss)]
        #[allow(clippy::cast_possible_truncation)]
        Ok(i as u32)
    } else {
        Err("Expected a json value convertible to pb u32 without truncation or sign loss".into())
    }
}

pub(crate) fn maybe_double_to_pb(data: Option<&Value<'_>>) -> Result<f64> {
    if let Some(&Value::Static(StaticNode::F64(i))) = data {
        Ok(i)
    } else {
        Err("Expected a json f64 to convert to pb f64".into())
    }
}

pub(crate) fn maybe_bool_to_pb(data: Option<&Value<'_>>) -> Result<bool> {
    if let Some(&Value::Static(StaticNode::Bool(i))) = data {
        Ok(i)
    } else {
        Err("Expected a json bool to convert to pb bool".into())
    }
}

pub(crate) fn f64_repeated_to_pb(json: Option<&Value<'_>>) -> Result<Vec<f64>> {
    if let Some(Value::Array(json)) = json {
        let mut arr = Vec::new();
        for data in json {
            let value = maybe_double_to_pb(Some(data))?;
            arr.push(value);
        }
        return Ok(arr);
    }

    Err("Unable to map json value to repeated f64 pb".into())
}

pub(crate) fn u64_repeated_to_pb(json: Option<&Value<'_>>) -> Result<Vec<u64>> {
    if let Some(Value::Array(json)) = json {
        let mut arr = Vec::new();
        for data in json {
            let value = maybe_int_to_pbu64(Some(data))?;
            arr.push(value);
        }
        return Ok(arr);
    }

    Err("Unable to map json value to repeated u64 pb".into())
}

#[cfg(test)]
mod test {
    use std::{f64, i32};

    use super::*;

    use proptest::proptest;
    use proptest::{bits::u64, prelude::*};
    use simd_json::json;

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

    #[test]
    fn boolean() -> Result<()> {
        assert!(maybe_bool_to_pb(Some(&Value::Static(StaticNode::Bool(
            true
        ))))?);
        assert!(!maybe_bool_to_pb(Some(&Value::Static(StaticNode::Bool(
            false
        ))))?);

        Ok(())
    }

    prop_compose! {
        fn fveci()(vec in prop::collection::vec(f64::MIN..f64::MAX, 1..3))
                        (index in 0..vec.len(), vec in Just(vec))
                        -> (Vec<f64>, usize) {
           (vec, index)
       }
    }

    prop_compose! {
        fn uveci()(vec in prop::collection::vec(u64::MIN..u64::MAX, 1..3))
                        (index in 0..vec.len(), vec in Just(vec))
                        -> (Vec<u64>, usize) {
           (vec, index)
       }
    }

    proptest! {
        #[test]
        fn prop_string(
            arb_strings in prop::collection::vec(".*", 0..1000),
        ) {
            for expected in arb_strings {
                let json = Value::String(expected.clone().into());
                let pb = maybe_string_to_pb(Some(&json))?;
                prop_assert_eq!(&expected, &pb);
                prop_assert_eq!(pb.len(), expected.len());
            }
        }

        #[test]
        fn prop_pb_u64(
            arb_ints in prop::collection::vec(u64::MIN..u64::MAX, 0..1000),
        ) {
            for expected in arb_ints {
                let json = Value::Static(StaticNode::U64(expected));
                let pb = maybe_int_to_pbu64(Some(&json))?;
                // TODO error on i64?
                prop_assert_eq!(expected, pb);
            }
        }

        #[test]
        fn prop_pb_i64(
            arb_ints in prop::collection::vec(i64::MIN..i64::MAX, 0..1000),
        ) {
            for expected in arb_ints {
                let json = Value::Static(StaticNode::I64(expected));
                let pb = maybe_int_to_pbi64(Some(&json))?;
                // TODO error on i64?
                prop_assert_eq!(expected, pb);
            }
        }

        #[test]
        fn prop_pb_u32(
            arb_ints in prop::collection::vec(u32::MIN..u32::MAX, 0..1000),
        ) {
            for expected in arb_ints {
                let json = Value::Static(StaticNode::U64(expected as u64));
                let pb = maybe_int_to_pbu32(Some(&json))?;
                prop_assert_eq!(expected, pb);
            }
        }

        #[test]
        fn prop_pb_i32(
            arb_ints in prop::collection::vec(i32::MIN..i32::MAX, 0..1000),
        ) {
            for expected in arb_ints {
                let json = Value::Static(StaticNode::I64(expected as i64));
                let pb = maybe_int_to_pbi32(Some(&json))?;
                prop_assert_eq!(expected, pb);
            }
        }

        #[test]
        fn prop_pb_f64(
            arb_ints in prop::collection::vec(f64::MIN..f64::MAX, 0..1000),
        ) {
            for expected in arb_ints {
                let json = Value::Static(StaticNode::F64(expected as f64));
                let pb = maybe_double_to_pb(Some(&json))?;
                prop_assert_eq!(expected, pb);
            }
        }

        #[test]
        fn prop_pb_f64_repeated((vec, _index) in fveci()) {
                let json: Value = json!(vec).into();
                let pb = f64_repeated_to_pb(Some(&json))?;
                prop_assert_eq!(&vec, &pb);
                prop_assert_eq!(pb.len(), vec.len());
        }

        #[test]
        fn prop_pb_u64_repeated((vec, _index) in uveci()) {
                let json: Value = json!(vec).into();
                let pb = u64_repeated_to_pb(Some(&json))?;
                prop_assert_eq!(&vec, &pb);
                prop_assert_eq!(pb.len(), vec.len());
        }
    }

    #[test]
    fn bad_boolean() {
        assert!(maybe_bool_to_pb(Some(&Value::String("snot".into()))).is_err());
    }

    #[test]
    fn bad_string() {
        // NOTE no coercion, no casting
        assert!(maybe_string_to_pb(Some(&Value::Static(StaticNode::Bool(false)))).is_err());
    }

    #[test]
    fn bad_int_numerics() {
        assert!(maybe_int_to_pbu64(Some(&Value::Static(StaticNode::Bool(false)))).is_err());
        assert!(maybe_int_to_pbi64(Some(&Value::Static(StaticNode::Bool(false)))).is_err());
        assert!(maybe_int_to_pbu32(Some(&Value::Static(StaticNode::Bool(false)))).is_err());
        assert!(maybe_int_to_pbi32(Some(&Value::Static(StaticNode::Bool(false)))).is_err());
    }

    #[test]
    fn bad_double_numerics() {
        assert!(maybe_double_to_pb(Some(&Value::Static(StaticNode::Bool(false)))).is_err());
    }

    #[test]
    fn bad_repeated_numerics() {
        assert!(
            f64_repeated_to_pb(Some(&Value::Array(vec![Value::Static(StaticNode::Bool(
                true
            ))])))
            .is_err()
        );
        assert!(
            u64_repeated_to_pb(Some(&Value::Array(vec![Value::Static(StaticNode::Bool(
                true
            ))])))
            .is_err()
        );
    }

    #[test]
    fn bad_json_inputs() {
        // NOTE All of these functions are designed to process the output of
        // tremor-value/simd-json `object.get("field-name")` or other *valid*
        // calls that return `Option<&Value>`.
        //
        // As such attempting to convert a non-existent value is an
        // error by construction
        assert!(maybe_bool_to_pb(None).is_err());
        assert!(maybe_string_to_pb(None).is_err());
        assert!(maybe_int_to_pbu64(None).is_err());
        assert!(maybe_int_to_pbi64(None).is_err());
        assert!(maybe_int_to_pbu32(None).is_err());
        assert!(maybe_int_to_pbi32(None).is_err());
        assert!(f64_repeated_to_pb(None).is_err());
        assert!(u64_repeated_to_pb(None).is_err());
    }
}
