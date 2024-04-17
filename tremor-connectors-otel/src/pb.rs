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

use tremor_value::prelude::*;

/// converts a maybe null option to a string. if the value is none it returns an empty string
/// # Errors
/// It errors if the value is some but not a string
pub(crate) fn maybe_string(data: Option<&Value<'_>>) -> Result<String, TryTypeError> {
    if data.is_none() {
        Ok(String::new())
    } else {
        data.try_as_str().map(ToString::to_string)
    }
}

/// *sob* this is a trait that should not exist
pub(crate) trait FromValue: Sized {
    /// *sob* this is a trait that should not exist
    /// # Errors
    /// It errors if the value is some but not the right type
    fn maybe_from_value(data: Option<&Value<'_>>) -> Result<Option<Self>, TryTypeError> {
        data.map(|d| Self::from_value(d)).transpose()
    }
    /// *sob* this is a trait that should not exist
    /// # Errors
    /// It errors if the value is not the right type
    fn from_value(data: &Value<'_>) -> Result<Self, TryTypeError>;
}

/// one of many protobuf helper functerins that require to be cleaned up
///
/// TODO: kill this with fire
///
/// # Errors
/// It errors if the value is some but not a the right type
pub(crate) fn maybe_from_value<T: FromValue>(
    data: Option<&Value<'_>>,
) -> Result<Option<T>, TryTypeError> {
    T::maybe_from_value(data)
}

/// Converts a json array of f64 values to a protobuf repeated f64
/// # Errors
/// It errors if the value is not an array or if any of the values are not `f64`
pub(crate) fn f64_repeated(json: Option<&Value<'_>>) -> Result<Vec<f64>, TryTypeError> {
    json.try_as_array()?
        .iter()
        .map(Some)
        .map(|v| v.try_as_f64())
        .collect()
}

/// Converts a json array of u64 values to a protobuf repeated u64
/// # Errors
/// It errors if the value is not an array or if any of the values are not `u64`
pub(crate) fn u64_repeated(json: Option<&Value<'_>>) -> Result<Vec<u64>, TryTypeError> {
    json.try_as_array()?
        .iter()
        .map(Some)
        .map(|v| v.try_as_u64())
        .collect()
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
            let pb = maybe_string(Some(&json))?;
            prop_assert_eq!(&arb_string, &pb);
            prop_assert_eq!(pb.len(), arb_string.len());
        }




        #[test]
        fn prop_pb_f64_repeated((vec, _index) in fveci()) {
            let json: Value = literal!(vec.clone());
            let pb = f64_repeated(Some(&json))?;
            prop_assert_eq!(&vec, &pb);
            prop_assert_eq!(pb.len(), vec.len());
        }

        #[test]
        fn prop_pb_u64_repeated((vec, _index) in uveci()) {
            let json: Value = literal!(vec.clone());
            let pb = u64_repeated(Some(&json))?;
            prop_assert_eq!(&vec, &pb);
            prop_assert_eq!(pb.len(), vec.len());
        }
    }

    #[test]
    fn bad_string() {
        // NOTE no coercion, no casting
        assert!(maybe_string(Some(&Value::from(false))).is_err());

        // NOTE We allow None for string and map to pb default of "" ( empty string )
        assert_eq!(maybe_string(None).ok(), Some(String::new()));
    }

    #[test]
    fn bad_repeated_numerics() {
        assert!(f64_repeated(Some(&Value::from(vec![true]))).is_err());
        assert!(u64_repeated(Some(&Value::from(vec![true]))).is_err());
    }

    #[test]
    fn bad_json_inputs() {
        // NOTE All of these functions are designed to process the output of
        // tremor-value/simd-json `object.get("field-name")` or other *valid*
        // calls that return `Option<&Value>`.
        //
        // As such attempting to convert a non-existent value is an
        // error by construction
        assert!(f64_repeated(None).is_err());
        assert!(u64_repeated(None).is_err());
    }
}
