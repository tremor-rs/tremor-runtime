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

use super::Value;
use simd_json::{prelude::*, BorrowedValue, OwnedValue};
use tremor_common::base64::{Engine, BASE64};

#[allow(clippy::cast_sign_loss, clippy::default_trait_access)]
impl<'value> PartialEq for Value<'value> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Static(s1), Self::Static(s2)) => s1 == s2,
            (Self::String(v1), Self::String(v2)) => v1.eq(v2),
            (Self::Array(v1), Self::Array(v2)) => v1.eq(v2),
            (Self::Object(v1), Self::Object(v2)) => v1.eq(v2),
            (Self::Bytes(v1), Self::Bytes(v2)) => v1.eq(v2),
            _ => false,
        }
    }
}

impl<'value, T> PartialEq<&T> for Value<'value>
where
    Value<'value>: PartialEq<T>,
{
    #[inline]
    #[must_use]
    fn eq(&self, other: &&T) -> bool {
        self == *other
    }
}

impl<'value> PartialEq<OwnedValue> for Value<'value> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &OwnedValue) -> bool {
        match (self, other) {
            (Self::Static(s1), OwnedValue::Static(s2)) => s1 == s2,
            (Self::String(v1), OwnedValue::String(v2)) => v1.eq(v2),
            (Self::Array(v1), OwnedValue::Array(v2)) => v1.eq(v2),
            (Self::Object(v1), OwnedValue::Object(v2)) => {
                if v1.len() != v2.len() {
                    return false;
                }
                v2.iter()
                    .all(|(key, value)| v1.get(key.as_str()).map_or(false, |v| v.eq(value)))
            }
            _ => false,
        }
    }
}

impl<'value> PartialEq<BorrowedValue<'value>> for Value<'value> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &BorrowedValue) -> bool {
        match (self, other) {
            (Self::Static(s1), BorrowedValue::Static(s2)) => s1 == s2,
            (Self::String(v1), BorrowedValue::String(v2)) => v1.as_ref().eq(v2.as_ref()),
            (Self::Array(v1), BorrowedValue::Array(v2)) => v1.eq(v2),
            (Self::Object(v1), BorrowedValue::Object(v2)) => {
                if v1.len() != v2.len() {
                    return false;
                }
                v2.iter()
                    .all(|(key, value)| v1.get(key.as_ref()).map_or(false, |v| v.eq(value)))
            }
            _ => false,
        }
    }
}

impl<'value> From<Value<'value>> for OwnedValue {
    #[inline]
    #[must_use]
    fn from(other: Value<'value>) -> OwnedValue {
        match other {
            Value::Static(s) => OwnedValue::from(s),
            Value::String(s) => OwnedValue::from(s.to_string()),
            Value::Array(a) => a.into_iter().collect(),
            Value::Object(m) => m.into_iter().collect(),
            Value::Bytes(b) => OwnedValue::from(BASE64.encode(b)),
        }
    }
}

impl<'value> From<Value<'value>> for BorrowedValue<'value> {
    #[inline]
    #[must_use]
    fn from(other: Value<'value>) -> BorrowedValue<'value> {
        match other {
            Value::Static(s) => BorrowedValue::from(s),
            Value::String(s) => BorrowedValue::from(s.to_string()),
            Value::Array(a) => a.into_iter().collect(),
            Value::Object(m) => m.into_iter().collect(),
            Value::Bytes(b) => BorrowedValue::from(BASE64.encode(b)),
        }
    }
}

impl<'v> PartialEq<()> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, _other: &()) -> bool {
        self.is_null()
    }
}

impl<'v> PartialEq<bool> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &bool) -> bool {
        self.as_bool().is_some_and(|t| t.eq(other))
    }
}

impl<'v> PartialEq<beef::Cow<'v, str>> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &beef::Cow<str>) -> bool {
        self.as_str().is_some_and(|t| t.eq(other))
    }
}

impl<'v> PartialEq<std::borrow::Cow<'v, str>> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &std::borrow::Cow<str>) -> bool {
        self.as_str().is_some_and(|t| t.eq(other))
    }
}

impl<'v> PartialEq<str> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &str) -> bool {
        self.as_str().is_some_and(|t| t.eq(other))
    }
}

impl<'v> PartialEq<&str> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &&str) -> bool {
        self == *other
    }
}

impl<'v> PartialEq<String> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &String) -> bool {
        self.as_str().is_some_and(|t| t.eq(other))
    }
}

impl<'v> PartialEq<i8> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &i8) -> bool {
        self.as_i8().is_some_and(|t| t.eq(other))
    }
}

impl<'v> PartialEq<i16> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &i16) -> bool {
        self.as_i16().is_some_and(|t| t.eq(other))
    }
}

impl<'v> PartialEq<i32> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &i32) -> bool {
        self.as_i32().is_some_and(|t| t.eq(other))
    }
}

impl<'v> PartialEq<i64> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &i64) -> bool {
        self.as_i64().is_some_and(|t| t.eq(other))
    }
}

impl<'v> PartialEq<i128> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &i128) -> bool {
        self.as_i128().is_some_and(|t| t.eq(other))
    }
}

impl<'v> PartialEq<u8> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &u8) -> bool {
        self.as_u8().is_some_and(|t| t.eq(other))
    }
}

impl<'v> PartialEq<u16> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &u16) -> bool {
        self.as_u16().is_some_and(|t| t.eq(other))
    }
}

impl<'v> PartialEq<u32> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &u32) -> bool {
        self.as_u32().is_some_and(|t| t.eq(other))
    }
}

impl<'v> PartialEq<u64> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &u64) -> bool {
        self.as_u64().is_some_and(|t| t.eq(other))
    }
}

impl<'v> PartialEq<usize> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &usize) -> bool {
        self.as_usize().is_some_and(|t| t.eq(other))
    }
}

impl<'v> PartialEq<u128> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &u128) -> bool {
        self.as_u128().is_some_and(|t| t.eq(other))
    }
}

impl<'v> PartialEq<f32> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &f32) -> bool {
        self.as_f32().is_some_and(|t| t.eq(other))
    }
}

impl<'v> PartialEq<f64> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &f64) -> bool {
        self.as_f64().is_some_and(|t| t.eq(other))
    }
}

impl<'v, T> PartialEq<&[T]> for Value<'v>
where
    Value<'v>: PartialEq<T>,
{
    #[inline]
    #[must_use]
    fn eq(&self, other: &&[T]) -> bool {
        self.as_array().is_some_and(|t| t.eq(other))
    }
}

impl<'v, K, T, S> PartialEq<std::collections::HashMap<K, T, S>> for Value<'v>
where
    K: AsRef<str> + std::hash::Hash + Eq,
    Value<'v>: PartialEq<T>,
    S: std::hash::BuildHasher,
{
    #[inline]
    #[must_use]
    fn eq(&self, other: &std::collections::HashMap<K, T, S>) -> bool {
        self.as_object().map_or(false, |object| {
            object.len() == other.len()
                && other
                    .iter()
                    .all(|(key, value)| object.get(key.as_ref()).map_or(false, |v| *v == *value))
        })
    }
}

#[cfg(test)]
mod test {
    use crate::{literal, Value};

    #[test]
    fn diff_types() {
        assert_ne!(Value::from(1), Value::from("snot"));
    }
    #[test]
    fn obj() {
        let o1 = literal!({"snot":1});
        let o2 = literal!({"snot":1, "badger":2});
        assert_ne!(o1, o2);
    }

    #[test]
    fn number_128() {
        assert_ne!(Value::from(1), 2_i128);
        assert_ne!(Value::from(1), 2_u128);
        assert_eq!(Value::from(42), 42_i128);
        assert_eq!(Value::from(42), 42_u128);
    }

    #[test]
    fn bytes() {
        let v1 = vec![1_u8, 2, 3];
        let v2 = vec![1_u8, 2, 3, 4];
        assert_eq!(
            Value::Bytes(v1.clone().into()),
            Value::Bytes(v1.clone().into())
        );
        assert_ne!(Value::Bytes(v1.into()), Value::Bytes(v2.into()));
    }
}
