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
use simd_json::prelude::*;
use simd_json::OwnedValue;

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

impl<'value> Into<OwnedValue> for Value<'value> {
    #[inline]
    #[must_use]
    fn into(self) -> OwnedValue {
        match self {
            Value::Static(s) => OwnedValue::from(s),
            Value::String(s) => OwnedValue::from(s.to_string()),
            Value::Array(a) => a.into_iter().collect(),
            Value::Object(m) => m.into_iter().collect(),
            Value::Bytes(b) => OwnedValue::from(base64::encode(b)),
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
        self.as_bool().map(|t| t.eq(other)).unwrap_or_default()
    }
}

impl<'v> PartialEq<str> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &str) -> bool {
        self.as_str().map(|t| t.eq(other)).unwrap_or_default()
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
        self.as_str().map(|t| t.eq(other)).unwrap_or_default()
    }
}

impl<'v> PartialEq<i8> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &i8) -> bool {
        self.as_i8().map(|t| t.eq(other)).unwrap_or_default()
    }
}

impl<'v> PartialEq<i16> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &i16) -> bool {
        self.as_i16().map(|t| t.eq(other)).unwrap_or_default()
    }
}

impl<'v> PartialEq<i32> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &i32) -> bool {
        self.as_i32().map(|t| t.eq(other)).unwrap_or_default()
    }
}

impl<'v> PartialEq<i64> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &i64) -> bool {
        self.as_i64().map(|t| t.eq(other)).unwrap_or_default()
    }
}

impl<'v> PartialEq<i128> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &i128) -> bool {
        self.as_i128().map(|t| t.eq(other)).unwrap_or_default()
    }
}

impl<'v> PartialEq<u8> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &u8) -> bool {
        self.as_u8().map(|t| t.eq(other)).unwrap_or_default()
    }
}

impl<'v> PartialEq<u16> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &u16) -> bool {
        self.as_u16().map(|t| t.eq(other)).unwrap_or_default()
    }
}

impl<'v> PartialEq<u32> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &u32) -> bool {
        self.as_u32().map(|t| t.eq(other)).unwrap_or_default()
    }
}

impl<'v> PartialEq<u64> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &u64) -> bool {
        self.as_u64().map(|t| t.eq(other)).unwrap_or_default()
    }
}

impl<'v> PartialEq<usize> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &usize) -> bool {
        self.as_usize().map(|t| t.eq(other)).unwrap_or_default()
    }
}

impl<'v> PartialEq<u128> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &u128) -> bool {
        self.as_u128().map(|t| t.eq(other)).unwrap_or_default()
    }
}

impl<'v> PartialEq<f32> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &f32) -> bool {
        self.as_f32().map(|t| t.eq(other)).unwrap_or_default()
    }
}

impl<'v> PartialEq<f64> for Value<'v> {
    #[inline]
    #[must_use]
    fn eq(&self, other: &f64) -> bool {
        self.as_f64().map(|t| t.eq(other)).unwrap_or_default()
    }
}

impl<'v, T> PartialEq<&[T]> for Value<'v>
where
    Value<'v>: PartialEq<T>,
{
    #[inline]
    #[must_use]
    fn eq(&self, other: &&[T]) -> bool {
        self.as_array().map(|t| t.eq(other)).unwrap_or_default()
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
    use crate::Value;
    use simd_json::json;
    #[test]
    fn diff_types() {
        assert_ne!(Value::from(1), Value::from("snot"));
    }
    #[test]
    fn obj() {
        let o1: Value = json!({"snot":1}).into();
        let o2: Value = json!({"snot":1, "badger":2}).into();
        assert_ne!(o1, o2);
    }

    #[test]
    fn number_128() {
        assert_ne!(Value::from(1), 2_i128);
        assert_ne!(Value::from(1), 2_u128);
        assert_eq!(Value::from(42), 42_i128);
        assert_eq!(Value::from(42), 42_u128);
    }
}
