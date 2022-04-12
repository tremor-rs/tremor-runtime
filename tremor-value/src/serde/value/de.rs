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

use crate::{Error, Object, Value};
use beef::Cow;
use serde::de::{EnumAccess, IntoDeserializer, VariantAccess};
use serde_ext::de::{
    self, Deserialize, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor,
};
use serde_ext::forward_to_deserialize_any;
use simd_json::StaticNode;
use std::fmt;

impl<'de> de::Deserializer<'de> for Value<'de> {
    type Error = Error;

    // Look at the input data to decide what Serde data model type to
    // deserialize as. Not all data formats are able to support this operation.
    // Formats that support `deserialize_any` are known as self-describing.
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        match self {
            Value::Static(StaticNode::Null) => visitor.visit_unit(),
            Value::Static(StaticNode::Bool(b)) => visitor.visit_bool(b),
            Self::Static(StaticNode::I64(n)) => visitor.visit_i64(n),
            #[cfg(feature = "128bit")]
            Self::Static(StaticNode::I128(n)) => visitor.visit_i128(n),
            Self::Static(StaticNode::U64(n)) => visitor.visit_u64(n),
            #[cfg(feature = "128bit")]
            Self::Static(StaticNode::U128(n)) => visitor.visit_u128(n),
            Value::Static(StaticNode::F64(n)) => visitor.visit_f64(n),
            Value::String(s) => {
                if s.is_borrowed() {
                    visitor.visit_borrowed_str(s.unwrap_borrowed())
                } else {
                    visitor.visit_string(s.into_owned())
                }
            }
            Value::Array(a) => visitor.visit_seq(Array(a.iter())),
            Value::Object(o) => visitor.visit_map(ObjectAccess {
                i: o.iter(),
                v: &Value::Static(StaticNode::Null),
            }),
            Value::Bytes(b) => visitor.visit_bytes(&b),
        }
    }

    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        if self == Self::Static(StaticNode::Null) {
            visitor.visit_unit()
        } else {
            visitor.visit_some(self)
        }
    }

    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        match self {
            // Give the visitor access to each element of the sequence.
            Value::Array(a) => visitor.visit_seq(Array(a.iter())),
            Value::Object(o) => visitor.visit_map(ObjectAccess {
                i: o.iter(),
                v: &Value::Static(StaticNode::Null),
            }),
            _ => Err(Error::ExpectedMap),
        }
    }

    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn deserialize_enum<V>(
        self,
        _name: &str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        let (variant, value) = match self {
            Value::Object(value) => {
                let mut iter = value.into_iter();
                let (variant, value) = match iter.next() {
                    Some(v) => v,
                    None => {
                        // FIXME: better error
                        return Err(Error::Serde("missing enum type".to_string()));
                    }
                };
                // enums are encoded in json as maps with a single key:value pair
                if iter.next().is_some() {
                    // FIXME: better error
                    return Err(Error::Serde("extra values in enum".to_string()));
                }
                (variant, Some(value))
            }
            Value::String(variant) => (variant, None),
            _other => {
                return Err(Error::Serde("Not a string".to_string()));
            }
        };

        visitor.visit_enum(EnumDeserializer { variant, value })
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
            bytes byte_buf unit unit_struct newtype_struct seq tuple
            tuple_struct map identifier ignored_any
    }
}

struct EnumDeserializer<'de> {
    variant: Cow<'de, str>,
    value: Option<Value<'de>>,
}

impl<'de> EnumAccess<'de> for EnumDeserializer<'de> {
    type Error = Error;
    type Variant = VariantDeserializer<'de>;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Error>
    where
        V: DeserializeSeed<'de>,
    {
        let variant = self.variant.into_deserializer();
        let visitor = VariantDeserializer { value: self.value };
        seed.deserialize(variant).map(|v| (v, visitor))
    }
}

impl<'de> IntoDeserializer<'de, Error> for Value<'de> {
    type Deserializer = Self;

    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}

struct VariantDeserializer<'de> {
    value: Option<Value<'de>>,
}

impl<'de> VariantAccess<'de> for VariantDeserializer<'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<(), Error> {
        match self.value {
            Some(value) => Deserialize::deserialize(value),
            None => Ok(()),
        }
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Error>
    where
        T: DeserializeSeed<'de>,
    {
        match self.value {
            Some(value) => seed.deserialize(value),
            None => Err(Error::Serde("expected newtype variant".to_string()))
            // None => Err(serde::de::Error::invalid_type(
            //     Unexpected::UnitVariant,
            //     &"newtype variant",
            // )),
        }
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Some(Value::Array(v)) => {
                if v.is_empty() {
                    visitor.visit_unit()
                } else {
                    visitor.visit_seq(Array(v.iter()))
                }
            }
            // FIXME
            Some(_) | None => Err(Error::Serde("expected tuple variant".to_string()))
            // Some(other) => Err(serde::de::Error::invalid_type(
            //     other.unexpected(),
            //     &"tuple variant",
            // )),
            // None => Err(serde::de::Error::invalid_type(
            //     Unexpected::UnitVariant,
            //     &"tuple variant",
            // )),
        }
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Some(Value::Object(v)) => visitor.visit_map(ObjectAccess {
                i: v.iter(),
                v: &Value::Static(StaticNode::Null),
            }),
            // FIXME
            Some(_) | None => Err(Error::Serde("expected struct variant".to_string()))

            // Some(other) => Err(serde::de::Error::invalid_type(
            //     other.unexpected(),
            //     &"struct variant",
            // )),
            // None => Err(serde::de::Error::invalid_type(
            //     Unexpected::UnitVariant,
            //     &"struct variant",
            // )),
        }
    }
}

struct Array<'de, 'value: 'de>(std::slice::Iter<'de, Value<'value>>);

// `SeqAccess` is provided to the `Visitor` to give it the ability to iterate
// through elements of the sequence.
impl<'de, 'value> SeqAccess<'de> for Array<'value, 'de> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        //TODO: This is ugly
        self.0
            .next()
            .map_or(Ok(None), |v| seed.deserialize(v.clone()).map(Some))
    }
}

struct ObjectAccess<'de, 'value: 'de> {
    i: halfbrown::Iter<'de, Cow<'value, str>, Value<'value>>,
    v: &'de Value<'value>,
}

// `MapAccess` is provided to the `Visitor` to give it the ability to iterate
// through entries of the map.
impl<'de, 'value> MapAccess<'de> for ObjectAccess<'value, 'de> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        if let Some((k, v)) = self.i.next() {
            self.v = v;
            seed.deserialize(Value::String(k.clone())).map(Some)
        } else {
            Ok(None)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        //TODO: This is ugly
        seed.deserialize(self.v.clone())
    }
}

impl<'de> Deserialize<'de> for Value<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Value<'de>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(ValueVisitor)
    }
}

struct ValueVisitor;

impl<'de> Visitor<'de> for ValueVisitor {
    type Value = Value<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an JSONesque value")
    }

    /****************** unit ******************/
    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(Value::Static(StaticNode::Null))
    }

    /****************** bool ******************/
    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(Value::Static(StaticNode::Bool(value)))
    }

    /****************** Option ******************/
    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(Value::Static(StaticNode::Null))
    }

    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }

    /****************** enum ******************/
    /*
    fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error> where
        A: EnumAccess<'de>,
    {
    }
     */

    /****************** i64 ******************/
    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_i8<E>(self, value: i8) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Static(StaticNode::I64(i64::from(value))))
    }

    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_i16<E>(self, value: i16) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Static(StaticNode::I64(i64::from(value))))
    }

    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_i32<E>(self, value: i32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Static(StaticNode::I64(i64::from(value))))
    }

    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Static(StaticNode::I64(value)))
    }

    #[cfg(feature = "128bit")]
    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_i128<E>(self, value: i128) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Static(StaticNode::I128(value)))
    }

    /****************** u64 ******************/

    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_u8<E>(self, value: u8) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Static(StaticNode::U64(u64::from(value))))
    }

    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_u16<E>(self, value: u16) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Static(StaticNode::U64(u64::from(value))))
    }

    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_u32<E>(self, value: u32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Static(StaticNode::U64(u64::from(value))))
    }

    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Static(StaticNode::U64(value)))
    }

    #[cfg(feature = "128bit")]
    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_u128<E>(self, value: u128) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Static(StaticNode::U128(value)))
    }

    /****************** f64 ******************/

    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_f32<E>(self, value: f32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Static(StaticNode::F64(f64::from(value))))
    }

    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Static(StaticNode::F64(value)))
    }

    /****************** stringy stuff ******************/
    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_char<E>(self, value: char) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::from(value.to_string()))
    }

    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::from(value))
    }

    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::String(value.to_owned().into()))
    }

    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::String(value.into()))
    }

    /****************** byte stuff ******************/

    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_borrowed_bytes<E>(self, value: &'de [u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Bytes(value.into()))
    }
    /*

    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_str<E>(self, value: &[u8]) -> Result<Self::Value, E>
    where
    'a: 'de
        E: de::Error,
    {
      Ok(Value::String(value))
    }

    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_string<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
      Ok(Value::String(&value))
    }
     */
    /****************** nested stuff ******************/

    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let size = map.size_hint().unwrap_or_default();

        let mut m = Object::with_capacity(size);
        while let Some(k) = map.next_key::<&str>()? {
            let v = map.next_value()?;
            m.insert(k.into(), v);
        }
        Ok(Value::from(m))
    }

    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let size = seq.size_hint().unwrap_or_default();

        let mut v = Vec::with_capacity(size);
        while let Some(e) = seq.next_element()? {
            v.push(e);
        }
        Ok(Value::Array(v))
    }
}

/// Returns a struct populated against the DOM value via serde deserialization
///
/// # Errors
///
/// Will return Err if the DOM value cannot be deserialized to the target struct  
pub fn structurize<'de, T>(value: Value<'de>) -> crate::error::Result<T>
where
    T: de::Deserialize<'de>,
{
    match T::deserialize(value) {
        Ok(t) => Ok(t),
        Err(e) => Err(Error::Serde(e.to_string())),
    }
}

#[cfg(test)]
mod test {
    use crate::error::Result;
    use crate::structurize;

    #[derive(serde::Deserialize, Debug)]
    pub struct SO {}

    #[derive(serde::Deserialize, Debug)]
    pub struct S {
        pub o: Option<SO>,
        pub s: Option<String>,
        pub b: Option<bool>,
        pub a: Option<Vec<String>>,
        pub uw: Option<u8>,
        pub ux: Option<u16>,
        pub uy: Option<u32>,
        pub uz: Option<u64>,
        pub sw: Option<i8>,
        pub sx: Option<i16>,
        pub sy: Option<i32>,
        pub sz: Option<i64>,
        pub fx: Option<f32>,
        pub fy: Option<f64>,
    }

    #[derive(serde::Deserialize, Debug)]
    pub struct N {
        pub o: SO,
        pub s: String,
        pub b: bool,
        pub a: Vec<String>,
        pub uw: u8,
        pub ux: u16,
        pub uy: u32,
        pub uz: u64,
        pub sw: i8,
        pub sx: i16,
        pub sy: i32,
        pub sz: i64,
        pub fx: f32,
        pub fy: f64,
    }

    #[test]
    fn option_field_absent() -> Result<()> {
        let mut raw_json = r#"{}"#.to_string();
        let result: Result<S> =
            structurize(crate::parse_to_value(unsafe { raw_json.as_bytes_mut() })?);
        assert!(result.is_ok());

        let mut raw_json = r#"{}"#.to_string();
        let result: Result<N> =
            structurize(crate::parse_to_value(unsafe { raw_json.as_bytes_mut() })?);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn option_field_present() -> Result<()> {
        let mut raw_json = r#"{
            "o": {},
            "b": true,
            "s": "snot",
            "a": [],
            "uw": 0,
            "ux": 0,
            "uy": 0,
            "uz": 0,
            "sw": 0,
            "sx": 0,
            "sy": 0,
            "sz": 0,
            "fx": 0,
            "fy": 0
        }"#
        .to_string();
        let result: Result<S> =
            structurize(crate::parse_to_value(unsafe { raw_json.as_bytes_mut() })?);
        assert!(result.is_ok());

        let result: Result<N> =
            structurize(crate::parse_to_value(unsafe { raw_json.as_bytes_mut() })?);
        assert!(result.is_ok());

        let mut raw_json = r#"{}"#.to_string();
        let result: Result<S> =
            structurize(crate::parse_to_value(unsafe { raw_json.as_bytes_mut() })?);
        assert!(result.is_ok());

        Ok(())
    }
}
