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
use simd_json::{ObjectHasher, StaticNode};
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
            Value::Array(a) => visitor.visit_seq(Array(a.into_iter())),
            Value::Object(o) => visitor.visit_map(ObjectAccess::new(o.into_iter())),
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
            Value::Array(a) => visitor.visit_seq(Array(a.into_iter())),
            Value::Object(o) => visitor.visit_map(ObjectAccess::new(o.into_iter())),
            _ => Err(Error::ExpectedMap),
        }
    }

    #[cfg_attr(not(feature = "no-inline"), inline)]
    fn deserialize_enum<V>(
        self,
        name: &str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        let (variant, value) = match self {
            Value::Object(value) => {
                let mut iter = value.into_iter();
                let (variant, value) = iter.next().ok_or_else(|| {
                    Error::Serde(format!("Missing enum type for variant in enum `{name}`"))
                })?;
                // enums are encoded in json as maps with a single key:value pair
                if let Some((extra, _)) = iter.next() {
                    return Err(Error::Serde(format!(
                        "extra values in enum `{name}`: `{variant}` .. `{extra}`"
                    )));
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
            None => Err(Error::Serde("expected newtype variant".to_string())),
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
                    visitor.visit_seq(Array(v.into_iter()))
                }
            }
            Some(_) | None => Err(Error::Serde("expected tuple variant".to_string())),
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
            Some(Value::Object(o)) => visitor.visit_map(ObjectAccess::new(o.into_iter())),
            Some(_) | None => Err(Error::Serde("expected struct variant".to_string())),
        }
    }
}

struct Array<'de>(std::vec::IntoIter<Value<'de>>);

// `SeqAccess` is provided to the `Visitor` to give it the ability to iterate
// through elements of the sequence.
impl<'de> SeqAccess<'de> for Array<'de> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        //TODO: This is ugly
        self.0
            .next()
            .map_or(Ok(None), |v| seed.deserialize(v).map(Some))
    }
}

struct ObjectAccess<'de, const N: usize = 32> {
    i: halfbrown::IntoIter<Cow<'de, str>, Value<'de>, N>,
    v: Option<Value<'de>>,
}

impl<'de, const N: usize> ObjectAccess<'de, N> {
    fn new(i: halfbrown::IntoIter<Cow<'de, str>, Value<'de>, N>) -> Self {
        Self { i, v: None }
    }
}

// `MapAccess` is provided to the `Visitor` to give it the ability to iterate
// through entries of the map.
impl<'de> MapAccess<'de> for ObjectAccess<'de> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        if let Some((k, v)) = self.i.next() {
            self.v = Some(v);
            seed.deserialize(Value::String(k)).map(Some)
        } else {
            Ok(None)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        match self.v.take() {
            Some(v) => seed.deserialize(v),
            None => Err(Error::Serde("emtpy object".to_string())),
        }
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

        let mut m = Object::with_capacity_and_hasher(size, ObjectHasher::default());
        while let Some(k) = map.next_key::<Cow<'_, str>>()? {
            let v = map.next_value()?;
            m.insert(k, v);
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
    use serde::Deserialize;
    use simd_json::prelude::*;

    use crate::{error::Result, Value};
    use crate::{literal, structurize};

    #[derive(serde::Deserialize, Debug)]
    pub struct SO {}

    #[derive(serde::Deserialize, Debug)]
    pub struct StructTest {
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
        let mut raw_json = "{}".to_string();
        let result: Result<StructTest> =
            structurize(crate::parse_to_value(unsafe { raw_json.as_bytes_mut() })?);
        assert!(result.is_ok());

        let mut raw_json = "{}".to_string();
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
        let result: Result<StructTest> =
            structurize(crate::parse_to_value(unsafe { raw_json.as_bytes_mut() })?);
        assert!(result.is_ok());

        let result: Result<N> =
            structurize(crate::parse_to_value(unsafe { raw_json.as_bytes_mut() })?);
        assert!(result.is_ok());

        let mut raw_json = "{}".to_string();
        let result: Result<StructTest> =
            structurize(crate::parse_to_value(unsafe { raw_json.as_bytes_mut() })?);
        assert!(result.is_ok());

        Ok(())
    }

    #[test]
    fn variant() {
        #[derive(Clone, Debug, Default)]
        struct NameWithConfig {
            name: String,
            config: Option<Value<'static>>,
        }
        impl<'v> serde::Deserialize<'v> for NameWithConfig {
            fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
            where
                D: serde::Deserializer<'v>,
            {
                // This is ugly but it's needed for `serde::Deserialize` to not explode on lifetimes
                // An error like this is otherwise produced:
                //     error: lifetime may not live long enough
                //     --> src/serde/value/borrowed/de.rs:960:25
                //      |
                //  953 |                 #[derive(Deserialize)]
                //      |                          ----------- lifetime `'de` defined here
                //  ...
                //  956 |                 enum Variants<'v> {
                //      |                               -- lifetime `'v` defined here
                //  ...
                //  960 |                         config: Option<borrowed::Value<'v>>,
                //      |                         ^^^^^^ requires that `'de` must outlive `'v`
                //      |
                //      = help: consider adding the following bound: `'de: 'v`

                //  error: lifetime may not live long enough
                //     --> src/serde/value/borrowed/de.rs:960:25
                //      |
                //  953 |                 #[derive(Deserialize)]
                //      |                          ----------- lifetime `'de` defined here
                //  ...
                //  956 |                 enum Variants<'v> {
                //      |                               -- lifetime `'v` defined here
                //  ...
                //  960 |                         config: Option<borrowed::Value<'v>>,
                //      |                         ^^^^^^ requires that `'v` must outlive `'de`
                //      |
                //      = help: consider adding the following bound: `'v: 'de`
                #[derive(Deserialize)]
                #[serde(bound(deserialize = "'de: 'v, 'v: 'de"), untagged)]
                enum Variants<'v> {
                    Name(String),
                    NameAndConfig {
                        name: String,
                        config: Option<Value<'v>>,
                    },
                }

                let var = Variants::deserialize(deserializer)?;

                match var {
                    Variants::Name(name) => Ok(NameWithConfig { name, config: None }),
                    Variants::NameAndConfig { name, config } => Ok(NameWithConfig {
                        name,
                        config: config.map(Value::into_static),
                    }),
                }
            }
        }

        let v = literal!({"name": "json", "config": {"mode": "sorted"}});
        let nac = NameWithConfig::deserialize(v).expect("could structurize two element struct");
        assert_eq!(nac.name, "json");
        assert!(nac.config.as_object().is_some());
        let v = literal!({"name": "yaml"});
        let nac = NameWithConfig::deserialize(v).expect("could structurize one element struct");
        assert_eq!(nac.name, "yaml");
        assert_eq!(nac.config, None);
        let v = literal!("name");
        let nac = NameWithConfig::deserialize(v).expect("could structurize string");
        assert_eq!(nac.name, "name");
        assert_eq!(nac.config, None);
    }
}
