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

use crate::{value::Bytes, Error, Object, Result, Value};
use serde_ext::ser::{
    self, Serialize, SerializeMap as SerializeMapTrait, SerializeSeq as SerializeSeqTrait,
};
use simd_json::{stry, ObjectHasher, StaticNode};

type Impossible<T> = ser::Impossible<T, Error>;

impl<'value> Serialize for Value<'value> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match self {
            Value::Static(StaticNode::Null) => serializer.serialize_unit(),
            Value::Static(StaticNode::Bool(b)) => serializer.serialize_bool(*b),
            Value::Static(StaticNode::F64(f)) => serializer.serialize_f64(*f),
            Value::Static(StaticNode::U64(i)) => serializer.serialize_u64(*i),
            #[cfg(feature = "128bit")]
            Value::Static(StaticNode::U128(i)) => serializer.serialize_u128(*i),
            Value::Static(StaticNode::I64(i)) => serializer.serialize_i64(*i),
            #[cfg(feature = "128bit")]
            Value::Static(StaticNode::I128(i)) => serializer.serialize_i128(*i),
            Value::String(s) => serializer.serialize_str(s),
            Value::Array(v) => {
                let mut seq = serializer.serialize_seq(Some(v.len()))?;
                for e in v {
                    seq.serialize_element(e)?;
                }
                seq.end()
            }
            Value::Object(m) => {
                let mut map = serializer.serialize_map(Some(m.len()))?;
                for (k, v) in m.iter() {
                    let k: &str = k;
                    map.serialize_entry(k, v)?;
                }
                map.end()
            }
            Value::Bytes(b) => serializer.serialize_bytes(b),
        }
    }
}

/// convert anything implementing `Serialize` into a `Value` using our own `Serializer`.
///
/// # Errors
///
/// if the given value cannot be serialized
pub fn to_value<T>(value: T) -> Result<Value<'static>>
where
    T: Serialize,
{
    value.serialize(Serializer::default())
}

#[derive(Default)]
pub struct Serializer {}

impl serde::Serializer for Serializer {
    type Ok = Value<'static>;
    type Error = Error;

    type SerializeSeq = SerializeVec;
    type SerializeTuple = SerializeVec;
    type SerializeTupleStruct = SerializeVec;
    type SerializeTupleVariant = SerializeTupleVariant;
    type SerializeMap = SerializeMap;
    type SerializeStruct = SerializeMap;
    type SerializeStructVariant = SerializeStructVariant;

    #[inline]
    fn serialize_bool(self, value: bool) -> Result<Value<'static>> {
        Ok(Value::Static(StaticNode::Bool(value)))
    }

    #[inline]
    fn serialize_i8(self, value: i8) -> Result<Value<'static>> {
        self.serialize_i64(i64::from(value))
    }

    #[inline]
    fn serialize_i16(self, value: i16) -> Result<Value<'static>> {
        self.serialize_i64(i64::from(value))
    }

    #[inline]
    fn serialize_i32(self, value: i32) -> Result<Value<'static>> {
        self.serialize_i64(i64::from(value))
    }

    fn serialize_i64(self, value: i64) -> Result<Value<'static>> {
        Ok(Value::Static(StaticNode::I64(value)))
    }

    #[inline]
    fn serialize_u8(self, value: u8) -> Result<Value<'static>> {
        self.serialize_u64(u64::from(value))
    }

    #[inline]
    fn serialize_u16(self, value: u16) -> Result<Value<'static>> {
        self.serialize_u64(u64::from(value))
    }

    #[inline]
    fn serialize_u32(self, value: u32) -> Result<Value<'static>> {
        self.serialize_u64(u64::from(value))
    }

    #[inline]
    #[allow(clippy::cast_possible_wrap)]
    fn serialize_u64(self, value: u64) -> Result<Value<'static>> {
        Ok(Value::Static(StaticNode::U64(value)))
    }

    #[inline]
    fn serialize_f32(self, value: f32) -> Result<Value<'static>> {
        self.serialize_f64(f64::from(value))
    }

    #[inline]
    fn serialize_f64(self, value: f64) -> Result<Value<'static>> {
        Ok(Value::Static(StaticNode::F64(value)))
    }

    #[inline]
    fn serialize_char(self, value: char) -> Result<Value<'static>> {
        let mut s = String::new();
        s.push(value);
        self.serialize_str(&s)
    }

    #[inline]
    fn serialize_str(self, value: &str) -> Result<Value<'static>> {
        Ok(Value::from(value.to_owned()))
    }

    fn serialize_bytes(self, value: &[u8]) -> Result<Value<'static>> {
        Ok(Value::Bytes(Bytes::owned(value.to_vec())))
    }

    #[inline]
    fn serialize_unit(self) -> Result<Value<'static>> {
        Ok(Value::Static(StaticNode::Null))
    }

    #[inline]
    fn serialize_unit_struct(self, _name: &'static str) -> Result<Value<'static>> {
        self.serialize_unit()
    }

    #[inline]
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Value<'static>> {
        self.serialize_str(variant)
    }

    #[inline]
    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<Value<'static>>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Value<'static>>
    where
        T: Serialize + ?Sized,
    {
        let mut values = Object::with_capacity_and_hasher(1, ObjectHasher::default());
        values.insert(variant.into(), stry!(to_value(value)));
        Ok(Value::from(values))
    }

    #[inline]
    fn serialize_none(self) -> Result<Value<'static>> {
        self.serialize_unit()
    }

    #[inline]
    fn serialize_some<T>(self, value: &T) -> Result<Value<'static>>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(SerializeVec {
            vec: Vec::with_capacity(len.unwrap_or(0)),
        })
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Ok(SerializeTupleVariant {
            name: variant.to_owned(),
            vec: Vec::with_capacity(len),
        })
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap> {
        Ok(SerializeMap::Map {
            map: Object::with_capacity_and_hasher(len.unwrap_or_default(), ObjectHasher::default()),
            next_key: None,
        })
    }

    fn serialize_struct(self, _name: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        self.serialize_map(Some(len))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Ok(SerializeStructVariant {
            name: variant.to_owned(),
            map: Object::with_capacity_and_hasher(len, ObjectHasher::default()),
        })
    }
}

pub struct SerializeVec {
    vec: Vec<Value<'static>>,
}

pub struct SerializeTupleVariant {
    name: String,
    vec: Vec<Value<'static>>,
}

pub enum SerializeMap {
    Map {
        map: Object<'static>,
        next_key: Option<String>,
    },
}

pub struct SerializeStructVariant {
    name: String,
    map: Object<'static>,
}

impl serde::ser::SerializeSeq for SerializeVec {
    type Ok = Value<'static>;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: Serialize + ?Sized,
    {
        self.vec.push(stry!(to_value(value)));
        Ok(())
    }

    fn end(self) -> Result<Value<'static>> {
        Ok(Value::Array(self.vec))
    }
}

impl serde::ser::SerializeTuple for SerializeVec {
    type Ok = Value<'static>;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: Serialize + ?Sized,
    {
        serde::ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Value<'static>> {
        serde::ser::SerializeSeq::end(self)
    }
}

impl serde::ser::SerializeTupleStruct for SerializeVec {
    type Ok = Value<'static>;
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: Serialize + ?Sized,
    {
        serde::ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Value<'static>> {
        serde::ser::SerializeSeq::end(self)
    }
}

impl serde::ser::SerializeTupleVariant for SerializeTupleVariant {
    type Ok = Value<'static>;
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: Serialize + ?Sized,
    {
        self.vec.push(stry!(to_value(value)));
        Ok(())
    }

    fn end(self) -> Result<Value<'static>> {
        let mut object = Object::with_capacity_and_hasher(1, ObjectHasher::default());

        object.insert(self.name.into(), Value::Array(self.vec));

        Ok(Value::from(object))
    }
}

impl serde::ser::SerializeMap for SerializeMap {
    type Ok = Value<'static>;
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<()>
    where
        T: Serialize + ?Sized,
    {
        match *self {
            Self::Map {
                ref mut next_key, ..
            } => {
                *next_key = Some(stry!(key.serialize(MapKeySerializer {})));
                Ok(())
            }
        }
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<()>
    where
        T: Serialize + ?Sized,
    {
        match *self {
            Self::Map {
                ref mut map,
                ref mut next_key,
            } => {
                let key = next_key.take();
                // ALLOW: Panic because this indicates a bug in the program rather than an expected failure.
                let key = key.expect("serialize_value called before serialize_key");
                map.insert(key.into(), stry!(to_value(value)));
                Ok(())
            }
        }
    }

    fn end(self) -> Result<Value<'static>> {
        match self {
            Self::Map { map, .. } => Ok(Value::from(map)),
        }
    }
}

struct MapKeySerializer {}

fn key_must_be_a_string() -> Error {
    Error::Serde("Key must be a String.".to_string())
}

impl serde_ext::Serializer for MapKeySerializer {
    type Ok = String;
    type Error = Error;

    type SerializeSeq = Impossible<String>;
    type SerializeTuple = Impossible<String>;
    type SerializeTupleStruct = Impossible<String>;
    type SerializeTupleVariant = Impossible<String>;
    type SerializeMap = Impossible<String>;
    type SerializeStruct = Impossible<String>;
    type SerializeStructVariant = Impossible<String>;

    #[inline]
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok> {
        Ok(variant.to_owned())
    }

    #[inline]
    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<Self::Ok>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self)
    }

    fn serialize_bool(self, _value: bool) -> Result<Self::Ok> {
        Err(key_must_be_a_string())
    }

    fn serialize_i8(self, _value: i8) -> Result<Self::Ok> {
        //Ok(value.to_string())
        Err(key_must_be_a_string())
    }

    fn serialize_i16(self, _value: i16) -> Result<Self::Ok> {
        //Ok(value.to_string())
        Err(key_must_be_a_string())
    }

    fn serialize_i32(self, _value: i32) -> Result<Self::Ok> {
        //Ok(value.to_string())
        Err(key_must_be_a_string())
    }

    fn serialize_i64(self, _value: i64) -> Result<Self::Ok> {
        //Ok(value.to_string())
        Err(key_must_be_a_string())
    }

    fn serialize_u8(self, _value: u8) -> Result<Self::Ok> {
        //Ok(value.to_string())
        Err(key_must_be_a_string())
    }

    fn serialize_u16(self, _value: u16) -> Result<Self::Ok> {
        //Ok(value.to_string())
        Err(key_must_be_a_string())
    }

    fn serialize_u32(self, _value: u32) -> Result<Self::Ok> {
        //Ok(value.to_string())
        Err(key_must_be_a_string())
    }

    fn serialize_u64(self, _value: u64) -> Result<Self::Ok> {
        //Ok(value.to_string())
        Err(key_must_be_a_string())
    }

    fn serialize_f32(self, _value: f32) -> Result<Self::Ok> {
        //Err(key_must_be_a_string())
        Err(key_must_be_a_string())
    }

    fn serialize_f64(self, _value: f64) -> Result<Self::Ok> {
        //Err(key_must_be_a_string())
        Err(key_must_be_a_string())
    }

    fn serialize_char(self, _value: char) -> Result<Self::Ok> {
        Err(key_must_be_a_string())
    }

    #[inline]
    fn serialize_str(self, value: &str) -> Result<Self::Ok> {
        Ok(value.to_owned())
    }

    fn serialize_bytes(self, _value: &[u8]) -> Result<Self::Ok> {
        Err(key_must_be_a_string())
    }

    fn serialize_unit(self) -> Result<Self::Ok> {
        Err(key_must_be_a_string())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok> {
        Err(key_must_be_a_string())
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok>
    where
        T: Serialize + ?Sized,
    {
        Err(key_must_be_a_string())
    }

    fn serialize_none(self) -> Result<Self::Ok> {
        Err(key_must_be_a_string())
    }

    fn serialize_some<T>(self, _value: &T) -> Result<Self::Ok>
    where
        T: Serialize + ?Sized,
    {
        Err(key_must_be_a_string())
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        Err(key_must_be_a_string())
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Err(key_must_be_a_string())
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        Err(key_must_be_a_string())
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Err(key_must_be_a_string())
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        Err(key_must_be_a_string())
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Err(key_must_be_a_string())
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Err(key_must_be_a_string())
    }
}

impl serde::ser::SerializeStruct for SerializeMap {
    type Ok = Value<'static>;
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: Serialize + ?Sized,
    {
        match *self {
            Self::Map { .. } => {
                stry!(serde::ser::SerializeMap::serialize_key(self, key));
                serde::ser::SerializeMap::serialize_value(self, value)
            }
        }
    }

    fn end(self) -> Result<Value<'static>> {
        match self {
            Self::Map { .. } => serde::ser::SerializeMap::end(self),
        }
    }
}

impl serde::ser::SerializeStructVariant for SerializeStructVariant {
    type Ok = Value<'static>;
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: Serialize + ?Sized,
    {
        self.map.insert(key.into(), stry!(to_value(value)));
        Ok(())
    }

    fn end(self) -> Result<Value<'static>> {
        let mut object = Object::with_capacity_and_hasher(1, ObjectHasher::default());

        object.insert(self.name.into(), Value::from(self.map));

        Ok(Value::from(object))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::prelude::*;
    use beef::Cow;
    use serde_ext::Serialize;

    #[derive(Serialize)]
    enum Snot {
        Struct { badger: String, snot: Option<u64> },
        NotAStruct,
        TupleStruct(Vec<u8>, usize),
    }

    #[test]
    fn to_value_enum_variants() -> Result<()> {
        let x = Snot::Struct {
            badger: "snot".to_string(),
            snot: Some(0),
        };
        let value = to_value(x)?;
        let inner = value
            .get("Struct")
            .ok_or("Struct not serialized with its name at teh toplevel")?;
        let snot = inner.get("snot");
        assert_eq!(Some(&Value::Static(StaticNode::U64(0))), snot);
        assert_eq!(Some(&Value::String("snot".into())), inner.get("badger"));

        let not_a_struct = Snot::NotAStruct;
        let nas_value = to_value(not_a_struct)?;
        assert_eq!(Value::String("NotAStruct".into()), nas_value);

        let tuple = Snot::TupleStruct(vec![1, 2, 3], 3);
        let t_value = to_value(tuple)?;
        if let Value::Object(map) = t_value {
            if let Some(&Value::Array(values)) = map.get("TupleStruct").as_ref() {
                let first_field = values
                    .first()
                    .and_then(ValueAsContainer::as_array)
                    .ok_or("Vec<u8> not serialized as array")?;
                assert_eq!(
                    Some(&Value::Static(StaticNode::I64(1))),
                    first_field.first()
                );
                assert_eq!(Some(&Value::Static(StaticNode::I64(2))), first_field.get(1));
                assert_eq!(Some(&Value::Static(StaticNode::I64(3))), first_field.get(2));
                assert_eq!(Some(&Value::Static(StaticNode::U64(3))), values.get(1));
            }
        }

        Ok(())
    }
    macro_rules! assert_to_value {
        ($expected:pat, $arg:expr) => {
            let res = to_value($arg)?;
            match res {
                $expected => {}
                _ => fail!(
                    "{:?} did serialized to {:?}, instead of expected {:?}",
                    $arg,
                    res,
                    stringify!($expected)
                ),
            }
        };
    }

    macro_rules! fail {
        ( $($args:expr),+ ) => {
            assert!( false, $($args),* )
        }
    }

    #[test]
    fn serialize_numbers() -> Result<()> {
        // signed
        assert_to_value!(Value::Static(StaticNode::I64(1)), 1_i8);
        assert_to_value!(Value::Static(StaticNode::I64(127)), i8::max_value());
        assert_to_value!(Value::Static(StaticNode::I64(-128)), i8::min_value());

        assert_to_value!(Value::Static(StaticNode::I64(1)), 1_i16);
        assert_to_value!(Value::Static(StaticNode::I64(32767)), i16::max_value());
        assert_to_value!(Value::Static(StaticNode::I64(-32768)), i16::min_value());
        assert_to_value!(Value::Static(StaticNode::I64(1)), 1_i32);
        assert_to_value!(
            Value::Static(StaticNode::I64(2_147_483_647)),
            i32::max_value()
        );
        assert_to_value!(
            Value::Static(StaticNode::I64(-2_147_483_648)),
            i32::min_value()
        );
        assert_to_value!(Value::Static(StaticNode::I64(1)), 1_i64);
        assert_to_value!(
            Value::Static(StaticNode::I64(9_223_372_036_854_775_807)),
            i64::max_value()
        );
        assert_to_value!(
            Value::Static(StaticNode::I64(-9_223_372_036_854_775_808)),
            i64::min_value()
        );

        // unsigned
        assert_to_value!(Value::Static(StaticNode::U64(1)), 1_u8);
        assert_to_value!(Value::Static(StaticNode::U64(255)), u8::max_value());
        assert_to_value!(Value::Static(StaticNode::U64(0)), u8::min_value());

        assert_to_value!(Value::Static(StaticNode::U64(1)), 1_u16);
        assert_to_value!(Value::Static(StaticNode::U64(65535)), u16::max_value());
        assert_to_value!(Value::Static(StaticNode::U64(0)), u16::min_value());

        assert_to_value!(Value::Static(StaticNode::U64(1)), 1_u32);
        assert_to_value!(
            Value::Static(StaticNode::U64(4_294_967_295)),
            u32::max_value()
        );
        assert_to_value!(Value::Static(StaticNode::U64(0)), u32::min_value());

        assert_to_value!(Value::Static(StaticNode::U64(1)), 1_u64);
        assert_to_value!(
            Value::Static(StaticNode::U64(18_446_744_073_709_551_615)),
            u64::max_value()
        );
        assert_to_value!(Value::Static(StaticNode::U64(0)), u64::min_value());

        assert_to_value!(Value::Static(StaticNode::Bool(true)), true);
        assert_to_value!(Value::Static(StaticNode::Bool(false)), false);

        assert_eq!(Value::Static(StaticNode::F64(0.5)), to_value(0.5_f32)?);
        assert_eq!(Value::Static(StaticNode::F64(0.5)), to_value(0.5_f64)?);

        assert_eq!(Value::String("a".into()), to_value('a')?);

        Ok(())
    }

    #[derive(Serialize, Clone)]
    struct NestedStruct {
        key: String,
        number: Option<i8>,
        tuple: (String, bool),
    }

    #[test]
    fn serialize_option() -> Result<()> {
        let mut x: Option<(NestedStruct, usize)> = None;
        assert_eq!(Value::Static(StaticNode::Null), to_value(x)?);
        x = Some((
            NestedStruct {
                key: "key".to_string(),
                number: None,
                tuple: (String::new(), false),
            },
            3,
        ));
        let res = to_value(x.clone())?;
        let elems = res
            .as_array()
            .ok_or("tuple in option not serialized correctly to array")?;
        let values = elems
            .first()
            .ok_or("struct in tuple in option not serialized correctly")?;
        let key = values
            .get("key")
            .ok_or_else(|| Error::Serde("struct fields not serialized correctly".to_string()))?;

        assert_eq!(Some("key"), key.as_str());
        assert!(values.get("number").is_some_and(TypedScalarValue::is_null));
        let array = values
            .get_array("tuple")
            .ok_or("Tuple in struct not correctly serialized")?;
        assert_eq!(Value::String("".into()), array.first().unwrap());
        assert_eq!(
            Value::Static(StaticNode::Bool(false)),
            array.get(1).unwrap()
        );
        assert_eq!(Value::Static(StaticNode::U64(3)), elems.get(1).unwrap());

        // assert it is the same without the option wrapped around it
        let res = to_value(x.unwrap())?;
        let elems = res
            .as_array()
            .ok_or("tuple in option not serialized correctly to array")?;
        let values = elems
            .first()
            .ok_or("struct in tuple in option not serialized correctly")?;
        let key = values
            .get("key")
            .ok_or_else(|| Error::Serde("struct fields not serialized correctly".to_string()))?;

        assert_eq!(Some("key"), key.as_str());
        assert!(values.get("number").is_some_and(TypedScalarValue::is_null));

        let array = values
            .get_array("tuple")
            .ok_or("Tuple in struct not correctly serialized")?;
        assert_eq!(Value::String("".into()), array.first().unwrap());
        assert_eq!(
            Value::Static(StaticNode::Bool(false)),
            array.get(1).unwrap()
        );

        Ok(())
    }

    #[test]
    fn serialize_unit_struct() -> Result<()> {
        #[derive(Serialize)]
        struct UnitStruct(u8);

        assert_eq!(
            Value::Static(StaticNode::U64(1)),
            to_value(UnitStruct(1_u8))?
        );
        Ok(())
    }

    #[test]
    fn serialize_tuple() -> Result<()> {
        #[derive(Serialize)]
        struct UnitStruct;
        #[derive(Serialize)]
        struct TupleStruct(UnitStruct, String);

        assert_eq!(Value::Static(StaticNode::Null), to_value(UnitStruct)?);

        let t = (UnitStruct, TupleStruct(UnitStruct, "ABC".to_string()));
        let values = to_value(t)?;
        assert_eq!(2, values.as_array().unwrap().len());
        assert!(values.get_idx(0).unwrap().is_null());
        let vec = values.get_idx(1).unwrap();
        assert!(vec.get_idx(0).unwrap().is_null());
        assert_eq!(Some("ABC"), vec.get_idx(1).and_then(ValueAsScalar::as_str));

        Ok(())
    }

    #[test]
    fn serialize_map() -> Result<()> {
        let mut map: std::collections::HashMap<String, Vec<f64>> =
            std::collections::HashMap::with_capacity(2);
        map.insert("k".to_string(), vec![1.0, 0.5, -23.123]);
        map.insert("snot".to_string(), vec![]);

        let value_map = to_value(map)?;
        match value_map {
            Value::Object(kvs) => match kvs.get("k").unwrap() {
                Value::Array(arr) => {
                    assert_eq!(3, arr.len());
                    assert_eq!(Value::Static(StaticNode::F64(1.0)), arr.first().unwrap());
                    assert_eq!(Value::Static(StaticNode::F64(0.5)), arr.get(1).unwrap());
                    assert_eq!(Value::Static(StaticNode::F64(-23.123)), arr.get(2).unwrap());
                }
                _ => fail!(
                    "Failed to serialize array in map, got {:?}",
                    kvs.get("k").unwrap()
                ),
            },
            _ => fail!("Failed to serialize map, got {:?}", value_map),
        }
        let empty: std::collections::HashMap<String, Vec<f64>> =
            std::collections::HashMap::with_capacity(0);
        if let Value::Object(kvs) = to_value(empty.clone())? {
            assert_eq!(0, kvs.len());
        } else {
            fail!("Failed to serialize empty map. Got {:?}", to_value(empty)?);
        }
        Ok(())
    }

    #[test]
    fn serialize_seq() -> Result<()> {
        let vec = vec![Some("bla"), Some(""), None];
        let v = to_value(vec)?;
        match v {
            Value::Array(elems) => {
                assert_eq!(3, elems.len());
                assert_eq!(Value::String("bla".into()), elems.first().unwrap());
                assert_eq!(Value::String("".into()), elems.get(1).unwrap());
                assert_eq!(Value::Static(StaticNode::Null), elems.get(2).unwrap());
            }
            _ => fail!("Vec not properly serialized"),
        }
        Ok(())
    }

    #[test]
    fn serialize_map_no_string_keys() {
        let mut map = std::collections::HashMap::with_capacity(2);
        map.insert(1_u8, "foo");
        match to_value(map) {
            Err(e) => assert_eq!("Key must be a String.".to_string(), e.to_string()),
            other => fail!("Did not fail for non-string map keys, got: {:?}", other),
        }
    }

    /*
    not working until rust has specialization

       #[test]
       fn serialize_bytes() -> Result<()> {
           let bytes = vec![1_u8, 1_u8, 1_u8];
           if let Value::Bytes(serialized) = to_value(bytes.as_slice())? {
               assert_eq!(bytes, serialized.to_owned());
           } else {
               assert!(
                   false,
                   "&[u8] not serialized as Bytes but as {:?}",
                   to_value(bytes.as_slice())?
               );
           }

           let some_bytes = Some(bytes.as_slice());
           if let Value::Bytes(serialized) = to_value(&some_bytes)? {
               assert_eq!(bytes, serialized.to_owned());
           } else {
               assert!(
                   false,
                   "Option<&[u8]> not serialized as Bytes but as {:?}",
                   to_value(&some_bytes)?
               );
           }
           Ok(())
       }
    */

    #[test]
    fn serialize_value_bytes() {
        let bytes = Value::Bytes(Cow::borrowed(&[1, 2, 3]));
        let serialized = bytes.serialize(Serializer::default());
        assert!(serialized.is_ok());

        // stupidly asserting that it remains the same
        assert_eq!(bytes, serialized.unwrap());
    }
}
