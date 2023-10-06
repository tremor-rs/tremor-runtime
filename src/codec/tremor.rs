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

//! The Tremor codec is meant for high performance interfacing between tremor instances.
//!
//! The codec is a binary format not meant to be

use std::{
    fmt,
    io::{Cursor, Write},
};

use super::prelude::*;
use beef::Cow;
use byteorder::{ByteOrder, NetworkEndian, ReadBytesExt, WriteBytesExt};
use simd_json::StaticNode;

/// Tremor to Tremor codec
#[derive(Clone, Default, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Tremor {
    buf: Vec<u8>,
}

type DefaultByteOrder = NetworkEndian;

/// Tremor codec error
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Error {
    /// Invalid data
    InvalidData,
    /// Invalid version
    InvalidVersion,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidData => write!(f, "Invalid data"),
            Self::InvalidVersion => write!(f, "Invalid version"),
        }
    }
}
impl std::error::Error for Error {}

impl Tremor {
    const NULL: u8 = 0;
    const BOOL_TRUE: u8 = 1;
    const BOOL_FALSE: u8 = 2;
    const I64: u8 = 3;
    const U64: u8 = 4;
    const F64: u8 = 5;
    const STRING: u8 = 6;
    const BYTES: u8 = 7;
    const ARRAY: u8 = 8;
    const OBJECT: u8 = 9;
    const VERSION: u16 = 0;
    fn decode(data: &[u8]) -> Result<Value> {
        let mut cursor = Cursor::new(data);
        let version = cursor.read_u16::<NetworkEndian>()?;
        match version {
            0 => Self::decode_0::<DefaultByteOrder>(data.get(2..).ok_or(Error::InvalidData)?)
                .map(|v| v.0),
            _ => Err(Error::InvalidVersion.into()),
        }
    }
    fn encode(value: &Value, w: &mut impl Write) -> Result<()> {
        w.write_u16::<NetworkEndian>(Self::VERSION)?;
        Self::encode_::<DefaultByteOrder>(value, w)
    }
    #[inline]
    #[allow(clippy::cast_possible_truncation)]
    fn read_len<E: ByteOrder>(_: u8, data: &[u8]) -> Result<(usize, usize)> {
        let mut cursor = Cursor::new(data);
        let len = cursor.read_u64::<E>()? as usize;
        Ok((len, 8))
    }
    #[inline]
    fn decode_string<E: ByteOrder>(t: u8, data: &[u8]) -> Result<(Value, usize)> {
        let (len, read) = Self::read_len::<E>(t, data)?;
        let s = unsafe {
            std::str::from_utf8_unchecked(data.get(read..read + len).ok_or(Error::InvalidData)?)
        };
        Ok((Value::String(s.into()), 1 + read + len))
    }
    #[inline]
    fn decode_bytes<E: ByteOrder>(t: u8, data: &[u8]) -> Result<(Value, usize)> {
        let (len, read) = Self::read_len::<E>(t, data)?;
        Ok((
            Value::Bytes(data.get(read..read + len).ok_or(Error::InvalidData)?.into()),
            1 + read + len,
        ))
    }
    #[inline]
    fn decode_array<E: ByteOrder>(t: u8, mut data: &[u8]) -> Result<(Value, usize)> {
        let (len, mut total) = Self::read_len::<E>(t, data)?;
        //ALLOW: `total` is the data we've already read so we know they exist
        data = unsafe { data.get_unchecked(total..) };
        let mut a = Vec::with_capacity(len);
        for _ in 0..len {
            let (v, read) = Self::decode_0::<E>(data)?;
            total += read;
            //ALLOW: `read` is the data we've already read so we know they exist
            data = unsafe { data.get_unchecked(read..) };
            a.push(v);
        }
        Ok((Value::Array(a), 1 + total))
    }
    #[inline]
    #[allow(clippy::cast_possible_truncation)]
    fn decode_object<E: ByteOrder>(t: u8, mut data: &[u8]) -> Result<(Value, usize)> {
        let (len, mut total) = Self::read_len::<E>(t, data)?;
        //ALLOW: `total` is the data we've already read so we know they exist
        data = unsafe { data.get_unchecked(total..) };
        let mut v = Value::object_with_capacity(len);
        let o = match v.as_object_mut() {
            Some(o) => o,
            // ALLOW: We knowm tis is an object
            None => unreachable!(),
        };
        for _ in 0..len {
            let mut cursor = Cursor::new(data);
            let len = cursor.read_u64::<E>()? as usize;

            let k = unsafe {
                std::str::from_utf8_unchecked(data.get(8..8 + len).ok_or(Error::InvalidData)?)
            };
            total += 8 + len;
            //ALLOW: `read + len + 1` is the data we've already read so we know they exist
            data = unsafe { data.get_unchecked((8 + len)..) };
            let (v, read) = Self::decode_0::<E>(data)?;
            total += read;
            //ALLOW: `read` is the data we've already read so we know they exist
            data = unsafe { data.get_unchecked(read..) };
            o.insert_nocheck(Cow::from(k), v);
        }
        Ok((v, 1 + total))
    }
    #[inline]
    #[allow(clippy::cast_possible_truncation)]
    fn decode_0<E: ByteOrder>(data: &[u8]) -> Result<(Value, usize)> {
        let (t, data) = data.split_first().ok_or(Error::InvalidData)?;
        let t = *t;
        match t {
            Self::NULL => Ok((Value::const_null(), 1)),
            Self::BOOL_TRUE => Ok((Value::const_true(), 1)),
            Self::BOOL_FALSE => Ok((Value::const_false(), 1)),
            Self::I64 => {
                let mut cursor = Cursor::new(data);
                Ok((Value::Static(StaticNode::I64(cursor.read_i64::<E>()?)), 9))
            }
            Self::U64 => {
                let mut cursor = Cursor::new(data);
                Ok((Value::Static(StaticNode::U64(cursor.read_u64::<E>()?)), 9))
            }
            Self::F64 => {
                let mut cursor = Cursor::new(data);
                Ok((Value::Static(StaticNode::F64(cursor.read_f64::<E>()?)), 9))
            }
            Self::STRING => Self::decode_string::<E>(t, data),
            Self::BYTES => Self::decode_bytes::<E>(t, data),
            Self::ARRAY => Self::decode_array::<E>(t, data),
            Self::OBJECT => Self::decode_object::<E>(t, data),
            _ => Err(Error::InvalidData.into()),
        }
    }
    #[inline]
    fn write_static<E: ByteOrder>(s: &StaticNode, w: &mut impl Write) -> Result<()> {
        match s {
            StaticNode::I64(n) => {
                w.write_u8(Self::I64)?;
                w.write_i64::<E>(*n)?;
            }
            StaticNode::U64(n) => {
                w.write_u8(Self::U64)?;
                w.write_u64::<E>(*n)?;
            }
            StaticNode::F64(n) => {
                w.write_u8(Self::F64)?;
                w.write_f64::<E>(*n)?;
            }
            StaticNode::Bool(true) => w.write_u8(Self::BOOL_TRUE)?,
            StaticNode::Bool(false) => w.write_u8(Self::BOOL_FALSE)?,
            StaticNode::Null => w.write_u8(Self::NULL)?,
        }
        Ok(())
    }
    #[inline]
    #[allow(clippy::cast_possible_truncation)]
    fn write_type_and_len<E: ByteOrder>(t: u8, len: usize, w: &mut impl Write) -> Result<()> {
        w.write_u8(t)?;
        w.write_u64::<E>(len as u64)?;
        Ok(())
    }
    #[inline]
    #[allow(clippy::cast_possible_truncation)]
    fn write_string<E: ByteOrder>(s: &str, w: &mut impl Write) -> Result<()> {
        Self::write_type_and_len::<E>(Self::STRING, s.len(), w)?;
        w.write_all(s.as_bytes())?;
        Ok(())
    }
    #[inline]
    #[allow(clippy::cast_possible_truncation)]
    fn write_bytes<E: ByteOrder>(b: &[u8], w: &mut impl Write) -> Result<()> {
        Self::write_type_and_len::<E>(Self::BYTES, b.len(), w)?;
        w.write_all(b)?;
        Ok(())
    }
    #[inline]
    #[allow(clippy::cast_possible_truncation)]
    fn write_array<E: ByteOrder>(a: &[Value], w: &mut impl Write) -> Result<()> {
        Self::write_type_and_len::<E>(Self::ARRAY, a.len(), w)?;
        for v in a {
            Tremor::encode_::<E>(v, w)?;
        }
        Ok(())
    }
    #[inline]
    fn write_object<E: ByteOrder>(o: &Object, w: &mut impl Write) -> Result<()> {
        Self::write_type_and_len::<E>(Self::OBJECT, o.len(), w)?;
        for (k, v) in o {
            w.write_u64::<E>(k.len() as u64)?;
            w.write_all(k.as_bytes())?;
            Tremor::encode_::<E>(v, w)?;
        }
        Ok(())
    }
    #[inline]
    fn encode_<E: ByteOrder>(value: &Value, w: &mut impl Write) -> Result<()> {
        match value {
            Value::Static(s) => Tremor::write_static::<E>(s, w),
            Value::String(s) => Self::write_string::<E>(s, w),
            Value::Array(a) => Self::write_array::<E>(a, w),
            Value::Object(o) => Self::write_object::<E>(o.as_ref(), w),
            Value::Bytes(b) => Self::write_bytes::<E>(b, w),
        }
    }
}

impl Codec for Tremor {
    fn name(&self) -> &str {
        "tremor"
    }

    fn mime_types(&self) -> Vec<&'static str> {
        vec!["application/octet-stream"]
    }

    fn decode<'input>(
        &mut self,
        data: &'input mut [u8],
        _ingest_ns: u64,
    ) -> Result<Option<Value<'input>>> {
        Tremor::decode(data).map(Some)
    }

    fn encode(&mut self, data: &Value) -> Result<Vec<u8>> {
        Self::encode(data, &mut self.buf)?;
        let res = self.buf.clone();
        self.buf.clear();
        Ok(res)
    }

    fn boxed_clone(&self) -> Box<dyn Codec> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tremor_value::literal;

    #[test]
    fn test_null() -> Result<()> {
        let mut v: Vec<u8> = Vec::new();
        Tremor::encode(&Value::const_null(), &mut v)?;
        let val = Tremor::decode(&v)?;
        assert_eq!(val, Value::const_null());
        Ok(())
    }
    #[test]
    fn test_bool() -> Result<()> {
        let mut v: Vec<u8> = Vec::new();
        Tremor::encode(&Value::const_true(), &mut v)?;
        let val = Tremor::decode(&v)?;
        assert_eq!(val, Value::const_true());

        let mut v: Vec<u8> = Vec::new();
        Tremor::encode(&Value::const_false(), &mut v)?;
        let val = Tremor::decode(&v)?;
        assert_eq!(val, Value::const_false());
        Ok(())
    }
    #[test]
    fn test_i64() -> Result<()> {
        let mut v: Vec<u8> = Vec::new();
        let forty_two = Value::from(-42_i64);
        Tremor::encode(&forty_two, &mut v)?;
        let val = Tremor::decode(&v)?;
        assert_eq!(val, forty_two);
        Ok(())
    }

    #[test]
    fn test_u64() -> Result<()> {
        let mut v: Vec<u8> = Vec::new();
        let forty_two = Value::from(42_u64);
        Tremor::encode(&forty_two, &mut v)?;
        let val = Tremor::decode(&v)?;
        assert_eq!(val, forty_two);

        let mut v: Vec<u8> = Vec::new();
        let big = Value::from(9_223_372_036_854_775_808_u64);
        Tremor::encode(&big, &mut v)?;
        let val = Tremor::decode(&v)?;
        assert_eq!(val, big);

        Ok(())
    }
    #[test]
    fn test_f64() -> Result<()> {
        let mut v: Vec<u8> = Vec::new();
        let forty_two = Value::from(42_f64);
        Tremor::encode(&forty_two, &mut v)?;
        let val = Tremor::decode(&v)?;
        assert_eq!(val, forty_two);
        Ok(())
    }
    #[test]
    fn test_string() -> Result<()> {
        let mut v: Vec<u8> = Vec::new();
        let forty_two = Value::from("42 🦡");
        Tremor::encode(&forty_two, &mut v)?;
        let val = Tremor::decode(&v)?;
        assert_eq!(val, forty_two);
        Ok(())
    }
    #[test]
    fn test_bytes() -> Result<()> {
        let mut v: Vec<u8> = Vec::new();
        let forty_two_tree_four = Value::Bytes(vec![42_u8, 43, 44].into());
        Tremor::encode(&forty_two_tree_four, &mut v)?;
        let val = Tremor::decode(&v)?;
        assert_eq!(val, forty_two_tree_four);
        Ok(())
    }
    #[test]
    fn test_array() -> Result<()> {
        let mut v: Vec<u8> = Vec::new();
        let forty_two_tree_four = Value::Array(vec![
            Value::from(42_u64),
            Value::from(43_u64),
            Value::from(44_u64),
        ]);
        Tremor::encode(&forty_two_tree_four, &mut v)?;
        let val = Tremor::decode(&v)?;
        assert_eq!(val, forty_two_tree_four);
        Ok(())
    }
    #[test]
    fn test_ojbect() -> Result<()> {
        let mut v: Vec<u8> = Vec::new();
        let forty_two_tree_four = literal!({
            "a": 42,
            "b": 43,
            "c": 44,
        });
        Tremor::encode(&forty_two_tree_four, &mut v)?;
        let val = Tremor::decode(&v)?;
        assert_eq!(val, forty_two_tree_four);
        Ok(())
    }

    use proptest::prelude::*;

    // ALLOW: This is a test
    #[allow(clippy::arc_with_non_send_sync)]
    fn arb_tremor_value() -> BoxedStrategy<Value<'static>> {
        let leaf = prop_oneof![
            Just(Value::Static(StaticNode::Null)),
            any::<bool>()
                .prop_map(StaticNode::Bool)
                .prop_map(Value::Static),
            any::<i64>()
                .prop_map(StaticNode::I64)
                .prop_map(Value::Static),
            any::<u64>()
                .prop_map(StaticNode::U64)
                .prop_map(Value::Static),
            any::<Vec<u8>>().prop_map(Cow::from).prop_map(Value::Bytes),
            any::<f64>()
                .prop_map(StaticNode::F64)
                .prop_map(Value::Static),
            ".*".prop_map(Value::from),
        ];
        leaf.prop_recursive(
            8,   // 8 levels deep
            256, // Shoot for maximum size of 256 nodes
            10,  // We put up to 10 items per collection
            |inner| {
                prop_oneof![
                    // Take the inner strategy and make the two recursive cases.
                    prop::collection::vec(inner.clone(), 0..10).prop_map(Value::Array),
                    prop::collection::hash_map(".*".prop_map(Cow::from), inner, 0..10)
                        .prop_map(|m| m.into_iter().collect()),
                ]
            },
        )
        .boxed()
    }
    proptest! {
        #[test]
        #[allow(clippy::ignored_unit_patterns)]

        fn prop_round_trip(v1 in arb_tremor_value()) {
            let mut v: Vec<u8> = Vec::new();
            Tremor::encode(&v1, &mut v).expect("failed to encode");
            let v2 = Tremor::decode(&v).expect("failed to decode");
            assert_eq!(v1, v2);
        }
    }
}
