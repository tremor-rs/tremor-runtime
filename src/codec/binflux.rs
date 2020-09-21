// Copyright 2020, The Tremor Team
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

use super::prelude::*;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use simd_json::borrowed::Object;
use std::borrow::Cow;
use std::convert::TryFrom;
use std::io::{Cursor, Write};
use std::str;

const TYPE_I64: u8 = 0;
const TYPE_F64: u8 = 1;
const TYPE_STRING: u8 = 2;
const TYPE_TRUE: u8 = 3;
const TYPE_FALSE: u8 = 4;

#[allow(clippy::module_name_repetitions)]
#[derive(Clone)]
pub struct BInflux {}

impl BInflux {
    pub fn encode(v: &simd_json::BorrowedValue) -> Result<Vec<u8>> {
        fn write_str<W: Write>(w: &mut W, s: &str) -> Result<()> {
            w.write_u16::<BigEndian>(
                u16::try_from(s.len())
                    .map_err(|_| ErrorKind::InvalidBInfluxData("string too long".into()))?,
            )?;
            w.write_all(s.as_bytes())?;
            Ok(())
        }

        let mut res = Vec::with_capacity(512);
        res.write_u16::<BigEndian>(0)?;
        if let Some(measurement) = v.get("measurement").and_then(Value::as_str) {
            write_str(&mut res, measurement)?;
        } else {
            return Err(ErrorKind::InvalidBInfluxData("measurement missing".into()).into());
        }

        if let Some(timestamp) = v.get("timestamp").and_then(Value::as_u64) {
            res.write_u64::<BigEndian>(timestamp)?;
        } else {
            return Err(ErrorKind::InvalidBInfluxData("timestamp missing".into()).into());
        }
        if let Some(tags) = v.get("tags").and_then(Value::as_object) {
            res.write_u16::<BigEndian>(
                u16::try_from(tags.len())
                    .map_err(|_| ErrorKind::InvalidBInfluxData("too many tags".into()))?,
            )?;

            for (k, v) in tags {
                if let Some(v) = v.as_str() {
                    write_str(&mut res, k)?;
                    write_str(&mut res, v)?;
                }
            }
        } else {
            res.write_u16::<BigEndian>(0 as u16)?;
        }

        if let Some(fields) = v.get("fields").and_then(Value::as_object) {
            res.write_u16::<BigEndian>(
                u16::try_from(fields.len())
                    .map_err(|_| ErrorKind::InvalidBInfluxData("too many fields".into()))?,
            )?;
            for (k, v) in fields {
                write_str(&mut res, k)?;
                if let Some(v) = v.as_i64() {
                    res.write_u8(TYPE_I64)?;
                    res.write_i64::<BigEndian>(v)?;
                } else if let Some(v) = v.as_f64() {
                    res.write_u8(TYPE_F64)?;
                    res.write_f64::<BigEndian>(v)?;
                } else if let Some(v) = v.as_bool() {
                    if v {
                        res.write_u8(TYPE_TRUE)?;
                    } else {
                        res.write_u8(TYPE_FALSE)?;
                    }
                } else if let Some(v) = v.as_str() {
                    res.write_u8(TYPE_STRING)?;
                    write_str(&mut res, v)?;
                } else {
                    error!("Unknown type as influx line value: {:?}", v.value_type())
                }
            }
        } else {
            res.write_u16::<BigEndian>(0 as u16)?;
        }
        Ok(res)
    }

    pub fn decode<'event>(data: &'event [u8]) -> Result<Value<'event>> {
        fn read_string<'event>(c: &mut Cursor<&'event [u8]>) -> Result<Cow<'event, str>> {
            let l = c.read_u16::<BigEndian>()? as usize;
            #[allow(clippy::cast_possible_truncation)]
            let p = c.position() as usize;
            c.set_position((p + l) as u64);
            unsafe { Ok(str::from_utf8_unchecked(&c.get_ref()[p..p + l]).into()) }
        };
        let mut c = Cursor::new(data);
        let vsn = c.read_u16::<BigEndian>()?;
        if vsn != 0 {
            return Err(ErrorKind::InvalidBInfluxData("invalid version".into()).into());
        };
        let measurement = Value::from(read_string(&mut c)?);
        let timestamp = Value::from(c.read_u64::<BigEndian>()?);
        let tag_count = c.read_u16::<BigEndian>()? as usize;
        let mut tags = Object::with_capacity(tag_count);
        for _i in 0..tag_count {
            let key = read_string(&mut c)?;
            let value = read_string(&mut c)?;
            tags.insert(key, Value::from(value));
        }
        let field_count = c.read_u16::<BigEndian>()? as usize;
        let mut fields = Object::with_capacity(field_count);
        for _i in 0..field_count {
            let key = read_string(&mut c)?;
            let kind = c.read_u8()?;
            match kind {
                TYPE_I64 => {
                    let value = c.read_i64::<BigEndian>()?;
                    fields.insert(key, Value::from(value));
                }
                TYPE_F64 => {
                    let value = c.read_f64::<BigEndian>()?;
                    fields.insert(key, Value::from(value));
                }
                TYPE_STRING => {
                    let value = read_string(&mut c)?;
                    fields.insert(key, Value::from(value));
                }
                TYPE_TRUE => {
                    fields.insert(key, Value::from(true));
                }
                TYPE_FALSE => {
                    fields.insert(key, Value::from(false));
                }
                o => error!("bad field type: {}", o),
            }
        }
        let mut result = Object::with_capacity(4);
        result.insert("measurement".into(), measurement);
        result.insert("tags".into(), Value::from(tags));
        result.insert("fields".into(), Value::from(fields));
        result.insert("timestamp".into(), timestamp);

        Ok(Value::from(result))
    }
}

impl Codec for BInflux {
    fn name(&self) -> std::string::String {
        "binflux".to_string()
    }

    fn decode<'input>(
        &mut self,
        data: &'input mut [u8],
        _ingest_ns: u64,
    ) -> Result<Option<Value<'input>>> {
        Self::decode(data).map(Some)
    }

    fn encode(&self, data: &simd_json::BorrowedValue) -> Result<Vec<u8>> {
        Self::encode(data)
    }

    fn boxed_clone(&self) -> Box<dyn Codec> {
        Box::new(self.clone())
    }
}
