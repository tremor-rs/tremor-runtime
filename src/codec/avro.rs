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

//! The `avro` codec supports Apache Avro binary encoding.
//!
//! The codec is configured with a codec following the avro json codec specification
//!
//! ## Mappings
//!
//! | avro | tremor (to) | tremor (from) |
//! | null | null | null |
//! | boolean | bool | bool |
//! | int | i64 | i64, u64|
//! | long | i64 | i64, u64 |
//! | float | f64 | f64 |
//! | double | f64 | f64 |
//! | bytes | bytes | bytes, string |
//! | string | string | string |
//! | fixed | bytes | bytes |
//! | enum | string | string |
//! | union | value | value |
//! | array | array | array |
//! | map | record | record |
//! | record | record | record |
//! | date | i64 | i64 |
//! | decimal | bytes | bytes |
//! | time-millis | i64 | i64, u64 |
//! | time-micros | i64 | i64, u64 |
//! | timestamp-millis | i64 | i64, u64 |
//! | timestamp-micros | i64 | i64, u64 |
//! | duration | bytes[12] | bytes[12] |

use std::collections::HashMap;

use super::prelude::*;
use apache_avro::{
    schema::Name, types::Value as AvroValue, Codec as Compression, Decimal, Duration, Reader,
    Schema, Writer,
};
use serde::Deserialize;
use value_trait::TryTypeError;

const AVRO_BUFFER_CAP: usize = 512;

#[derive(Clone, Debug, Default)]
struct AvroRegistry {
    by_name: HashMap<Name, Schema>,
    by_id: HashMap<u32, Schema>,
}

impl AvroRegistry {
    #[allow(dead_code, clippy::unused_self)] // FIXME: use this
    fn get_schema_by_id(&self, id: u32) -> Option<&Schema> {
        self.by_id.get(&id)
    }
    fn get_schema_by_name(&self, name: &Name) -> Option<&Schema> {
        self.by_name.get(name)
    }
}

#[derive(Clone, Debug)]
pub struct Avro {
    schema: Schema,
    registry: AvroRegistry,
    compression: Compression,
}

impl Avro {
    pub(crate) fn from_config(config: Option<&Value>) -> Result<Box<dyn Codec>> {
        let compression = match config.get_str("compression") {
            Some("deflate") => Compression::Deflate,
            Some("snappy") => Compression::Snappy,
            Some("zstd") => Compression::Zstandard,
            Some("bzip2") => Compression::Bzip2,
            Some("xz") => Compression::Xz,
            None | Some("none") => Compression::Null,
            Some(c) => return Err(format!("Unknown compression codec: {c}").into()),
        };

        let mut registry = AvroRegistry::default();
        match config.get("schema") {
            Some(schema) => {
                let schema = Schema::parse_str(&schema.encode())?;
                if let Some(name) = schema.name().cloned() {
                    registry.by_name.insert(name, schema.clone());
                }
                Ok(Box::new(Avro {
                    schema,
                    registry,
                    compression,
                }))
            }
            None => Err("Missing avro schema".into()),
        }
    }
    #[allow(clippy::too_many_lines)]
    fn to_avro_value(&self, data: &Value, schema: &Schema) -> Result<AvroValue> {
        Ok(match schema {
            Schema::Null => {
                let got = data.value_type();
                if got == ValueType::Null {
                    AvroValue::Null
                } else {
                    return Err(TryTypeError {
                        expected: ValueType::Null,
                        got,
                    }
                    .into());
                }
            }
            Schema::Boolean => AvroValue::Boolean(data.try_as_bool()?),
            Schema::Int => AvroValue::Int(data.try_as_i32()?),
            Schema::Long => AvroValue::Long(data.try_as_i64()?),
            Schema::Float => AvroValue::Float(data.try_as_f32()?),
            Schema::Double => AvroValue::Double(data.try_as_f64()?),
            Schema::Bytes => AvroValue::Bytes(data.try_as_bytes()?.to_vec()),
            Schema::String => AvroValue::String(data.try_as_str()?.to_string()),
            Schema::Array(s) => AvroValue::Array(
                data.try_as_array()?
                    .iter()
                    .map(|d| self.to_avro_value(d, s))
                    .collect::<Result<_>>()?,
            ),
            Schema::Map(s) => AvroValue::Map(
                data.try_as_object()?
                    .iter()
                    .map(|(k, v)| Ok((k.to_string(), self.to_avro_value(v, s)?)))
                    .collect::<Result<_>>()?,
            ),
            Schema::Union(s) => {
                for (i, variant) in s.variants().iter().enumerate() {
                    if let Ok(v) = self.to_avro_value(data, variant) {
                        return Ok(AvroValue::Union(u32::try_from(i)?, Box::new(v)));
                    }
                }
                return Err(format!("No variant matched for {}", data.value_type()).into());
            }
            Schema::Record(r) => {
                let mut res: Vec<(String, AvroValue)> = Vec::with_capacity(r.fields.len());
                for f in &r.fields {
                    let d = data.get(f.name.as_str());

                    if d.is_none() && f.default.is_some() {
                        // from_value(f.default.clone().ok_or("unreachable")?)?;
                        let val =
                            Value::<'static>::deserialize(f.default.clone().ok_or("unreachable")?)
                                .map_err(|e| format!("Failed to deserialize default value: {e}"))?;
                        res.push((f.name.clone(), self.to_avro_value(&val, &f.schema)?));
                        continue;
                    } else if d.is_none() && f.is_nullable() {
                        res.push((f.name.clone(), AvroValue::Null));
                    } else if let Some(d) = d {
                        res.push((f.name.clone(), self.to_avro_value(d, &f.schema)?));
                    } else {
                        return Err(format!("Missing field {}", f.name).into());
                    }
                }
                AvroValue::Record(res)
            }
            Schema::Enum(e) => {
                let this = data.try_as_str()?;
                for (i, variant) in e.symbols.iter().enumerate() {
                    if variant == this {
                        return Ok(AvroValue::Enum(u32::try_from(i)?, variant.clone()));
                    }
                }
                return Err(format!("No variant matched for {this}").into());
            }
            Schema::Fixed(f) => {
                // TODO: possibly allow other types here
                let b = data.try_as_bytes()?;
                if b.len() != f.size {
                    return Err(format!(
                        "Invalid size for fixed type, expected {} got {}",
                        f.size,
                        b.len()
                    )
                    .into());
                }
                AvroValue::Fixed(b.len(), b.to_vec())
            }
            Schema::Decimal(_s) => {
                // TODO: possibly allow other types here
                let d = data.try_as_bytes()?;
                let d = Decimal::try_from(d).map_err(|e| format!("Invalid decimal: {e}"))?;
                AvroValue::Decimal(d)
            }
            Schema::Uuid => AvroValue::Uuid(data.try_as_str()?.parse()?), // TODO: allow bytes and eventually 128 bit numbers
            Schema::Date => AvroValue::Date(data.try_as_i32()?), // TODO: allow strings and other date types?
            Schema::TimeMillis => AvroValue::TimeMillis(data.try_as_i32()?),
            Schema::TimeMicros => AvroValue::TimeMicros(data.try_as_i64()?),
            Schema::TimestampMillis => AvroValue::TimestampMillis(data.try_as_i64()?),
            Schema::TimestampMicros => AvroValue::TimestampMicros(data.try_as_i64()?),
            Schema::Duration => {
                let v: [u8; 12] = data
                    .as_bytes()
                    .and_then(|v| v.try_into().ok())
                    .ok_or("Invalid duration")?;

                AvroValue::Duration(Duration::from(v))
            }
            Schema::Ref { name } => {
                let schema = self.registry.get_schema_by_name(name).ok_or_else(|| {
                    format!("Schema refferences are not supported, asking for {name}")
                })?;
                self.to_avro_value(data, schema)?
            }
        })
    }
    fn convert_avro_value(val: AvroValue) -> Result<Value<'static>> {
        Ok(match val {
            AvroValue::Null => Value::const_null(),
            AvroValue::Boolean(v) => Value::from(v),
            AvroValue::Int(v) | AvroValue::TimeMillis(v) | AvroValue::Date(v) => Value::from(v),
            AvroValue::Long(v)
            | AvroValue::TimestampMicros(v)
            | AvroValue::TimestampMillis(v)
            | AvroValue::TimeMicros(v) => Value::from(v),
            AvroValue::Float(v) => Value::from(v),
            AvroValue::Double(v) => Value::from(v),
            AvroValue::Bytes(v) | AvroValue::Fixed(_, v) => Value::Bytes(v.into()),
            AvroValue::String(v) | AvroValue::Enum(_, v) => Value::from(v),
            AvroValue::Union(_, v) => Self::convert_avro_value(*v)?,
            AvroValue::Array(v) => Value::Array(
                v.into_iter()
                    .map(Self::convert_avro_value)
                    .collect::<Result<_>>()?,
            ),
            AvroValue::Map(v) => Value::from(
                v.into_iter()
                    .map(|(k, v)| Ok((k.into(), Self::convert_avro_value(v)?)))
                    .collect::<Result<Object>>()?,
            ),
            AvroValue::Record(r) => Value::from(
                r.into_iter()
                    .map(|(k, v)| Ok((k.into(), Self::convert_avro_value(v)?)))
                    .collect::<Result<Object>>()?,
            ),
            AvroValue::Decimal(v) => {
                let d = <Vec<u8>>::try_from(&v)?;
                Value::Bytes(d.into())
            }
            AvroValue::Duration(v) => {
                let d: [u8; 12] = v.into();
                Value::Bytes(d.to_vec().into())
            }
            AvroValue::Uuid(v) => Value::from(v.to_string()),
        })
    }
    fn write_value<'a>(&self, data: &'a Value, writer: &mut Writer<'a, Vec<u8>>) -> Result<()> {
        let v = self.to_avro_value(data, writer.schema())?;
        writer.append(v)?;

        Ok(())
    }
}

impl Codec for Avro {
    fn name(&self) -> &str {
        "avro"
    }

    fn mime_types(&self) -> Vec<&'static str> {
        vec!["application/vnd.apache.avro+binary"]
        // TODO: application/json-seq for one json doc per line?
    }

    fn decode<'input>(
        &mut self,
        data: &'input mut [u8],
        _ingest_ns: u64,
        meta: Value<'input>,
    ) -> Result<Option<(Value<'input>, Value<'input>)>> {
        let reader = Reader::new(&*data)?;

        let mut vals = reader.map(|v| Self::convert_avro_value(v?));
        vals.next().map(|v| v.map(|v| (v, meta))).transpose()
    }

    fn encode(&mut self, data: &Value) -> Result<Vec<u8>> {
        let mut writer = Writer::with_codec(
            &self.schema,
            Vec::with_capacity(AVRO_BUFFER_CAP),
            self.compression,
        );
        self.write_value(data, &mut writer)?;

        writer.into_inner().map_err(Error::from)
    }

    fn boxed_clone(&self) -> Box<dyn Codec> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use simd_json_derive::Serialize;
    use tremor_value::literal;

    #[derive(Debug, Deserialize, serde::Serialize)]
    struct Test {
        int: i32,
        long: i64,
        string: String,
    }

    fn test_schema() -> Value<'static> {
        literal!(
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "int", "type": "int", "default": 42},
                    {"name": "long", "type": "long", "default": 42},
                    {"name": "string", "type": "string"}
                ]
            }
        )
    }

    fn test_codec(schema: Value<'static>) -> Result<Box<dyn Codec>> {
        Avro::from_config(Some(&literal!({
            "schema": schema,
            "compression": "none",
        })))
    }
    #[test]
    fn encode() -> Result<()> {
        let mut codec = test_codec(test_schema())?;
        let decoded = literal!({ "long": 27, "string": "string" });
        let encoded = dbg!(codec.encode(&decoded));

        assert!(encoded.is_ok());
        Ok(())
    }

    #[test]
    fn null() -> Result<()> {
        let mut codec = test_codec(literal!({"type": "null"}))?;
        let decoded = literal!(());
        let encoded = codec.encode(&decoded);

        assert!(encoded.is_ok());
        Ok(())
    }

    #[test]
    fn boolean() -> Result<()> {
        let mut codec = test_codec(literal!({"type":"boolean"}))?;
        let decoded = literal!(true);
        let encoded = codec.encode(&decoded);

        assert!(encoded.is_ok());
        Ok(())
    }

    #[test]
    fn int() -> Result<()> {
        let mut codec = test_codec(literal!({"type":"int"}))?;
        let decoded = literal!(42i32);
        let encoded = codec.encode(&decoded);

        assert!(encoded.is_ok());
        Ok(())
    }

    #[test]
    fn long() -> Result<()> {
        let mut codec = test_codec(literal!({"type":"long"}))?;
        let decoded = literal!(42);
        let encoded = codec.encode(&decoded);

        assert!(encoded.is_ok());
        Ok(())
    }

    #[test]
    fn float() -> Result<()> {
        let mut codec = test_codec(literal!({"type":"float"}))?;
        let decoded = literal!(42f32);
        let encoded = codec.encode(&decoded);

        assert!(encoded.is_ok());
        Ok(())
    }

    #[test]
    fn double() -> Result<()> {
        let mut codec = test_codec(literal!({"type":"double"}))?;
        let decoded = literal!(42f64);
        let encoded = codec.encode(&decoded);

        assert!(encoded.is_ok());
        Ok(())
    }

    #[test]
    fn bytes() -> Result<()> {
        let mut codec = test_codec(literal!({"type":"bytes"}))?;
        let decoded = Value::Bytes(vec![1, 2, 3].into());
        let encoded = codec.encode(&decoded);

        assert!(encoded.is_ok());
        Ok(())
    }

    #[test]
    fn string() -> Result<()> {
        let mut codec = test_codec(literal!({"type":"string"}))?;
        let decoded = literal!("foo");
        let encoded = codec.encode(&decoded);

        assert!(encoded.is_ok());
        Ok(())
    }

    #[test]
    fn record() -> Result<()> {
        let mut codec = test_codec(literal!({
            "type":"record",
            "name":"test",
            "fields": [{"name": "one", "type": "int"}]
        }))?;
        let decoded = literal!({"one": 1});
        let encoded = codec.encode(&decoded);

        assert!(encoded.is_ok());
        Ok(())
    }

    #[test]
    fn _enum() -> Result<()> {
        let mut codec = test_codec(literal!({
            "type":"enum",
            "name":"test",
            "symbols": ["SNOT", "BADGER"]
        }))?;
        let decoded = literal!("SNOT");
        let encoded = codec.encode(&decoded);

        assert!(encoded.is_ok());
        Ok(())
    }

    #[test]
    fn array() -> Result<()> {
        let mut codec = test_codec(literal!({
            "type":"array",
            "items":"string"
        }))?;
        let decoded = literal!(["SNOT", "BADGER"]);
        let encoded = codec.encode(&decoded);

        assert!(encoded.is_ok());
        Ok(())
    }

    #[test]
    fn map() -> Result<()> {
        let mut codec = test_codec(literal!({
            "type":"map",
            "values":"string"
        }))?;
        let decoded = literal!({"SNOT": "BADGER"});
        let encoded = codec.encode(&decoded);

        assert!(encoded.is_ok());
        Ok(())
    }

    #[test]
    fn decode() -> Result<()> {
        let mut codec = test_codec(test_schema())?;

        let expected = literal!({
            "string": "foo",
            "int": 23,
            "long": 27,
        });

        let schema = Schema::parse_str(&test_schema().json_string()?)?;

        let mut writer = Writer::with_codec(&schema, Vec::new(), Compression::Null);

        let test = Test {
            int: 23,
            long: 27,
            string: "foo".to_owned(),
        };

        writer.append_ser(test)?;
        let mut encoded = writer.into_inner()?;

        let decoded = codec
            .decode(encoded.as_mut_slice(), 0, Value::object())?
            .expect("no data");

        assert_eq!(decoded.0, expected);
        Ok(())
    }
    #[test]
    fn round_robin() -> Result<()> {
        let mut codec = test_codec(literal!(
            {
                "type": "record",
                "name": "record",
                "fields": [
                    {"name": "null", "type": "null"},
                    {"name": "boolean", "type": "boolean"},
                    {"name": "int", "type": "int"},
                    {"name": "long", "type": "long"},
                    {"name": "float", "type": "float"},
                    {"name": "double", "type": "double"},
                    {"name": "bytes", "type": "bytes"},
                    {"name": "string", "type": "string"},
                    {"name": "enum", "type": {
                        "type": "enum",
                        "name": "enumType",
                        "symbols": ["SNOT", "BADGER"]}
                    },
                    {"name": "array", "type": {
                        "type": "array",
                        "items": "string"}
                    },
                    {"name": "map", "type": {
                        "type": "map",
                        "values": "string"}
                    },
                ]
            }
        ))?;
        let decoded = literal!({
            "null": null,
            "boolean": true,
            "int": 27,
            "long": 42,
            "float": 1.0,
            "double": 2.0,
            "bytes": Value::Bytes(vec![1u8, 2, 3].into()),
            "string": "foo",
            "enum": "SNOT",
            "array": ["SNOT", "BADGER"],
            "map": {"SNOT": "BADGER"}
        });
        let mut encoded = codec.encode(&decoded)?;

        let redecoded = codec
            .decode(&mut encoded, 0, Value::object())?
            .expect("no data");
        assert_eq!(decoded, redecoded.0);

        Ok(())
    }
}
