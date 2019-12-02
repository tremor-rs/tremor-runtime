// Copyright 2018-2019, Wayfair GmbH
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

use super::Codec;
use crate::errors::*;
use simd_json::value::borrowed::{Object, Value};
use simd_json::value::Value as ValueTrait;
use std::str;
use tremor_script::prelude::*;

#[derive(Clone)]
pub struct StatsD {}

impl Codec for StatsD {
    fn decode(&mut self, data: Vec<u8>, ingest_ns: u64) -> Result<Option<LineValue>> {
        LineValue::try_new(vec![data], |raw| {
            decode(&raw[0], ingest_ns).map(ValueAndMeta::from)
        })
        .map_err(|e| e.0)
        .map(Some)
    }

    fn encode(&self, data: &simd_json::BorrowedValue) -> Result<Vec<u8>> {
        encode(data)
    }
}

fn encode(value: &Value) -> Result<Vec<u8>> {
    let mut r = String::new();
    if let Some(m) = value.get("metric").and_then(|v| v.as_str()) {
        r.push_str(&m);
    } else {
        return Err(ErrorKind::InvalidStatsD.into());
    };
    let t = if let Some(s) = value.get("type").and_then(|v| v.as_str()) {
        s
    } else {
        return Err(ErrorKind::InvalidStatsD.into());
    };
    if let Some(val) = value.get("value") {
        r.push(':');
        if t == "g" {
            if let Some(s) = value.get("action").and_then(Value::as_str) {
                match s {
                    "add" => r.push('+'),
                    "sub" => r.push('-'),
                    _ => (),
                }
            }
        };
        if val.is_i64() || val.is_f64() {
            r.push_str(&val.encode());
        } else {
            return Err(ErrorKind::InvalidStatsD.into());
        }
    } else {
        return Err(ErrorKind::InvalidStatsD.into());
    };

    r.push('|');
    r.push_str(&t);

    if let Some(val) = value.get("sample_rate") {
        r.push_str("|@");
        if val.is_i64() || val.is_f64() {
            r.push_str(&val.encode());
        } else {
            return Err(ErrorKind::InvalidStatsD.into());
        }
    }

    Ok(r.as_bytes().to_vec())
}

fn decode<'input>(data: &'input [u8], _ingest_ns: u64) -> Result<Value<'input>> {
    enum Sign {
        Plus,
        Minus,
        None,
    };
    let mut d = data.iter().enumerate().peekable();
    let mut m = Object::with_capacity(4);
    let value_start: usize;
    let mut is_float = false;
    loop {
        match d.next() {
            Some((idx, b':')) => {
                let v = str::from_utf8(&data[0..idx])?;
                value_start = idx + 1;
                m.insert("metric".into(), Value::from(v));
                break;
            }
            Some(_) => (),
            None => return Err(ErrorKind::InvalidStatsD.into()),
        }
    }
    let sign = match d.peek() {
        Some((_, b'+')) => Sign::Plus,
        Some((_, b'-')) => Sign::Minus,
        _ => Sign::None,
    };
    let mut value: Value;
    loop {
        match d.next() {
            Some((_, b'.')) => is_float = true,
            Some((idx, b'|')) => {
                let s = str::from_utf8(&data[value_start..idx])?;
                if is_float {
                    let v: f64 = s.parse()?;
                    value = Value::from(v);
                } else {
                    let v: i64 = s.parse()?;
                    value = Value::from(v);
                };
                break;
            }
            Some(_) => (),
            None => return Err(ErrorKind::InvalidStatsD.into()),
        }
    }
    match d.next() {
        Some((i, b'c')) | Some((i, b'h')) | Some((i, b's')) => {
            m.insert("type".into(), str::from_utf8(&data[i..=i])?.into())
        }
        Some((i, b'm')) => {
            if let Some((j, b's')) = d.next() {
                m.insert("type".into(), str::from_utf8(&data[i..=j])?.into())
            } else {
                return Err(ErrorKind::InvalidStatsD.into());
            }
        }
        Some((i, b'g')) => {
            match sign {
                Sign::Plus => {
                    m.insert("action".into(), "add".into());
                }
                Sign::Minus => {
                    // If it was a `-` we got to negate the number
                    value = if let Some(v) = value.as_i64() {
                        Value::from(-v)
                    } else if let Some(v) = value.as_f64() {
                        Value::from(-v)
                    } else {
                        return Err(ErrorKind::InvalidStatsD.into());
                    };
                    m.insert("action".into(), "sub".into());
                }
                Sign::None => (),
            };
            m.insert("type".into(), str::from_utf8(&data[i..=i])?.into())
        }
        _ => return Err(ErrorKind::InvalidStatsD.into()),
    };
    match d.next() {
        Some((_, b'|')) => {
            if let Some((sample_start, b'@')) = d.next() {
                let s = str::from_utf8(&data[sample_start + 1..])?;
                let v: f64 = s.parse()?;
                m.insert("sample_rate".into(), Value::from(v));
            } else {
                return Err(ErrorKind::InvalidStatsD.into());
            }
        }
        None => (),
        _ => return Err(ErrorKind::InvalidStatsD.into()),
    };
    m.insert("value".into(), value);
    Ok(Value::from(m))
}

#[cfg(test)]
mod test {
    use super::*;
    use simd_json::json;
    // gorets:1|c
    #[test]
    fn gorets() {
        let data = b"gorets:1|c";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected: Value = json!({
            "type": "c",
            "metric": "gorets",
            "value": 1,

        })
        .into();
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }
    // glork:320|ms
    #[test]
    fn glork() {
        let data = b"glork:320|ms";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected: Value = json!({
            "type": "ms",
            "metric": "glork",
            "value": 320,

        })
        .into();
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    // gaugor:333|g
    #[test]
    fn gaugor() {
        let data = b"gaugor:333|g";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected: Value = json!({
            "type": "g",
            "metric": "gaugor",
            "value": 333,

        })
        .into();
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    // uniques:765|s
    #[test]
    fn uniques() {
        let data = b"uniques:765|s";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected: Value = json!({
            "type": "s",
            "metric": "uniques",
            "value": 765,

        })
        .into();
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    #[test]
    fn horst() {
        let data = b"horst:42.23|h";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected: Value = json!({
            "type": "h",
            "metric": "horst",
            "value": 42.23,

        })
        .into();
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    #[test]
    fn sam() {
        let data = b"sam:7|c|@0.1";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected: Value = json!({
            "type": "c",
            "metric": "sam",
            "value": 7,
            "sample_rate": 0.1

        })
        .into();
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    #[test]
    fn addy() {
        let data = b"addy:+123|g";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected: Value = json!({
            "type": "g",
            "metric": "addy",
            "action": "add",
            "value": 123,

        })
        .into();
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    #[test]
    fn subastian() {
        let data = b"subastian:-234|g";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected: Value = json!({
            "type": "g",
            "metric": "subastian",
            "action": "sub",
            "value": 234,

        })
        .into();
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }
}
