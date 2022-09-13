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

use super::prelude::*;
use tremor_common::string::substr;

#[derive(Clone)]
pub struct StatsD {}

impl Codec for StatsD {
    fn name(&self) -> &str {
        "statsd"
    }

    fn decode<'input>(
        &mut self,
        data: &'input mut [u8],
        ingest_ns: u64,
    ) -> Result<Option<Value<'input>>> {
        decode(data, ingest_ns).map(Some)
    }

    fn encode(&self, data: &Value) -> Result<Vec<u8>> {
        encode(data)
    }

    fn boxed_clone(&self) -> Box<dyn Codec> {
        Box::new(self.clone())
    }
}

fn encode(value: &Value) -> Result<Vec<u8>> {
    let mut r = String::new();
    r.push_str(value.get_str("metric").ok_or(ErrorKind::InvalidStatsD)?);
    let t = value.get_str("type").ok_or(ErrorKind::InvalidStatsD)?;
    let val = value.get("value").ok_or(ErrorKind::InvalidStatsD)?;
    if !val.is_number() {
        return Err(ErrorKind::InvalidStatsD.into());
    };
    r.push(':');
    if t == "g" {
        match value.get_str("action") {
            Some("add") => r.push('+'),
            Some("sub") => r.push('-'),
            _ => (),
        }
    };

    r.push_str(&val.encode());
    r.push('|');
    r.push_str(t);

    if let Some(val) = value.get("sample_rate") {
        if val.is_number() {
            r.push_str("|@");
            r.push_str(&val.encode());
        } else {
            return Err(ErrorKind::InvalidStatsD.into());
        }
    }

    Ok(r.as_bytes().to_vec())
}

fn decode(data: &[u8], _ingest_ns: u64) -> Result<Value> {
    enum Sign {
        Plus,
        Minus,
        None,
    }
    let mut d = data.iter().enumerate().peekable();
    let mut m = Object::with_capacity(4);
    let value_start: usize;
    let mut is_float = false;
    loop {
        match d.next() {
            Some((idx, b':')) => {
                let v = substr(data, 0..idx)?;
                value_start = idx + 1;
                m.insert("metric".into(), Value::from(v));
                break;
            }
            Some(_) => (),
            None => return Err(invalid()),
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
                let s = substr(data, value_start..idx)?;
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
            None => return Err(invalid()),
        }
    }
    match d.next() {
        Some((i, b'c' | b'h' | b's')) => m.insert("type".into(), substr(data, i..=i)?.into()),
        Some((i, b'm')) => {
            if let Some((j, b's')) = d.next() {
                m.insert("type".into(), substr(data, i..=j)?.into())
            } else {
                return Err(invalid());
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
                        return Err(invalid());
                    };
                    m.insert("action".into(), "sub".into());
                }
                Sign::None => (),
            };
            m.insert("type".into(), substr(data, i..=i)?.into())
        }
        _ => return Err(invalid()),
    };
    match d.next() {
        Some((_, b'|')) => {
            if let Some((sample_start, b'@')) = d.next() {
                let s = substr(data, sample_start + 1..)?;
                let v: f64 = s.parse()?;
                m.insert("sample_rate".into(), Value::from(v));
            } else {
                return Err(invalid());
            }
        }
        Some(_) => return Err(invalid()),
        None => (),
    };
    m.insert("value".into(), value);
    Ok(Value::from(m))
}

fn invalid() -> Error {
    Error::from(ErrorKind::InvalidStatsD)
}

#[cfg(test)]
mod test {
    use super::*;
    use tremor_value::literal;

    // gorets:1|c
    #[test]
    fn gorets() {
        let data = b"gorets:1|c";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected = literal!({
            "type": "c",
            "metric": "gorets",
            "value": 1,

        });
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded.as_slice(), data);
    }
    // glork:320|ms
    #[test]
    fn glork() {
        let data = b"glork:320|ms";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected = literal!({
            "type": "ms",
            "metric": "glork",
            "value": 320,

        });
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    // gaugor:333|g
    #[test]
    fn gaugor() {
        let data = b"gaugor:333|g";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected = literal!({
            "type": "g",
            "metric": "gaugor",
            "value": 333,

        });
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    // uniques:765|s
    #[test]
    fn uniques() {
        let data = b"uniques:765|s";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected = literal!({
            "type": "s",
            "metric": "uniques",
            "value": 765,

        });
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    #[test]
    fn horst() {
        let mut c = StatsD {};
        let mut data = b"horst:42.23|h".to_vec();

        let parsed = c
            .decode(data.as_mut_slice(), 0)
            .expect("failed to decode")
            .unwrap();
        let expected = literal!({
            "type": "h",
            "metric": "horst",
            "value": 42.23,

        });
        assert_eq!(parsed, expected);
        let encoded = c.encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, b"horst:42.23|h");
    }

    #[test]
    fn sam() {
        let data = b"sam:7|c|@0.1";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected = literal!({
            "type": "c",
            "metric": "sam",
            "value": 7,
            "sample_rate": 0.1

        });
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    #[test]
    fn addy() {
        let data = b"addy:+123|g";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected = literal!({
            "type": "g",
            "metric": "addy",
            "action": "add",
            "value": 123,

        });
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    #[test]
    fn subastian() {
        let data = b"subastian:-234|g";
        let parsed = decode(data, 0).expect("failed to decode");
        let expected = literal!({
            "type": "g",
            "metric": "subastian",
            "action": "sub",
            "value": 234,

        });
        assert_eq!(parsed, expected);
        let encoded = encode(&parsed).expect("failed to encode");
        assert_eq!(encoded, data);
    }

    #[test]
    fn bench() {
        let data = b"foo:1620649445.3351967|h";
        let m = decode(data, 0).expect("failed to decode");
        assert_eq!(&data[..], encode(&m).expect("failed to encode"));

        let data = b"foo1:12345|c";
        let m = decode(data, 0).expect("failed to decode");
        assert_eq!(&data[..], encode(&m).expect("failed to encode"));

        let data = b"foo2:1234567890|c";
        let m = decode(data, 0).expect("failed to decode");
        assert_eq!(&data[..], encode(&m).expect("failed to encode"));
    }
}
