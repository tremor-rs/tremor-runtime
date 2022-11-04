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
    let mut itoa_buf = itoa::Buffer::new();
    let mut ryu_buf = ryu::Buffer::new();

    let mut r = Vec::with_capacity(512);
    r.extend_from_slice(
        value
            .get_str("metric")
            .ok_or(ErrorKind::InvalidStatsD)?
            .as_bytes(),
    );
    let t = value.get_str("type").ok_or(ErrorKind::InvalidStatsD)?;
    let val = value.get("value").ok_or(ErrorKind::InvalidStatsD)?;
    if !val.is_number() {
        return Err(ErrorKind::InvalidStatsD.into());
    };

    r.push(b':');
    if t == "g" {
        match value.get_str("action") {
            Some("add") => r.push(b'+'),
            Some("sub") => r.push(b'-'),
            _ => (),
        }
    };

    r.extend_from_slice(val.encode().as_bytes());
    r.push(b'|');
    r.extend_from_slice(t.as_bytes());

    if let Some(n) = value.get_u64("sample_rate") {
        r.extend_from_slice(b"|@");
        r.extend_from_slice(itoa_buf.format(n).as_bytes());
    } else if let Some(n) = value.get_f64("sample_rate") {
        r.extend_from_slice(b"|@");
        r.extend_from_slice(ryu_buf.format(n).as_bytes());
    }

    Ok(r)
}

fn decode(data: &[u8], _ingest_ns: u64) -> Result<Value> {
    #[derive(Debug, PartialEq)]
    enum Sign {
        Plus,
        Minus,
        None,
    }
    let data = simdutf8::basic::from_utf8(data)?;

    let mut m = Object::with_capacity(4);

    let (metric, data) = data.split_once(':').ok_or_else(invalid)?;
    m.insert_nocheck("metric".into(), Value::from(metric));

    let (sign, data) = if let Some(data) = data.strip_prefix('+') {
        (Sign::Plus, data)
    } else if data.starts_with('-') {
        (Sign::Minus, data)
    } else {
        (Sign::None, data)
    };

    let (v, data) = data.split_once('|').ok_or_else(invalid)?;

    let mut value = if v.contains('.') {
        lexical::parse::<f64, _>(v)
            .map(Value::from)
            .map_err(Error::from)?
    } else if v.starts_with('-') {
        lexical::parse::<i64, _>(v)
            .map(Value::from)
            .map_err(Error::from)?
    } else {
        lexical::parse::<u64, _>(v)
            .map(Value::from)
            .map_err(Error::from)?
    };

    let data = if data.starts_with(|c| matches!(c, 'c' | 'h' | 's')) {
        let (t, data) = data.split_at(1);
        m.insert_nocheck("type".into(), t.into());
        data
    } else if data.starts_with("ms") {
        m.insert_nocheck("type".into(), "ms".into());
        data.get(2..).ok_or_else(invalid)?
    } else if data.starts_with('g') {
        let (t, data) = data.split_at(1);
        m.insert_nocheck("type".into(), t.into());
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
        data
    } else {
        data
    };
    if let Some(s) = data.strip_prefix("|@") {
        let v: f64 = lexical::parse(s)?;
        m.insert("sample_rate".into(), Value::from(v));
    } else if !data.is_empty() {
        return Err(invalid());
    };

    m.insert("value".into(), value);
    Ok(Value::from(m))
}

fn invalid() -> Error {
    Error::from(ErrorKind::InvalidStatsD)
}

#[cfg(test)]
mod test {
    use std::convert::identity;

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
            .ok()
            .and_then(identity)
            .unwrap_or_default();
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
