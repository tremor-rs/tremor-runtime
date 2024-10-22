// Copyright 2024, The Tremor Team
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

//! The `graphite-plaintext` codec supports the [graphite plaintext format](https://graphite.readthedocs.io/en/latest/feeding-carbon.html#the-plaintext-protocol).
//!
//! The format is similar to the `influx` line protocol.
//!
//! The codec translates a single `graphite` measurement line into a structured event.
//!
//! ## Example
//!
//! The native format of a single graphite event line is as follows:
//!
//! ```text
//! <metric_path> <value> <timestamp>
//! ```
//!
//! The equivalent representation as a tremor value:
//!
//! ```json
//! {
//!   "metric": "my.metric.path",
//!   "value": 7,
//!  "timestamp": 1620649445 // number of seconds since the epoch
//! }
//! ```
//!
//! ## Supported types
//!
//! Graphite has only one type, `gauge` and supports only numeric values.
//!
//! ## Considerations
//!
//! If timestamps are not provided, the codec will use the current time as the event's timestamp.
//!
//! If timestamp value is -1 it will be replaced with the current time as the event's timestamp.

use crate::prelude::*;
use simd_json::ObjectHasher;
use std::io::Write;

#[derive(Clone, Default, Debug)]
pub struct PlaintextProtocol {
    buf: Vec<u8>,
}

#[async_trait::async_trait]
impl Codec for PlaintextProtocol {
    fn name(&self) -> &str {
        "graphite-plaintext"
    }

    async fn decode<'input>(
        &mut self,
        data: &'input mut [u8],
        ingest_ns: u64,
        meta: Value<'input>,
    ) -> Result<Option<(Value<'input>, Value<'input>)>> {
        decode_plaintext(data, ingest_ns).map(|v| Some((v, meta)))
    }

    async fn encode(&mut self, data: &Value, _meta: &Value) -> Result<Vec<u8>> {
        encode_plaintext(data, &mut self.buf)?;
        let v = self.buf.clone();
        self.buf.clear();
        Ok(v)
    }

    fn boxed_clone(&self) -> Box<dyn Codec> {
        Box::new(self.clone())
    }
}

fn encode_plaintext(value: &Value, r: &mut impl Write) -> Result<()> {
    let metric = value.get_str("metric").ok_or(ErrorKind::InvalidStatsD)?;
    let val = value
        .get("value")
        .ok_or(ErrorKind::InvalidGraphitePlaintext)?;

    let ts = value
        .get("timestamp")
        .ok_or(ErrorKind::InvalidGraphitePlaintext)?;

    if !val.is_number() {
        return Err(ErrorKind::InvalidGraphitePlaintext.into());
    };

    r.write_all(metric.as_bytes())?;
    r.write_all(b" ")?;
    r.write_all(val.encode().as_bytes())?;
    r.write_all(b" ")?;
    r.write_all(ts.encode().as_bytes())?;

    Ok(())
}

fn decode_plaintext(data: &[u8], ingest_ns: u64) -> Result<Value> {
    let data = simdutf8::basic::from_utf8(data)?;

    let mut m = Object::with_capacity_and_hasher(3, ObjectHasher::default());

    let (metric, data) = data
        .split_once(' ')
        .ok_or_else(|| Error::from(ErrorKind::InvalidGraphitePlaintext))?;

    let (v, ts) = data
        .split_once(' ')
        .ok_or_else(|| Error::from(ErrorKind::InvalidGraphitePlaintext))?;

    let value = if v.contains('.') {
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

    m.insert_nocheck("metric".into(), Value::from(metric));

    if "-1" == ts {
        m.insert("timestamp".into(), Value::from(ingest_ns));
    } else {
        let ts = lexical::parse::<u64, _>(ts)?;
        m.insert("timestamp".into(), Value::from(ts));
    }

    m.insert("value".into(), value);
    Ok(Value::from(m))
}

#[cfg(test)]
mod test {

    mod plaintext {
        use super::super::*;
        use tremor_value::literal;

        // beep.boop 7 1620649445
        #[tokio::test(flavor = "multi_thread")]
        async fn name_value_ts() {
            let data = b"beep.boop 7 1620649445";
            let parsed = decode_plaintext(data, 0).expect("failed to decode");
            let expected = literal!({
                "metric": "beep.boop",
                "value": 7,
                "timestamp": 1_620_649_445i64,

            });
            assert_eq!(parsed, expected);
            let mut encoded = Vec::new();
            encode_plaintext(&parsed, &mut encoded).expect("failed to encode");
            assert_eq!(encoded.as_slice(), data);
        }

        // beep.boop 320.0 -1
        #[tokio::test(flavor = "multi_thread")]
        async fn name_value_minusone() {
            let data = b"beep.boop 320.0 -1";
            let parsed = decode_plaintext(data, 1234).expect("failed to decode");
            let expected = literal!({
                "metric": "beep.boop",
                "value": 320.0,
                "timestamp": 1234,
            });
            assert_eq!(parsed, expected);
            let mut encoded = Vec::new();
            encode_plaintext(&parsed, &mut encoded).expect("failed to encode");
            assert_eq!(encoded, b"beep.boop 320.0 1234"); // -1 is replaced with 1234 so this is asymetric w.r.t. input
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_for_coverage() {
            let mut codec = PlaintextProtocol::default();
            assert_eq!("graphite-plaintext", codec.name());
            let cloned = codec.boxed_clone();
            assert_eq!("graphite-plaintext", cloned.name());

            let mut data = b"beep.boop 7 1620649445".to_vec();
            let decoded = codec.decode(&mut data, 0, Value::null()).await;
            assert_eq!(
                decoded,
                Ok(Some((
                    literal!({
                        "metric": "beep.boop",
                        "value": 7,
                        "timestamp": 1_620_649_445i64,
                    }),
                    Value::null()
                )))
            );

            let mut bad = b"".to_vec();
            let decoded = codec.decode(&mut bad, 0, Value::null()).await;
            assert!(decoded.is_err());

            let mut bad = b"metric.no.measure.no.timestamp".to_vec();
            let decoded = codec.decode(&mut bad, 0, Value::null()).await;
            assert!(decoded.is_err());

            let mut bad = b"metric.with.measure.no.timestamp 320.0".to_vec();
            let decoded = codec.decode(&mut bad, 0, Value::null()).await;
            assert!(decoded.is_err());

            let encoded = codec
                .encode(
                    &literal!({
                        "metric": "beep.boop",
                        "value": 7
                    }),
                    &Value::null(),
                )
                .await;
            assert!(encoded.is_err());

            let encoded = codec
                .encode(
                    &literal!({
                        "metric": "beep.boop"
                    }),
                    &Value::null(),
                )
                .await;
            assert!(encoded.is_err());
        }
    }
}
