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

//! # `InfluxDB` line protocol parser
//!
//! Parses the `InfluxDB` Line protocol into a nested data structure.
//!
//!
//! The line
//!
//! ```text
//! weather,location=us-midwest temperature=82 1465839830100400200
//! ```
//! will be translated to the nested structure:
//!
//! ```json
//! {
//!     "measurement": "weather",
//!     "tags": {"location": "us-midwest"},
//!     "fields": {"temperature": 82.0},
//!     "timestamp": 1465839830100400200
//! }
//! ```
//! ## Configuration
//!
//! This operator takes no configuration

use super::Codec;
use crate::errors::*;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::borrow::Cow;
use std::convert::TryFrom;
use std::io::{Cursor, Write};
use std::str;
use tremor_script::{
    influx::{parse, try_to_bytes},
    prelude::*,
};

const TYPE_I64: u8 = 0;
const TYPE_F64: u8 = 1;
const TYPE_STRING: u8 = 2;
const TYPE_TRUE: u8 = 3;
const TYPE_FALSE: u8 = 4;

#[derive(Clone)]
pub struct Influx {}

// This is ugly but we need to handle comments, thanks rental!
#[allow(clippy::large_enum_variant)]
enum RentalSnot {
    Error(Error),
    Skip,
}

impl From<std::str::Utf8Error> for RentalSnot {
    fn from(e: std::str::Utf8Error) -> Self {
        Self::Error(e.into())
    }
}

impl Codec for Influx {
    fn decode(&mut self, data: Vec<u8>, ingest_ns: u64) -> Result<Option<LineValue>> {
        let r: std::result::Result<LineValue, RentalSnot> = LineValue::try_new(vec![data], |raw| {
            match parse(str::from_utf8(&raw[0])?, ingest_ns) {
                Ok(None) => Err(RentalSnot::Skip),
                Ok(Some(v)) => Ok(v.into()),
                Err(e) => Err(RentalSnot::Error(e.into())),
            }
        })
        .map_err(|e| e.0);
        match r {
            Ok(v) => Ok(Some(v)),
            Err(RentalSnot::Skip) => Ok(None),
            Err(RentalSnot::Error(e)) => Err(e),
        }
    }

    fn encode(&self, data: &simd_json::BorrowedValue) -> Result<Vec<u8>> {
        if let Some(bytes) = try_to_bytes(data) {
            Ok(bytes)
        } else {
            Err(ErrorKind::InvalidInfluxData(
                "This event does not conform with the influx schema ".into(),
            )
            .into())
        }
    }
}

// This is a pun
#[allow(clippy::module_name_repetitions)]
pub struct BInflux {}

impl BInflux {
    pub fn encode(v: &simd_json::BorrowedValue) -> Result<Vec<u8>> {
        fn write_str<W: Write>(w: &mut W, s: &str) -> Result<()> {
            w.write_u16::<BigEndian>(
                u16::try_from(s.len())
                    .map_err(|_| ErrorKind::InvalidInfluxData("string too long".into()))?,
            )?;
            w.write_all(s.as_bytes())?;
            Ok(())
        }

        let mut res = Vec::with_capacity(512);
        res.write_u16::<BigEndian>(0)?;
        if let Some(measurement) = v.get("measurement").and_then(Value::as_str) {
            write_str(&mut res, measurement)?;
        } else {
            return Err(ErrorKind::InvalidInfluxData("measurement missing".into()).into());
        }

        if let Some(timestamp) = v.get("timestamp").and_then(Value::as_u64) {
            res.write_u64::<BigEndian>(timestamp)?;
        } else {
            return Err(ErrorKind::InvalidInfluxData("timestamp missing".into()).into());
        }
        if let Some(tags) = v.get("tags").and_then(Value::as_object) {
            res.write_u16::<BigEndian>(
                u16::try_from(tags.len())
                    .map_err(|_| ErrorKind::InvalidInfluxData("too many tags".into()))?,
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
                    .map_err(|_| ErrorKind::InvalidInfluxData("too many fields".into()))?,
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
            return Err(ErrorKind::InvalidInfluxData("invalid version".into()).into());
        };
        let measurement = Value::String(read_string(&mut c)?);
        let timestamp = Value::I64(c.read_i64::<BigEndian>()?);
        let tag_count = c.read_u16::<BigEndian>()? as usize;
        let mut tags = Object::with_capacity(tag_count);
        for _i in 0..tag_count {
            let key = read_string(&mut c)?;
            let value = read_string(&mut c)?;
            tags.insert(key, Value::String(value));
        }
        let field_count = c.read_u16::<BigEndian>()? as usize;
        let mut fields = Object::with_capacity(field_count);
        for _i in 0..field_count {
            let key = read_string(&mut c)?;
            let kind = c.read_u8()?;
            match kind {
                TYPE_I64 => {
                    let value = c.read_i64::<BigEndian>()?;
                    fields.insert(key, Value::I64(value));
                }
                TYPE_F64 => {
                    let value = c.read_f64::<BigEndian>()?;
                    fields.insert(key, Value::F64(value));
                }
                TYPE_STRING => {
                    let value = read_string(&mut c)?;
                    fields.insert(key, Value::String(value));
                }
                TYPE_TRUE => {
                    fields.insert(key, Value::Bool(true));
                }
                TYPE_FALSE => {
                    fields.insert(key, Value::Bool(false));
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
    fn decode(&mut self, data: Vec<u8>, _ingest_ns: u64) -> Result<Option<LineValue>> {
        let r: std::result::Result<LineValue, RentalSnot> = LineValue::try_new(vec![data], |raw| {
            Self::decode(&raw[0])
                .map(ValueAndMeta::from)
                .map_err(RentalSnot::Error)
        })
        .map_err(|e| e.0);
        match r {
            Ok(v) => Ok(Some(v)),
            Err(RentalSnot::Skip) => Ok(None),
            Err(RentalSnot::Error(e)) => Err(e),
        }
    }

    fn encode(&self, data: &simd_json::BorrowedValue) -> Result<Vec<u8>> {
        Self::encode(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use halfbrown::HashMap;
    use pretty_assertions::assert_eq;
    use simd_json::{json, BorrowedValue as Value};

    #[test]
    fn unparse_test() {
        let s = "weather,location=us-midwest temperature=82 1465839830100400200";
        let d = parse(s, 0)
            .expect("failed to parse")
            .expect("failed to parse");
        // This is a bit ugly but to make a sensible compairison we got to convert the data
        // from an object to json to an object
        let j: Value = d;
        let e: Value = json!({
            "measurement": "weather",
            "tags": {"location": "us-midwest"},
            "fields": {"temperature": 82.0},
            "timestamp": 1_465_839_830_100_400_200_i64
        })
        .into();

        assert_eq!(e, j)
    }

    #[test]
    fn simple_bin_parse() -> Result<()> {
        let s = "weather,location=us-midwest,name=cake temperature=82 1465839830100400200";
        let d = parse(s, 0)?.expect("failed to parse");
        let b = BInflux::encode(&d)?;
        let e = BInflux::decode(&b)?;
        assert_eq!(e, d);
        Ok(())
    }

    #[test]
    fn unparse_empty() {
        let s = "";
        let d = parse(s, 0).expect("failed to parse");
        assert!(d.is_none());
        let s = "  ";
        let d = parse(s, 0).expect("failed to parse");
        assert!(d.is_none());
        let s = "  \n";
        let d = parse(s, 0).expect("failed to parse");
        assert!(d.is_none());
        let s = " \t \n";
        let d = parse(s, 0).expect("failed to parse");
        assert!(d.is_none());
    }

    #[test]
    fn unparse_comment() {
        let s = "# bla";
        let d = parse(s, 0).expect("failed to parse");
        assert!(d.is_none());
        let s = "  # bla";
        let d = parse(s, 0).expect("failed to parse");
        assert!(d.is_none());
        let s = " \t \n# bla";
        let d = parse(s, 0).expect("failed to parse");
        assert!(d.is_none());
    }

    #[test]
    pub fn decode() {
        let s = b"weather,location=us-midwest temperature=82 1465839830100400200".to_vec();
        let mut codec = Influx {};

        let decoded = codec
            .decode(s, 0)
            .expect("failed to decode")
            .expect("failed to decode");

        let e: Value = json!({
            "measurement": "weather",
            "tags": {"location": "us-midwest"},
            "fields": {"temperature": 82.0},
            "timestamp": 1_465_839_830_100_400_200i64
        })
        .into();
        assert_eq!(decoded.suffix().value, e)
    }

    #[test]
    pub fn encode() {
        let s: Value = json!({
            "measurement": "weather",
           "tags": {"location": "us-midwest"},
            "fields": {"temperature": 82.0},
            "timestamp": 1_465_839_830_100_400_200i64
        })
        .into();

        let codec = Influx {};

        let encoded = codec.encode(&s).expect("failed to encode");

        let influx: Value = json!({
            "measurement": "weather",
            "tags": {"location": "us-midwest"},
            "fields": {"temperature": 82.0},
            "timestamp": 1_465_839_830_100_400_200i64
        })
        .into();

        assert_eq!(encoded, try_to_bytes(&influx).expect("failed to encode"))
    }

    #[test]
    pub fn encode_mixed_bag() {
        let tags: HashMap<String, String> = HashMap::new();
        let s: Value = json!({
            "measurement": r#"wea,\ ther"#,
            "tags": tags,
            "fields": {"temp=erature": 82.0, r#"too\ \\\"hot""#: true},
            "timestamp": 1_465_839_830_100_400_200i64
        })
        .into();

        let codec = Influx {};

        let encoded = codec.encode(&s).expect("failed to encode");

        let raw =
            r#"wea\,\\\ ther temp\=erature=82.0,too\\\ \\\\\\\"hot\"=true 1465839830100400200"#;

        println!(
            "got: {}",
            str::from_utf8(&encoded).expect("failed to convert utf8")
        );
        println!("exp: {}", raw);
        assert_eq!(
            str::from_utf8(&encoded).expect("failed to convert utf8"),
            raw
        );
    }

    pub fn get_data_for_tests() -> [(Vec<u8>, Value<'static>, &'static str); 13] {
        [
            (
                b"weather,location=us\\,midwest temperature=82 1465839830100400200".to_vec(),

                json!({
                    "measurement": "weather",
                    "tags": {"location": "us,midwest"},
                    "fields": {"temperature": 82.0},
                    "timestamp": 1_465_839_830_100_400_200i64
                }).into(),
                "case 0"
            ),

            (
                b"weather,location_place=us-midwest temp\\=erature=82 1465839830100400200".to_vec(),

            json!({
                "measurement": "weather",
                "tags": {"location_place": "us-midwest"},
                "fields": {"temp=erature": 82.0},
                "timestamp": 1_465_839_830_100_400_200i64
            }).into(),
                "case 1"

            ),

            (
                b"weather,location\\ place=us-midwest temperature=82 1465839830100400200".to_vec(),

            json!({
                "measurement": "weather",
                "tags": {"location place": "us-midwest"},
                "fields": {"temperature": 82.0},
                "timestamp": 1_465_839_830_100_400_200i64
            }).into(),
                "case 2"

            ),

            (
                b"wea\\,ther,location=us-midwest temperature=82 1465839830100400200".to_vec(),

            json!({
                "measurement": "wea,ther",
                "tags": {"location": "us-midwest"},
                "fields": {"temperature": 82.0},
                "timestamp": 1_465_839_830_100_400_200i64
            }).into(),
                "case 3"
            ),

            (

                b"wea\\ ther,location=us-midwest temperature=82 1465839830100400200".to_vec(),

                json!({
                    "measurement": "wea ther",
                    "tags": {"location": "us-midwest"},
                    "fields": {"temperature": 82.0},
                    "timestamp": 1_465_839_830_100_400_200i64
                }).into(),
                "case 4"
            ),

             (

               br#"weather,location=us-midwest temperature_str="too\ hot\cold" 1465839830100400203"#.to_vec(),
                 json!({
                      "measurement": "weather",
                     "tags": {"location": "us-midwest"} ,
                     "fields": {"temperature_str": "too\\ hot\\cold"},
                     "timestamp": 1_465_839_830_100_400_200i64
                 }).into(),
                 "case 5"
             ),
            (

                b"weather,location=us-midwest temperature_str=\"too hot/cold\" 1465839830100400202".to_vec(),
                json!({
                    "measurement": "weather",
                    "tags": {"location": "us-midwest"},
                    "fields": {"temperature_str": r#"too hot/cold"#},
                    "timestamp": 1_465_839_830_100_400_200i64
                }).into(),
                "case 6"
            ),

            (
                br#"weather,location=us-midwest temperature_str="too hot\\cold" 1465839830100400203"#.to_vec(),

                json!({
                    "measurement": "weather",
                    "tags": {"location": "us-midwest"},
                    "fields": {"temperature_str": r#"too hot\cold"#},
                    "timestamp": 1_465_839_830_100_400_200i64
                }).into(),
                "case 7"

                ),
            (
                br#"weather,location=us-midwest temperature_str="too hot\\\cold" 1465839830100400204"#.to_vec(),

                json!({
                    "measurement": "weather",
                    "tags": {"location": "us-midwest"},
                    "fields": {"temperature_str": r#"too hot\\cold"#},
                    "timestamp": 1_465_839_830_100_400_204i64
                }).into(),
                "case 8"

            ),

            (
                b"weather,location=us-midwest temperature_str=\"too hot\\\\\\\\cold\" 1465839830100400205".to_vec(),

                json!({
                    "measurement": "weather",
                    "tags": {"location": "us-midwest"},
                    "fields": {"temperature_str": "too hot\\\\cold"},
                    "timestamp": 1_465_839_830_100_400_205i64
                }).into(),
                "case 9"

            ),

            (
                b"weather,location=us-midwest temperature_str=\"too hot\\\\\\\\\\cold\" 1465839830100400206".to_vec(),
                json!({
                    "measurement": "weather",
                    "tags": {"location": "us-midwest"},
                    "fields": {"temperature_str": "too hot\\\\\\cold"},
                    "timestamp": 1_465_839_830_100_400_206i64
                }).into(),
                "case 10"
            ),

            (
                b"weather,location=us-midwest temperature=82,bug_concentration=98 1465839830100400200".to_vec(),
 json!({
            "measurement": "weather",
            "tags" :   { "location": "us-midwest" },
            "fields": {"temperature": 82.0, "bug_concentration": 98.0},
            "timestamp": 1_465_839_830_100_400_200i64
        }).into(),
        "case 11"
        ),

        (

           b"weather,location=us-midwest temperature=82i 1465839830100400200".to_vec(),
    json!({
            "measurement": "weather",
            "tags" :   { "location": "us-midwest" },
            "fields": {"temperature": 82},
            "timestamp": 1_465_839_830_100_400_200i64
        }).into(),
        "case 12"
)
        ]
    }

    #[test]
    pub fn round_trip_all_cases() -> Result<()> {
        let pairs = get_data_for_tests();

        for case in &pairs {
            let mut codec = Influx {};
            let v = case.1.clone();
            let encoded = codec.encode(&v)?;

            let decoded = codec.decode(encoded.clone(), 0)?.expect("failed to decode");
            let expected: Value = case.1.clone();
            let got = &decoded.suffix().value;
            let bin = BInflux::encode(&expected)?;
            if got != &expected {
                println!("{} fails while decoding", &case.2);
                assert_eq!(got.encode(), expected.encode());
            }

            let decoded_bin = BInflux::decode(&bin)?;
            assert_eq!(decoded_bin, expected);
        }
        Ok(())
    }

    #[test]
    pub fn parse_simple3() {
        let s =
            b"weather,location=us-midwest temperature=82,bug_concentration=98 1465839830100400200"
                .to_vec();
        let mut codec = Influx {};

        let decoded = codec
            .decode(s, 0)
            .expect("failed to decode")
            .expect("failed to decode");

        let e: Value = json!({
            "measurement": "weather",
            "tags" :   { "location": "us-midwest" },
            "fields": {"temperature": 82.0, "bug_concentration": 98.0},

            "timestamp": 1_465_839_830_100_400_200i64
        })
        .into();
        assert_eq!(decoded.suffix().value, e)
    }

    #[test]
    pub fn parse_int_value() {
        let s = b"weather,location=us-midwest temperature=82i 1465839830100400200".to_vec();
        let mut codec = Influx {};

        let decoded = codec
            .decode(s, 0)
            .expect("failed to decode")
            .expect("failed to decode");

        let e: Value = json!({
            "measurement": "weather",
            "tags" :   { "location": "us-midwest" },
            "fields": {"temperature": 82},
            "timestamp": 1_465_839_830_100_400_200i64
        })
        .into();
        assert_eq!(decoded.suffix().value, e)
    }

    #[test]
    pub fn live_usecase() {
        let s = b"kafka_BrokerTopicMetrics,agent=jmxtrans,dc=iad1,host_name=kafka-iad1-g4-1,junk=kafka_topic,kafka_type=server,metric_type=counter,topic_name=customerEmailServiceMessage BytesInPerSec=0i,BytesOutPerSec=0i,FailedFetchRequestsPerSec=0i,FetchMessageConversionsPerSec=0i,TotalFetchRequestsPerSec=1993153i 1562179275506000000".to_vec();

        let mut codec = Influx {};

        let e: Value = json!({
                    "measurement" : "kafka_BrokerTopicMetrics",
                    "tags" :  {
                        "agent": "jmxtrans",
                        "dc": "iad1",
                        "host_name": "kafka-iad1-g4-1",
                        "junk": "kafka_topic",
                        "kafka_type": "server",
                        "metric_type": "counter",
                        "topic_name": "customerEmailServiceMessage",
                        },
                    "fields" :  {
                        "TotalFetchRequestsPerSec": 1_993_153,
                        "BytesOutPerSec": 0,
                        "BytesInPerSec": 0,
                        "FailedFetchRequestsPerSec": 0,
                        "FetchMessageConversionsPerSec": 0
                       },
                    "timestamp" : 1_562_179_275_506_000_000i64
        })
        .into();
        let decoded = codec
            .decode(s.clone(), 0)
            .expect("failed to decode")
            .expect("failed to decode");
        let encoded = codec
            .encode(&decoded.suffix().value)
            .expect("failed to encode");

        assert_eq!(decoded.suffix().value, e);
        unsafe {
            assert_eq!(
                str::from_utf8_unchecked(&encoded),
                str::from_utf8_unchecked(&s)
            );
        }
    }

    /*
    #[bench]
    fn parse_bench(b: &mut Bencher) {
        let sarr = &[
    "weather,location=us-midwest too_hot=true 1465839830100400200",
    "weather,location=us-midwest too_hot=True 1465839830100400200",
    "weather,location=us-midwest too_hot=TRUE 1465839830100400200",
    "weather,location=us-midwest too_hot=t 1465839830100400200",
    "weather,location=us-midwest too_hot=T 1465839830100400200",
    "weather,location=us-midwest too_hot=false 1465839830100400200",
    "weather,location=us-midwest too_hot=False 1465839830100400200",
    "weather,location=us-midwest too_hot=FALSE 1465839830100400200",
    "weather,location=us-midwest too_hot=f 1465839830100400200",
    "weacrther,location=us-midwest too_hot=F 1465839830100400200",
    "weather,location=us-midwest temperature=82 1465839830100400200",
    "weather,location=us-midwest,season=summer temperature=82 1465839830100400200",
    "weather temperature=82 1465839830100400200",
    "weather temperature=82i 1465839830100400200",
    "weather,location=us-midwest temperature=\"too warm\" 1465839830100400200",
    "weather,location=us\\,midwest temperature=82 1465839830100400200",
    "weather,location=us-midwest temp\\=rature=82 1465839830100400200",
    "weather,location\\ place=us-midwest temperature=82 1465839830100400200",
    "wea\\,ther,location=us-midwest temperature=82 1465839830100400200",
    "wea\\ ther,location=us-midwest temperature=82 1465839830100400200",
    "weather,location=us-midwest temperature=\"too\\\"hot\\\"\" 1465839830100400200",
    "weather,location=us-midwest temperature_str=\"too hot/cold\" 1465839830100400201",
    "weather,location=us-midwest temperature_str=\"too hot\\cold\" 1465839830100400202",
    "weather,location=us-midwest temperature_str=\"too hot\\\\\\\\cold\" 1465839830100400205",
    "weather,location=us-midwest temperature_str=\"too hot\\\\\\\\\\cold\" 1465839830100400206",
    "weather,location=us-midwest temperature=82 1465839830100400200",
    "weather,location=us-midwest temperature=82,bug_concentration=98 1465839830100400200",
    "weather,location=us-midwest temperature_str=\"too hot\\\\cold\" 1465839830100400203",
    "weather,location=us-midwest temperature_str=\"too hot\\\\\\cold\" 1465839830100400204"];

        b.iter(|| {
            for s in sarr {
                parse(s, 0);
            }
        });
    }
    */
}
