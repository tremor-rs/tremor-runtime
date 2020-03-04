// Copyright 2018-2020, Wayfair GmbH
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

use super::prelude::*;
use std::{mem, str};
use tremor_influx as influx;

#[derive(Clone)]
pub struct Influx {}

// This is ugly but we need to handle comments, thanks rental!
#[allow(clippy::large_enum_variant)]
pub(crate) enum RentalSnot {
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
            let s: &'static str = unsafe { mem::transmute(str::from_utf8(&raw[0])?) };
            match influx::decode::<'static, Value<'static>>(s, ingest_ns) {
                Ok(None) => Err(RentalSnot::Skip),
                Ok(Some(v)) => Ok(v.into()),
                Err(e) => Err(RentalSnot::Error(
                    ErrorKind::InvalidInfluxData(String::from_utf8_lossy(&raw[0]).to_string(), e)
                        .into(),
                )),
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
        Ok(influx::encode(data)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::binflux::BInflux;
    use pretty_assertions::assert_eq;
    use simd_json::prelude::*;
    use simd_json::{json, value::borrowed::Value};
    use tremor_influx as influx;

    #[test]
    fn simple_bin_parse() -> Result<()> {
        let s = "weather,location=us-midwest,name=cake temperature=82 1465839830100400200";
        let d = influx::decode(s, 0)
            .expect("failed to parse")
            .expect("failed to parse");
        let b = BInflux::encode(&d)?;
        let e = BInflux::decode(&b)?;
        assert_eq!(e, d);
        Ok(())
    }

    #[test]
    pub fn encode_mixed_bag() {
        let s: Value = json!({
            "measurement": r#"wea,\ ther"#,
            "tags": {},
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
    #[test]
    pub fn decode_test() {
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

    fn get_data_for_tests() -> [(Vec<u8>, Value<'static>, &'static str); 13] {
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
}
