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

impl Codec for Influx {
    fn name(&self) -> String {
        "influx".to_string()
    }

    fn decode<'input>(
        &mut self,
        data: &'input mut [u8],
        ingest_ns: u64,
    ) -> Result<Option<Value<'input>>> {
        // This is safe as from_utf8 does not change the memory location
        // of the bytes, simply validates that it is UTF8 and if so
        // change the type.
        let s: &str = unsafe { mem::transmute(str::from_utf8(data)?) };
        influx::decode::<'static, Value<'static>>(s, ingest_ns).map_err(|e| {
            ErrorKind::InvalidInfluxData(String::from_utf8_lossy(data).to_string(), e).into()
        })
    }

    fn encode(&self, data: &simd_json::BorrowedValue) -> Result<Vec<u8>> {
        Ok(influx::encode(data)?)
    }

    fn boxed_clone(&self) -> Box<dyn Codec> {
        Box::new(self.clone())
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
        let mut s = b"weather,location=us-midwest temperature=82 1465839830100400200".to_vec();
        let mut codec = Influx {};

        let decoded = codec
            .decode(s.as_mut_slice(), 0)
            .expect("failed to decode")
            .expect("failed to decode");

        let e: Value = json!({
            "measurement": "weather",
            "tags": {"location": "us-midwest"},
            "fields": {"temperature": 82.0},
            "timestamp": 1_465_839_830_100_400_200i64
        })
        .into();
        assert_eq!(decoded, &e)
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
            let mut encoded = codec.encode(&v)?;

            let decoded = codec
                .decode(encoded.as_mut_slice(), 0)?
                .expect("failed to decode");
            let expected: Value = case.1.clone();
            let got = decoded;
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
        let mut s =
            b"weather,location=us-midwest temperature=82,bug_concentration=98 1465839830100400200"
                .to_vec();
        let mut codec = Influx {};

        let decoded = codec
            .decode(s.as_mut_slice(), 0)
            .expect("failed to decode")
            .expect("failed to decode");

        let e: Value = json!({
            "measurement": "weather",
            "tags" :   { "location": "us-midwest" },
            "fields": {"temperature": 82.0, "bug_concentration": 98.0},

            "timestamp": 1_465_839_830_100_400_200i64
        })
        .into();
        assert_eq!(decoded, &e)
    }

    #[test]
    pub fn parse_int_value() {
        let mut s = b"weather,location=us-midwest temperature=82i 1465839830100400200".to_vec();
        let mut codec = Influx {};

        let decoded = codec
            .decode(s.as_mut_slice(), 0)
            .expect("failed to decode")
            .expect("failed to decode");

        let e: Value = json!({
            "measurement": "weather",
            "tags" :   { "location": "us-midwest" },
            "fields": {"temperature": 82},
            "timestamp": 1_465_839_830_100_400_200i64
        })
        .into();
        assert_eq!(decoded, &e)
    }

    #[test]
    pub fn live_usecase() {
        let mut s = b"kafka_BrokerTopicMetrics,agent=jmxtrans,dc=iad1,host_name=kafka-iad1-g4-1,junk=kafka_topic,kafka_type=server,metric_type=counter,topic_name=customerEmailServiceMessage BytesInPerSec=0i,BytesOutPerSec=0i,FailedFetchRequestsPerSec=0i,FetchMessageConversionsPerSec=0i,TotalFetchRequestsPerSec=1993153i 1562179275506000000".to_vec();
        let expected = s.clone();

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
            .decode(s.as_mut_slice(), 0)
            .expect("failed to decode")
            .expect("failed to decode");
        let encoded = codec.encode(&decoded).expect("failed to encode");

        assert_eq!(decoded, &e);
        unsafe {
            assert_eq!(
                str::from_utf8_unchecked(&encoded),
                str::from_utf8_unchecked(expected.as_slice())
            );
        }
    }
}
