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

//! # InfluxDB line protocol parser
//!
//! Parses the InfluxDB Line protocol into a nested data structure.
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
use std::str;
use tremor_script::{
    influx::{parse, try_to_bytes},
    prelude::*,
};

#[derive(Clone)]
pub struct Influx {}

// This is ugly but we need to handle comments, thanks rental!
enum RentalSnot {
    Error(Error),
    Skip,
}

impl From<std::str::Utf8Error> for RentalSnot {
    fn from(e: std::str::Utf8Error) -> Self {
        RentalSnot::Error(e.into())
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
            "timestamp": 1_465_839_830_100_400_200i64
        })
        .into();

        assert_eq!(e, j)
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
    pub fn round_trip_all_cases() {
        let pairs = get_data_for_tests();

        pairs.iter().for_each(|case| {
            let mut codec = Influx {};
            let v = case.1.clone();
            let encoded = codec.encode(&v).expect("failed to encode");

            let decoded = codec
                .decode(encoded.clone(), 0)
                .expect("failed to dencode")
                .expect("failed to decode");
            let expected: Value = case.1.clone();
            let got = &decoded.suffix().value;
            if got != &expected {
                println!("{} fails while decoding", &case.2);
                assert_eq!(got.encode(), expected.encode());
            }
        })
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
