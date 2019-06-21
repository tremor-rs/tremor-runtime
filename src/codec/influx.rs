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
    influx::{parse, InfluxDatapoint},
    LineValue,
};

#[derive(Clone)]
pub struct Influx {}

impl Codec for Influx {
    fn decode(&self, data: Vec<u8>) -> Result<LineValue> {
        LineValue::try_new(Box::new(data), |raw| {
            let s = str::from_utf8(&raw)?;
            let parsed = parse(s)?;
            Ok(simd_json::serde::to_owned_value(parsed)?.into())
        })
        .map_err(|e| e.0)
    }

    fn encode(&self, data: LineValue) -> Result<Vec<u8>> {
        data.rent(|json| {
            let influx: InfluxDatapoint = simd_json::serde::from_borrowed_value(json.clone())?;
            let bytes = influx.try_to_bytes()?;
            Ok(bytes)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use halfbrown::{hashmap, HashMap};
    use simd_json::{json, OwnedValue};

    #[test]
    fn unparse_test() {
        let s = "weather,location=us-midwest temperature=82 1465839830100400200";
        let d = parse(s).expect("failed to parse");
        // This is a bit ugly but to make a sensible compairison we got to convert the data
        // from an object to json to an object
        let j: OwnedValue = serde_json::from_str(
            serde_json::to_string(&d)
                .expect("failed to encode")
                .as_str(),
        )
        .expect("failed to decode");
        let e: OwnedValue = json!({
            "measurement": "weather",
            "tags": hashmap!{"location" => "us-midwest"},
            "fields": hashmap!{"temperature" => 82.0},
            "timestamp": 1465839830100400200i64
        });
        assert_eq!(e, j)
    }

    #[test]
    pub fn decode() {
        let s = b"weather,location=us-midwest temperature=82 1465839830100400200".to_vec();
        let codec = Influx {};

        let decoded = codec.decode(s).expect("failed to decode");

        let e: OwnedValue = json!({
            "measurement": "weather",
            "tags": hashmap!{"location" => "us-midwest"},
            "fields": hashmap!{"temperature" => 82.0},
            "timestamp": 1465839830100400200i64
        });
        assert_eq!(decoded, e)
    }

    #[test]
    pub fn encode() {
        let s = json!({
            "measurement": "weather",
           "tags": hashmap!{"location" => "us-midwest"},
            "fields": hashmap!{"temperature" => 82.0},
            "timestamp": 1465839830100400200i64
        });

        let codec = Influx {};

        let encoded = codec.encode(s.into()).expect("failed to encode");

        let influx = InfluxDatapoint::from_parts(
            "weather",
            [("location".to_owned(), "us-midwest".to_owned())]
                .into_iter()
                .cloned()
                .collect(),
            [("temperature".to_owned(), 82u64.into())]
                .into_iter()
                .cloned()
                .collect(),
            1465839830100400200u64,
        );

        assert_eq!(encoded, influx.try_to_bytes().expect("failed to encode"))
    }

    #[test]
    pub fn encode_mixed_bag() {
        let tags: HashMap<String, String> = HashMap::new();
        let s = json!({
            "measurement": r#"wea,\ ther"#,
            "tags": tags,
            "fields": hashmap!{"temp=erature" => OwnedValue::F64(82.0), r#"too\ \\\"hot""# => OwnedValue::Bool(true)},
            "timestamp": 1465839830100400200u64
        });

        let codec = Influx {};

        let encoded = codec.encode(s.into()).expect("failed to encode");

        let raw = r#"wea\,\\\ ther temp\=erature=82 too\\\ \\\\\\\"hot\"=true 1465839830100400200"#;

        assert_eq!(
            str::from_utf8(&encoded).expect("failed to convert utf8"),
            raw
        );
    }

    pub fn get_data_for_tests() -> [(Vec<u8>, OwnedValue, &'static str); 11] {
        [
            (
                b"weather,location=us\\,midwest temperature=82 1465839830100400200".to_vec(),

                json!({
                    "measurement": "weather",
                    "tags": hashmap!{"location" => "us,midwest"},
                    "fields": hashmap!{"temperature" => 82.0},
                    "timestamp": 1465839830100400200i64
                }),
                "case 0"
            ),

            (
                b"weather,location_place=us-midwest temp\\=erature=82 1465839830100400200".to_vec(),

            json!({
                "measurement": "weather",
                "tags": hashmap!{"location_place" => "us-midwest"},
                "fields": hashmap!{"temp=erature" => 82.0},
                "timestamp": 1465839830100400200i64
            }),
                "case 1"

            ),

            (
                b"weather,location\\ place=us-midwest temperature=82 1465839830100400200".to_vec(),

            json!({
                "measurement": "weather",
                "tags": hashmap!{"location place" => "us-midwest"},
                "fields": hashmap!{"temperature" => 82.0},
                "timestamp": 1465839830100400200i64
            }),
                "case 2"

            ),

            (
                b"wea\\,ther,location=us-midwest temperature=82 1465839830100400200".to_vec(),

            json!({
                "measurement": "wea,ther",
                "tags": hashmap!{"location" => "us-midwest"},
                "fields": hashmap!{"temperature" => 82.0},
                "timestamp": 1465839830100400200i64
            }),
                "case 3"
            ),

            (

                b"wea\\ ther,location=us-midwest temperature=82 1465839830100400200".to_vec(),

                json!({
                    "measurement": "wea ther",
                    "tags": hashmap!{"location" => "us-midwest"},
                    "fields": hashmap!{"temperature" => 82.0},
                    "timestamp": 1465839830100400200i64
                }),
                "case 4"
            ),

             (

               br#"weather,location=us-midwest temperature_str="too\ hot\cold" 1465839830100400203"#.to_vec(),
                 json!({
                      "measurement": "weather",
                     "tags": hashmap!{"location" => "us-midwest"}, 
                     "fields": hashmap!{"temperature_str" => OwnedValue::from("too\\ hot\\cold")},
                     "timestamp": 1465839830100400203i64
                 }),
                 "case 5"
             ),
            (

                b"weather,location=us-midwest temperature_str=\"too hot/cold\" 1465839830100400202".to_vec(),
                json!({

                    "measurement": "weather",
                    "tags": hashmap!{"location" => "us-midwest"},
                    "fields": hashmap!{"temperature_str" => OwnedValue::from(r#"too hot/cold"#)},
                    "timestamp": 1465839830100400202i64
                }),
                "case 6"
            ),

            (
                br#"weather,location=us-midwest temperature_str="too hot\\cold" 1465839830100400203"#.to_vec(),

                json!({
                    "measurement": "weather",
                    "tags": hashmap!{"location" => "us-midwest"},
                    "fields": hashmap!{"temperature_str" => OwnedValue::from(r#"too hot\cold"#)},
                    "timestamp": 1465839830100400203i64
                }),
                "case 7"

                ),
            (
                br#"weather,location=us-midwest temperature_str="too hot\\\cold" 1465839830100400204"#.to_vec(),

                json!({
                    "measurement": "weather",
                    "tags": hashmap!{"location" => "us-midwest"},
                    "fields": hashmap!{"temperature_str" => OwnedValue::from(r#"too hot\\cold"#)},
                    "timestamp": 1465839830100400204i64
                }),
                "case 8"

            ),

            (
                b"weather,location=us-midwest temperature_str=\"too hot\\\\\\\\cold\" 1465839830100400205".to_vec(),

                json!({
                    "measurement": "weather",
                    "tags": hashmap!{"location" => "us-midwest"},
                    "fields": hashmap!{"temperature_str" => OwnedValue::from("too hot\\\\cold")},
                    "timestamp": 1465839830100400205i64
                }),
                "case 9"

            ),

            (
                b"weather,location=us-midwest temperature_str=\"too hot\\\\\\\\\\cold\" 1465839830100400206".to_vec(),
                json!({
                    "measurement": "weather",
                    "tags": hashmap!{"location" => "us-midwest"},
                    "fields": hashmap!{"temperature_str" => OwnedValue::from("too hot\\\\\\cold")},
                    "timestamp": 1465839830100400206i64
                }),
                "case 10"
            )


        ]
    }

    #[test]
    pub fn round_trip_all_cases() {
        let pairs = get_data_for_tests();

        pairs.iter().for_each(|case| {
            let codec = Influx {};

            let encoded = codec
                .encode(case.1.clone().into())
                .expect("failed to encode");

            let decoded = codec.decode(encoded.clone()).expect("failed to dencode");

            if decoded != case.1 {
                println!("{} fails while decoding", &case.2);
                assert_eq!(decoded, case.1);
            }
        })
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
                parse(s);
            }
        });
    }
    */
}
