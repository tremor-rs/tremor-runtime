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
use hashbrown::HashMap;
use serde_json::{json, Number, Value};
use std::str::{self, Chars};
use tremor_pipeline::EventValue;

#[derive(Clone)]
pub struct Influx {}

impl Codec for Influx {
    fn decode(&self, data: EventValue) -> Result<EventValue> {
        match data {
            EventValue::Raw(raw) => {
                let s = str::from_utf8(&raw)?;
                let parsed = parse(s)?;
                Ok(EventValue::JSON(json!(parsed)))
            }
            EventValue::JSON(_) => Ok(data),
            EventValue::None => Ok(EventValue::JSON(serde_json::Value::Null)),
        }
    }

    fn encode(&self, data: EventValue) -> Result<EventValue> {
        match data {
            EventValue::JSON(json) => {
                let measurement = json
                    .get("measurement")
                    .ok_or_else(|| ErrorKind::InvalidInfluxData(json.to_string()))?
                    .as_str()
                    .ok_or_else(|| ErrorKind::InvalidInfluxData(json.to_string()))?;
                let mapped_tags = json
                    .get("tags")
                    .ok_or_else(|| ErrorKind::InvalidInfluxData(json.to_string()))?
                    .as_object()
                    .ok_or_else(|| ErrorKind::InvalidInfluxData(json.to_string()))?;

                let mut tags = HashMap::new();
                for (key, value) in mapped_tags {
                    let val = value
                        .as_str()
                        .ok_or_else(|| ErrorKind::InvalidInfluxData(json.to_string()))?
                        .to_string();

                    tags.insert(key.to_owned(), val.to_owned());
                }
                let fields = json
                    .get("fields")
                    .ok_or_else(|| ErrorKind::InvalidInfluxData(json.to_string()))?
                    .as_object()
                    .ok_or_else(|| ErrorKind::InvalidInfluxData(json.to_string()))?
                    .iter()
                    .map(|(key, value)| (key.to_owned(), value.to_owned()))
                    .collect::<HashMap<String, Value>>();

                let timestamp = json
                    .get("timestamp")
                    .ok_or_else(|| ErrorKind::InvalidInfluxData(json.to_string()))?
                    .as_u64()
                    .ok_or_else(|| ErrorKind::InvalidInfluxData(json.to_string()))?;

                let influx = InfluxDatapoint::new(measurement, tags, fields, timestamp);

                let bytes = influx.try_to_bytes()?;
                Ok(EventValue::Raw(bytes.clone()))
            }
            _ => Err("Trying to encode non json data.".into()),
        }
    }
}

#[derive(PartialEq, Clone, Debug, Serialize)]
pub struct InfluxDatapoint {
    measurement: String,
    tags: HashMap<String, String>,
    fields: HashMap<String, Value>,
    timestamp: u64,
}

impl InfluxDatapoint {
    pub fn new(
        measurement: &str,
        tags: HashMap<String, String>,
        fields: HashMap<String, Value>,
        timestamp: u64,
    ) -> InfluxDatapoint {
        InfluxDatapoint {
            measurement: measurement.to_owned(),
            tags,
            fields,
            timestamp,
        }
    }

    pub fn try_to_bytes(self) -> Result<Vec<u8>> {
        let mut output = self.measurement.escape();

        let tag_collection = self
            .tags
            .iter()
            .map(|(key, value)| {
                let mut op = key.escape().to_owned();
                op.push('=');
                op.push_str(&value.escape());
                op
            })
            .collect::<Vec<String>>();

        let tags = tag_collection.join(",");
        if !tags.is_empty() {
            output.push_str(",");
            output.push_str(&tags);
        }

        output.push_str(" ");

        let mut field_collection = vec![];

        for (key, field) in self.fields.iter() {
            let value = match field {
                Value::String(s) => Ok(process_string(s)),

                Value::Number(num) => Ok(num.to_string()),

                Value::Bool(b) => Ok(b.to_string()),
                _ => Err(ErrorKind::InvalidInfluxData(
                    "Arrays are not supported for field values".into(),
                )),
            }?;

            let mut op = key.escape().to_owned();
            op.push('=');
            op.push_str(&value);
            field_collection.push(op);
        }

        let fields = field_collection.join(" ");
        output.push_str(&fields);
        output.push(' ');

        output.push_str(&self.timestamp.to_string());

        Ok(output.into())
    }
}

fn process_string(s: &str) -> String {
    let mut out = String::new();
    s.chars().for_each(|ch| match ch {
        c if c == '\\' || c == '\"' => {
            out.push('\\');
            out.push(c);
        }
        c => out.push(c),
    });

    format!("\"{}\"", out)
}

fn parse(data: &str) -> Result<InfluxDatapoint> {
    let mut data = String::from(data);
    loop {
        if let Some(c) = data.pop() {
            if c != '\n' {
                data.push(c);
                break;
            }
        } else {
            return Err(ErrorKind::InvalidInfluxData("Empty.".into()).into());
        }
    }

    let mut chars = data.as_mut_str().chars();
    let (measurement, c) = parse_to_char2(&mut chars, ',', ' ')?;
    let tags = if c == ',' {
        parse_tags(&mut chars)?
    } else {
        HashMap::new()
    };

    let fields = parse_fields(&mut chars)?;
    let timestamp = chars.as_str().parse()?;

    Ok(InfluxDatapoint {
        measurement,
        tags,
        fields,
        timestamp,
    })
}

fn parse_string(chars: &mut Chars) -> Result<(Value, char)> {
    let val = parse_to_char(chars, '"')?;
    match chars.next() {
        Some(',') => Ok((Value::String(val), ',')),
        Some(' ') => Ok((Value::String(val), ' ')),
        _ => Err(ErrorKind::InvalidInfluxData("Unexpected character after string".into()).into()),
    }
}

fn float_or_bool(s: &str) -> Result<Value> {
    match s {
        "t" | "T" | "true" | "True" | "TRUE" => Ok(Value::Bool(true)),
        "f" | "F" | "false" | "False" | "FALSE" => Ok(Value::Bool(false)),
        _ => Ok(num_f(s.parse()?)?),
    }
}
fn parse_value(chars: &mut Chars) -> Result<(Value, char)> {
    let mut res = String::new();
    match chars.next() {
        Some('"') => return parse_string(chars),
        Some(' ') | Some(',') | None => {
            return Err(ErrorKind::InvalidInfluxData("Unexpected end of values".into()).into());
        }
        Some(c) => res.push(c),
    }
    while let Some(c) = chars.next() {
        match c {
            ',' => return Ok((float_or_bool(&res)?, ',')),
            ' ' => return Ok((float_or_bool(&res)?, ' ')),
            'i' => match chars.next() {
                Some(' ') => return Ok((num_i(res.parse()?), ' ')),
                Some(',') => return Ok((num_i(res.parse()?), ',')),
                Some(c) => {
                    return Err(ErrorKind::InvalidInfluxData(format!(
                        "Unexpected character '{}', expected ' ' or ','.",
                        c
                    ))
                    .into());
                }
                None => {
                    return Err(
                        ErrorKind::InvalidInfluxData("Unexpected end of line".into()).into(),
                    );
                }
            },
            '\\' => {
                if let Some(c) = chars.next() {
                    res.push(c);
                }
            }
            _ => res.push(c),
        }
    }
    Err(
        ErrorKind::InvalidInfluxData("Unexpected character or end of value definition".into())
            .into(),
    )
}

fn parse_fields(chars: &mut Chars) -> Result<HashMap<String, Value>> {
    let mut res = HashMap::new();
    loop {
        let key = parse_to_char(chars, '=')?;

        let (val, c) = parse_value(chars)?;
        match c {
            ',' => {
                res.insert(key, val);
            }
            ' ' => {
                res.insert(key, val);
                return Ok(res);
            }
            _ => unreachable!(),
        };
    }
}

fn parse_tags(chars: &mut Chars) -> Result<HashMap<String, String>> {
    let mut res = HashMap::new();
    loop {
        let (key, c) = parse_to_char3(chars, '=', Some(' '), Some(','))?;
        if c != '=' {
            return Err(ErrorKind::InvalidInfluxData("Tag without value".into()).into());
        };
        let (val, c) = parse_to_char3(chars, '=', Some(' '), Some(','))?;
        if c == '=' {
            return Err(ErrorKind::InvalidInfluxData("= found in tag value".into()).into());
        }
        res.insert(key, val);
        if c == ' ' {
            return Ok(res);
        }
    }
}

fn parse_to_char3(
    chars: &mut Chars,
    end1: char,
    end2: Option<char>,
    end3: Option<char>,
) -> Result<(String, char)> {
    let mut res = String::new();
    while let Some(c) = chars.next() {
        match c {
            c if c == end1 => return Ok((res, end1)),
            c if Some(c) == end2 => return Ok((res, c)),
            c if Some(c) == end3 => return Ok((res, c)),
            '\\' => match chars.next() {
                Some(c) if c == '\\' || c == end1 || Some(c) == end2 || Some(c) == end3 => {
                    res.push(c)
                }
                Some(c) => {
                    res.push('\\');
                    res.push(c)
                }
                None => {
                    return Err(ErrorKind::InvalidInfluxData(
                        "non terminated escape sequence".into(),
                    )
                    .into());
                }
            },
            _ => res.push(c),
        }
    }
    Err(ErrorKind::InvalidInfluxData(format!(
        "Expected '{}', '{:?}' or '{:?}' but did not find it",
        end1, end2, end3
    ))
    .into())
}

fn parse_to_char2(chars: &mut Chars, end1: char, end2: char) -> Result<(String, char)> {
    parse_to_char3(chars, end1, Some(end2), None)
}
fn parse_to_char(chars: &mut Chars, end: char) -> Result<String> {
    let (res, _) = parse_to_char3(chars, end, None, None)?;
    Ok(res)
}

fn num_i(n: i64) -> Value {
    Value::Number(Number::from(n))
}

fn num_f(n: f64) -> Result<Value> {
    Ok(Value::Number(Number::from_f64(n as f64).ok_or_else(
        || Error::from("number can not be conveted to float."),
    )?))
}

trait Escaper {
    type Escaped;
    fn escape(&self) -> Self::Escaped;
    //  fn unescape(Self::Output) -> self;
}

impl Escaper for String {
    type Escaped = String;
    // type Unescaped = String;

    fn escape(&self) -> Self::Escaped {
        let mut out = String::new();
        self.chars().for_each(|ch| match ch {
            c if c == ',' || c == ' ' || c == '\\' || c == '=' || c == '\"' => {
                out.push('\\');
                out.push(c);
            }
            c => out.push(c),
        });

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use maplit::hashmap;
    use serde_json::{self, json, Value};

    //    use test::Bencher;

    fn f(n: f64) -> Value {
        num_f(n).expect("failed to create float")
    }
    #[test]
    fn parse_simple() {
        let s = "weather,location=us-midwest temperature=82 1465839830100400200";
        let r = InfluxDatapoint {
            measurement: "weather".to_string(),
            tags: hashmap! {
                "location".to_string() => "us-midwest".to_string()
            },
            fields: hashmap! {
                "temperature".to_string() => f(82.0)
            },
            timestamp: 1465839830100400200,
        };
        assert_eq!(Ok(r), parse(s))
    }
    #[test]
    fn parse_simple2() {
        let s = "weather,location=us-midwest,season=summer temperature=82 1465839830100400200";
        let r = InfluxDatapoint {
            measurement: "weather".to_string(),
            tags: hashmap! {
                "location".to_string() => "us-midwest".to_string(),
                "season".to_string() => "summer".to_string()
            },
            fields: hashmap! {
                "temperature".to_string() => f(82.0)
            },
            timestamp: 1465839830100400200,
        };
        assert_eq!(Ok(r), parse(s))
    }
    #[test]
    fn parse_simple3() {
        let s =
            "weather,location=us-midwest temperature=82,bug_concentration=98 1465839830100400200";
        let r = InfluxDatapoint {
            measurement: "weather".to_string(),
            tags: hashmap! {
                "location".to_string() => "us-midwest".to_string()
            },
            fields: hashmap! {
                "temperature".to_string() => f(82.0),
                "bug_concentration".to_string() => f(98.0),

            },
            timestamp: 1465839830100400200,
        };
        assert_eq!(Ok(r), parse(s))
    }

    #[test]
    fn parse_float_value() {
        let s = "weather temperature=82 1465839830100400200";
        let r = InfluxDatapoint {
            measurement: "weather".to_string(),
            tags: HashMap::new(),
            fields: hashmap! {
                "temperature".to_string() => f(82.0)
            },
            timestamp: 1465839830100400200,
        };
        assert_eq!(Ok(r), parse(s))
    }
    #[test]
    fn parse_int_value() {
        let s = "weather temperature=82i 1465839830100400200";
        let r = InfluxDatapoint {
            measurement: "weather".to_string(),
            tags: HashMap::new(),
            fields: hashmap! {
                "temperature".to_string() => num_i(82)
            },
            timestamp: 1465839830100400200,
        };
        assert_eq!(Ok(r), parse(s))
    }
    #[test]
    fn parse_str_value() {
        let s = "weather,location=us-midwest temperature=\"too warm\" 1465839830100400200";
        let r = InfluxDatapoint {
            measurement: "weather".to_string(),
            tags: hashmap! {
                "location".to_string() => "us-midwest".to_string()
            },
            fields: hashmap! {
                "temperature".to_string() => Value::String("too warm".to_string())
            },
            timestamp: 1465839830100400200,
        };
        assert_eq!(Ok(r), parse(s))
    }
    #[test]
    fn parse_true_value() {
        let sarr = &[
            "weather,location=us-midwest too_hot=true 1465839830100400200",
            "weather,location=us-midwest too_hot=True 1465839830100400200",
            "weather,location=us-midwest too_hot=TRUE 1465839830100400200",
            "weather,location=us-midwest too_hot=t 1465839830100400200",
            "weather,location=us-midwest too_hot=T 1465839830100400200",
        ];
        let r = InfluxDatapoint {
            measurement: "weather".to_string(),
            tags: hashmap! {
                "location".to_string() => "us-midwest".to_string()
            },
            fields: hashmap! {
                "too_hot".to_string() => Value::Bool(true)
            },
            timestamp: 1465839830100400200,
        };
        for s in sarr {
            assert_eq!(Ok(r.clone()), parse(s))
        }
    }
    #[test]
    fn parse_false_value() {
        let sarr = &[
            "weather,location=us-midwest too_hot=false 1465839830100400200",
            "weather,location=us-midwest too_hot=False 1465839830100400200",
            "weather,location=us-midwest too_hot=FALSE 1465839830100400200",
            "weather,location=us-midwest too_hot=f 1465839830100400200",
            "weather,location=us-midwest too_hot=F 1465839830100400200",
        ];
        let r = InfluxDatapoint {
            measurement: "weather".to_string(),
            tags: hashmap! {
                "location".to_string() => "us-midwest".to_string()
            },
            fields: hashmap! {
                "too_hot".to_string() => Value::Bool(false)
            },
            timestamp: 1465839830100400200,
        };
        for s in sarr {
            assert_eq!(Ok(r.clone()), parse(s))
        }
    }
    // Note: Escapes are escaped twice since we need one level of escaping for rust!
    #[test]
    fn parse_escape1() {
        let s = "weather,location=us\\,midwest temperature=82 1465839830100400200";
        let r = InfluxDatapoint {
            measurement: "weather".to_string(),
            tags: hashmap! {
                "location".to_string() => "us,midwest".to_string()
            },
            fields: hashmap! {
                "temperature".to_string() => f(82.0)
            },
            timestamp: 1465839830100400200,
        };
        assert_eq!(Ok(r), parse(s))
    }

    #[test]
    fn parse_escape2() {
        let s = "weather,location=us-midwest temp\\=rature=82 1465839830100400200";
        let r = InfluxDatapoint {
            measurement: "weather".to_string(),
            tags: hashmap! {
                "location".to_string() => "us-midwest".to_string()
            },
            fields: hashmap! {
                "temp=rature".to_string() => f(82.0)
            },
            timestamp: 1465839830100400200,
        };
        assert_eq!(Ok(r), parse(s))
    }
    #[test]
    fn parse_escape3() {
        let s = "weather,location\\ place=us-midwest temperature=82 1465839830100400200";
        let r = InfluxDatapoint {
            measurement: "weather".to_string(),
            tags: hashmap! {
                "location place".to_string() => "us-midwest".to_string()
            },
            fields: hashmap! {
                "temperature".to_string() => f(82.0)
            },
            timestamp: 1465839830100400200,
        };
        assert_eq!(Ok(r), parse(s))
    }

    #[test]
    fn parse_escape4() {
        let s = "wea\\,ther,location=us-midwest temperature=82 1465839830100400200";
        let r = InfluxDatapoint {
            measurement: "wea,ther".to_string(),
            tags: hashmap! {
                "location".to_string() => "us-midwest".to_string()
            },
            fields: hashmap! {
                "temperature".to_string() => f(82.0)
            },
            timestamp: 1465839830100400200,
        };
        assert_eq!(Ok(r), parse(s))
    }
    #[test]
    fn parse_escape5() {
        let s = "wea\\ ther,location=us-midwest temperature=82 1465839830100400200";
        let r = InfluxDatapoint {
            measurement: "wea ther".to_string(),
            tags: hashmap! {
                "location".to_string() => "us-midwest".to_string()
            },
            fields: hashmap! {
                "temperature".to_string() => f(82.0)
            },
            timestamp: 1465839830100400200,
        };
        assert_eq!(Ok(r), parse(s))
    }

    #[test]
    fn parse_escape6() {
        let s = "weather,location=us-midwest temperature=\"too\\\"hot\\\"\" 1465839830100400200";
        let r = InfluxDatapoint {
            measurement: "weather".to_string(),
            tags: hashmap! {
                "location".to_string() => "us-midwest".to_string()
            },
            fields: hashmap! {
                "temperature".to_string() => Value::String("too\"hot\"".to_string())
            },
            timestamp: 1465839830100400200,
        };
        assert_eq!(Ok(r), parse(s))
    }

    #[test]
    fn parse_escape7() {
        let s = "weather,location=us-midwest temperature_str=\"too hot/cold\" 1465839830100400201";
        let r = InfluxDatapoint {
            measurement: "weather".to_string(),
            tags: hashmap! {
                "location".to_string() => "us-midwest".to_string()
            },
            fields: hashmap! {
                "temperature_str".to_string() => Value::String("too hot/cold".to_string())
            },
            timestamp: 1465839830100400201,
        };
        assert_eq!(Ok(r), parse(s));
    }

    #[test]
    fn parse_escape8() {
        let s = "weather,location=us-midwest temperature_str=\"too hot\\cold\" 1465839830100400202";
        let r = InfluxDatapoint {
            measurement: "weather".to_string(),
            tags: hashmap! {
                "location".to_string() => "us-midwest".to_string()
            },
            fields: hashmap! {
                "temperature_str".to_string() => Value::String("too hot\\cold".to_string())
            },
            timestamp: 1465839830100400202,
        };
        assert_eq!(Ok(r), parse(s))
    }

    #[test]
    fn parse_escape9() {
        let s =
            "weather,location=us-midwest temperature_str=\"too hot\\\\cold\" 1465839830100400203";
        let r = InfluxDatapoint {
            measurement: "weather".to_string(),
            tags: hashmap! {
                "location".to_string() => "us-midwest".to_string()
            },
            fields: hashmap! {
                "temperature_str".to_string() => Value::String("too hot\\cold".to_string())
            },
            timestamp: 1465839830100400203,
        };
        assert_eq!(Ok(r), parse(s))
    }

    #[test]
    fn parse_escape10() {
        let s =
            "weather,location=us-midwest temperature_str=\"too hot\\\\\\cold\" 1465839830100400204";
        let r = InfluxDatapoint {
            measurement: "weather".to_string(),
            tags: hashmap! {
                "location".to_string() => "us-midwest".to_string()
            },
            fields: hashmap! {
                "temperature_str".to_string() => Value::String("too hot\\\\cold".to_string())
            },
            timestamp: 1465839830100400204,
        };
        assert_eq!(Ok(r), parse(s))
    }
    #[test]
    fn parse_escape11() {
        let s = "weather,location=us-midwest temperature_str=\"too hot\\\\\\\\cold\" 1465839830100400205";
        let r = InfluxDatapoint {
            measurement: "weather".to_string(),
            tags: hashmap! {


                "location".to_string() => "us-midwest".to_string()
            },
            fields: hashmap! {
                "temperature_str".to_string() => Value::String("too hot\\\\cold".to_string())
            },
            timestamp: 1_465_839_830_100_400_205,
        };
        assert_eq!(Ok(r), parse(s))
    }
    #[test]
    fn parse_escape12() {
        let s = "weather,location=us-midwest temperature_str=\"too hot\\\\\\\\\\cold\" 1465839830100400206";
        let r = InfluxDatapoint {
            measurement: "weather".to_string(),
            tags: hashmap! {
                "location".to_string() => "us-midwest".to_string()
            },
            fields: hashmap! {
                "temperature_str".to_string() => Value::String("too hot\\\\\\cold".to_string())
            },
            timestamp: 1465839830100400206,
        };
        assert_eq!(Ok(r), parse(s))
    }

    #[test]
    fn unparse_test() {
        let s = "weather,location=us-midwest temperature=82 1465839830100400200";
        let d = parse(s).expect("failed to parse");
        // This is a bit ugly but to make a sensible compairison we got to convert the data
        // from an object to json to an object
        let j: serde_json::Value = serde_json::from_str(
            serde_json::to_string(&d)
                .expect("failed to encode")
                .as_str(),
        )
        .expect("failed to decode");
        let e: serde_json::Value = json!({
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

        let decoded = codec.decode(EventValue::Raw(s)).expect("failed to decode");

        let e: serde_json::Value = json!({
            "measurement": "weather",
            "tags": hashmap!{"location" => "us-midwest"},
            "fields": hashmap!{"temperature" => 82.0},
            "timestamp": 1465839830100400200i64
        });
        match &decoded {
            EventValue::JSON(j) => assert_eq!(j, &e),
            _ => unreachable!(),
        }
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

        let encoded = codec.encode(EventValue::JSON(s)).expect("failed to encode");

        let influx = InfluxDatapoint::new(
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

        match &encoded {
            EventValue::Raw(r) => assert_eq!(*r, influx.try_to_bytes().expect("failed to encode")),
            _ => unreachable!(),
        }
    }

    #[test]
    pub fn encode_mixed_bag() {
        let tags: HashMap<String, String> = HashMap::new();
        let s = json!({
            "measurement": r#"wea,\ ther"#,
            "tags": tags,
            "fields": hashmap!{"temp=erature" => f(82.0), r#"too\ \\\"hot""# => Value::Bool(true)},
            "timestamp": 1465839830100400200u64
        });

        let codec = Influx {};

        let encoded = codec.encode(EventValue::JSON(s)).expect("failed to encode");

        match &encoded {
            EventValue::Raw(r) => {
                let raw =
                    r#"wea\,\\\ ther temp\=erature=82 too\\\ \\\\\\\"hot\"=true 1465839830100400200"#;

                assert_eq!(str::from_utf8(r).expect("failed to convert utf8"), raw);
            }

            _ => unreachable!(),
        }
    }

    pub fn get_data_for_tests() -> [(Vec<u8>, Value, &'static str); 11] {
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
                     "fields": hashmap!{"temperature_str" => Value::String("too\\ hot\\cold".to_string())},
                     "timestamp": 1465839830100400203i64
                 }),
                 "case 5"
             ),
            (

                b"weather,location=us-midwest temperature_str=\"too hot/cold\" 1465839830100400202".to_vec(),
                json!({

                    "measurement": "weather",
                    "tags": hashmap!{"location" => "us-midwest"},
                    "fields": hashmap!{"temperature_str" => Value::String(r#"too hot/cold"#.to_string())},
                    "timestamp": 1465839830100400202i64
                }),
                "case 6"
            ),

            (
                br#"weather,location=us-midwest temperature_str="too hot\\cold" 1465839830100400203"#.to_vec(),

                json!({
                    "measurement": "weather",
                    "tags": hashmap!{"location" => "us-midwest"},
                    "fields": hashmap!{"temperature_str" => Value::String(r#"too hot\cold"#.to_string())},
                    "timestamp": 1465839830100400203i64
                }),
                "case 7"

                ),
            (
                br#"weather,location=us-midwest temperature_str="too hot\\\cold" 1465839830100400204"#.to_vec(),

                json!({
                    "measurement": "weather",
                    "tags": hashmap!{"location" => "us-midwest"},
                    "fields": hashmap!{"temperature_str" => Value::String(r#"too hot\\cold"#.to_string())},
                    "timestamp": 1465839830100400204i64
                }),
                "case 8"

            ),

            (
                b"weather,location=us-midwest temperature_str=\"too hot\\\\\\\\cold\" 1465839830100400205".to_vec(),

                json!({
                    "measurement": "weather",
                    "tags": hashmap!{"location" => "us-midwest"},
                    "fields": hashmap!{"temperature_str" => Value::String("too hot\\\\cold".to_string())},
                    "timestamp": 1465839830100400205i64
                }),
                "case 9"

            ),

            (
                b"weather,location=us-midwest temperature_str=\"too hot\\\\\\\\\\cold\" 1465839830100400206".to_vec(),
                json!({
                    "measurement": "weather",
                    "tags": hashmap!{"location" => "us-midwest"},
                    "fields": hashmap!{"temperature_str" => Value::String("too hot\\\\\\cold".to_string())},
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
                .encode(EventValue::JSON(case.1.clone()))
                .expect("failed to encode");

            match &encoded {
                EventValue::Raw(r) => {
                    let decoded = codec
                        .decode(EventValue::Raw(r.clone()))
                        .expect("failed to dencode");

                    match &decoded {
                        EventValue::JSON(j) => {
                            if j != &case.1 {
                                println!("{} fails while decoding", &case.2);
                                assert_eq!(j, &case.1);
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                _ => unreachable!(),
            }
        });
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
