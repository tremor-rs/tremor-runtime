// Copyright 2018, Wayfair GmbH
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

use crate::errors::*;
use crate::pipeline::prelude::*;
use serde_json::{Number, Value};
use std::collections::HashMap;
use std::f64;
use std::str::{self, Chars};

/// The Raw Parser is a simple parser that performs no action on the
/// data and just hands on `raw`

#[derive(Debug, Clone)]
pub struct Parser {}
impl Parser {
    pub fn create(_opts: &ConfValue) -> Result<Self> {
        Ok(Self {})
    }
}

impl Opable for Parser {
    fn on_event(&mut self, event: EventData) -> EventResult {
        ensure_type!(event, "parse::influx", ValueType::Raw);
        let res = event.replace_value(|val| {
            if let EventValue::Raw(raw) = val {
                let s = str::from_utf8(&raw)?;
                let parsed = parse(s)?;
                match serde_json::to_value(parsed) {
                    Ok(val) => Ok(EventValue::JSON(val)),
                    Err(_e) => Err(ErrorKind::InvalidInfluxData(
                        "Could not create structure.".into(),
                    )
                    .into()),
                }
            } else {
                unreachable!()
            }
        });

        match res {
            Ok(n) => next!(n),
            Err(e) => e,
        }
    }
    opable_types!(ValueType::Raw, ValueType::JSON);
}

#[derive(PartialEq, Clone, Debug, Serialize)]
pub struct InfluxDatapoint {
    measurement: String,
    tags: HashMap<String, String>,
    fields: HashMap<String, Value>,
    timestamp: u64,
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
        _ => Ok(num_f(s.parse()?)),
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
                    return Err(ErrorKind::InvalidInfluxData("Unexpected end of line".into()).into());
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
            c if Some(c) == end2 => return Ok((res, end2.unwrap())),
            c if Some(c) == end3 => return Ok((res, end3.unwrap())),
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

fn num_f(n: f64) -> Value {
    Value::Number(Number::from_f64(n as f64).unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use maplit;
    use serde_json::{self, Number, Value};
    //    use test::Bencher;

    fn num_f(n: f64) -> Value {
        Value::Number(Number::from_f64(n).unwrap())
    }
    fn num_i(n: i64) -> Value {
        Value::Number(Number::from(n))
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
                "temperature".to_string() => num_f(82.0)
            },
            timestamp: 1465839830100400200,
        };
        assert_eq!(r, parse(s).unwrap())
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
                "temperature".to_string() => num_f(82.0)
            },
            timestamp: 1465839830100400200,
        };
        assert_eq!(r, parse(s).unwrap())
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
                "temperature".to_string() => num_f(82.0),
                "bug_concentration".to_string() => num_f(98.0)
            },
            timestamp: 1465839830100400200,
        };
        assert_eq!(r, parse(s).unwrap())
    }

    #[test]
    fn parse_float_value() {
        let s = "weather temperature=82 1465839830100400200";
        let r = InfluxDatapoint {
            measurement: "weather".to_string(),
            tags: HashMap::new(),
            fields: hashmap! {
                "temperature".to_string() => num_f(82.0)
            },
            timestamp: 1465839830100400200,
        };
        assert_eq!(r, parse(s).unwrap())
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
        assert_eq!(r, parse(s).unwrap())
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
        assert_eq!(r, parse(s).unwrap())
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
            assert_eq!(r, parse(s).unwrap())
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
            assert_eq!(r, parse(s).unwrap())
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
                "temperature".to_string() => num_f(82.0)
            },
            timestamp: 1465839830100400200,
        };
        assert_eq!(r, parse(s).unwrap())
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
                "temp=rature".to_string() => num_f(82.0)
            },
            timestamp: 1465839830100400200,
        };
        assert_eq!(r, parse(s).unwrap())
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
                "temperature".to_string() => num_f(82.0)
            },
            timestamp: 1465839830100400200,
        };
        assert_eq!(r, parse(s).unwrap())
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
                "temperature".to_string() => num_f(82.0)
            },
            timestamp: 1465839830100400200,
        };
        assert_eq!(r, parse(s).unwrap())
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
                "temperature".to_string() => num_f(82.0)
            },
            timestamp: 1465839830100400200,
        };
        assert_eq!(r, parse(s).unwrap())
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
        assert_eq!(r, parse(s).unwrap())
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
        assert_eq!(r, parse(s).unwrap())
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
        assert_eq!(r, parse(s).unwrap())
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
        assert_eq!(r, parse(s).unwrap())
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
        assert_eq!(r, parse(s).unwrap())
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
            timestamp: 1465839830100400205,
        };
        assert_eq!(r, parse(s).unwrap())
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
        assert_eq!(r, parse(s).unwrap())
    }

    #[test]
    fn unparse_test() {
        let s = "weather,location=us-midwest temperature=82 1465839830100400200";
        let d = parse(s).unwrap();
        // This is a bit ugly but to make a sensible compairison we got to convert the data
        // from an object to json to an object
        let j: serde_json::Value =
            serde_json::from_str(serde_json::to_string(&d).unwrap().as_str()).unwrap();
        let e: serde_json::Value = json!({
            "measurement": "weather",
            "tags": hashmap!{"location" => "us-midwest"},
            "fields": hashmap!{"temperature" => 82.0},
            "timestamp": 1465839830100400200i64
        });
        assert_eq!(e, j)
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
    "weather,location=us-midwest too_hot=F 1465839830100400200",
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
