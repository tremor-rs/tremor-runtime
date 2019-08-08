// Copyright 2018-2019, Wayfair GmbH
//
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

use crate::errors::*;
use halfbrown::HashMap;
use simd_json::value::borrowed::{Map, Value};
use simd_json::value::ValueTrait;
use std::borrow::Cow;
use std::str::Chars;

pub fn try_to_bytes<'input>(v: &Value<'input>) -> Option<Vec<u8>> {
    let mut output = v.get("measurement")?.as_string()?.escape();

    let mut tag_collection = v
        .get("tags")?
        .as_object()?
        .iter()
        .filter_map(|(key, value)| {
            let mut op = key.escape().to_owned();
            op.push('=');
            op.push_str(&value.as_string()?.escape());
            Some(op)
        })
        .collect::<Vec<String>>();
    tag_collection.sort();

    let tags = tag_collection.join(",");
    if !tags.is_empty() {
        output.push_str(",");
        output.push_str(&tags);
    }

    output.push_str(" ");
    let fields = v.get("fields")?.as_object()?;
    let mut field_collection = Vec::with_capacity(fields.len());
    for (key, field) in fields.iter() {
        let value = match field {
            Value::String(s) => process_string(s),
            Value::F64(num) => num.to_string(),
            Value::I64(num) => num.to_string() + "i",
            Value::Bool(b) => b.to_string(),
            _ => return None,
        };

        let mut op = key.escape().to_owned();
        op.push('=');
        op.push_str(&value);
        field_collection.push(op);
    }
    field_collection.sort();
    let fields = field_collection.join(",");
    output.push_str(&fields);
    output.push(' ');

    output.push_str(&v.get("timestamp")?.as_u64()?.to_string());

    Some(output.into())
}

fn process_string(s: &str) -> String {
    let mut out = String::from(r#"""#);
    s.chars().for_each(|ch| match ch {
        c if c == '\\' || c == '\"' => {
            out.push('\\');
            out.push(c);
        }
        c => out.push(c),
    });
    out.push('"');
    out
}

pub fn parse<'input>(data: &'input str, ingest_ns: u64) -> Result<Value<'input>> {
    let data = data.trim_end();
    if data.is_empty() {
        return Err(ErrorKind::InvalidInfluxData("Empty.".into()).into());
    }

    let mut chars = data.chars();
    let (measurement, c) = parse_to_char2(&mut chars, ',', ' ')?;
    let tags = if c == ',' {
        parse_tags(&mut chars)?
    } else {
        Map::new()
    };

    let fields = parse_fields(&mut chars)?;
    let timestamp_str = chars.as_str();
    let timestamp = if timestamp_str.is_empty() {
        ingest_ns
    } else {
        chars.as_str().parse()?
    };

    let mut m = Map::with_capacity(4);
    m.insert_nocheck("measurement".into(), Value::String(measurement));
    m.insert_nocheck("tags".into(), Value::Object(tags));
    m.insert_nocheck("fields".into(), Value::Object(fields));
    m.insert_nocheck("timestamp".into(), timestamp.into());
    Ok(Value::Object(m))
}

fn parse_string<'input>(chars: &mut Chars) -> Result<(Value<'input>, Option<char>)> {
    let val = parse_to_char(chars, '"')?;
    match chars.next() {
        c @ Some(',') | c @ Some(' ') | c @ None => Ok((Value::String(val), c)),
        _ => Err(ErrorKind::InvalidInfluxData("Unexpected character after string".into()).into()),
    }
}

fn float_or_bool(s: &str) -> Result<Value<'static>> {
    match s {
        "t" | "T" | "true" | "True" | "TRUE" => Ok(Value::from(true)),
        "f" | "F" | "false" | "False" | "FALSE" => Ok(Value::from(false)),
        _ => Ok(Value::F64(
            s.parse()
                .map_err(|_| ErrorKind::InvalidInfluxData(s.to_owned()))?,
        )),
    }
}
fn parse_value<'input>(chars: &mut Chars) -> Result<(Value<'input>, Option<char>)> {
    let mut res = String::new();
    match chars.next() {
        Some('"') => return parse_string(chars),
        Some(' ') | Some(',') | None => {
            return Err(ErrorKind::InvalidInfluxData("Unexpected end of values".into()).into());
        }
        Some(c) => res.push(c),
    }
    loop {
        match chars.next() {
            c @ Some(',') | c @ Some(' ') | c @ None => return Ok((float_or_bool(&res)?, c)),
            Some('i') => match chars.next() {
                c @ Some(' ') | c @ Some(',') | c @ None => {
                    return Ok((Value::I64(res.parse()?), c))
                }
                Some(c) => {
                    return Err(ErrorKind::InvalidInfluxData(format!(
                        "Unexpected character '{}', expected ' ' or ','.",
                        c
                    ))
                    .into());
                }
            },
            Some('\\') => {
                if let Some(c) = chars.next() {
                    res.push(c);
                } else {
                    return Err(ErrorKind::InvalidInfluxData("Escape without value.".into()).into());
                }
            }
            Some(c) => res.push(c),
        }
    }
}

fn parse_fields<'input>(chars: &mut Chars) -> Result<Map<'input>> {
    let mut res = HashMap::new();
    loop {
        let key = parse_to_char(chars, '=')?;

        let (val, c) = parse_value(chars)?;
        match c {
            Some(',') => {
                res.insert(key, val);
            }
            Some(' ') | None => {
                res.insert(key, val);
                return Ok(res);
            }
            _ => unreachable!(),
        };
    }
}

fn parse_tags<'input>(chars: &mut Chars) -> Result<Map<'input>> {
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
        res.insert(key, Value::String(val));
        if c == ' ' {
            return Ok(res);
        }
    }
}

fn parse_to_char3<'input>(
    chars: &mut Chars,
    end1: char,
    end2: Option<char>,
    end3: Option<char>,
) -> Result<(Cow<'input, str>, char)> {
    let mut res = String::new();
    while let Some(c) = chars.next() {
        match c {
            c if c == end1 => return Ok((res.into(), end1)),
            c if Some(c) == end2 => return Ok((res.into(), c)),
            c if Some(c) == end3 => return Ok((res.into(), c)),
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

fn parse_to_char2<'input>(
    chars: &mut Chars,
    end1: char,
    end2: char,
) -> Result<(Cow<'input, str>, char)> {
    parse_to_char3(chars, end1, Some(end2), None)
}
fn parse_to_char<'input>(chars: &mut Chars, end: char) -> Result<Cow<'input, str>> {
    let (res, _) = parse_to_char3(chars, end, None, None)?;
    Ok(res)
}

trait Escaper {
    type Escaped;
    fn escape(&self) -> Self::Escaped;
}
impl<'input> Escaper for Cow<'input, str> {
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

    use simd_json::json;

    #[test]
    fn parse_simple() {
        let s = "weather,location=us-midwest temperature=82 1465839830100400200";
        let r: Value = json!({
            "measurement": "weather",
            "tags": {
                "location": "us-midwest"
            },
            "fields": {
                "temperature": 82.0
            },
            "timestamp": 1465839830100400200i64,
        })
        .into();
        assert_eq!(Ok(r), parse(s, 0))
    }
    #[test]
    fn parse_simple2() {
        let s = "weather,location=us-midwest,season=summer temperature=82 1465839830100400200";
        let r: Value = json!({
            "measurement": "weather",
            "tags": {
                "location": "us-midwest",
                "season": "summer"
            },
            "fields": {
                "temperature": 82.0
            },
            "timestamp": 1465839830100400200i64,
        })
        .into();
        assert_eq!(Ok(r), parse(s, 0))
    }
    #[test]
    fn parse_simple3() {
        let s =
            "weather,location=us-midwest temperature=82,bug_concentration=98 1465839830100400200";
        let r: Value = json!({
            "measurement": "weather",
            "tags": {
                "location": "us-midwest"
            },
            "fields": {
                "temperature": 82.0,
                "bug_concentration": 98.0,

            },
            "timestamp": 1465839830100400200i64,
        })
        .into();
        assert_eq!(Ok(r), parse(s, 0))
    }

    #[test]
    fn parse_no_timestamp() {
        let s = "weather temperature=82i";
        let parsed = parse(s, 1465839830100400200u64).expect("failed to parse");
        let r: Value = json!({
            "measurement": "weather",
            "tags": {},
            "fields": {
                "temperature": 82
            },
            "timestamp": 1465839830100400200i64,
        })
        .into();
        assert_eq!(r, parsed);
    }

    #[test]
    fn parse_float_value() {
        let s = "weather temperature=82 1465839830100400200";
        let r: Value = json!({
            "measurement": "weather",
            "tags": {},
            "fields": {
                "temperature": 82.0
            },
            "timestamp": 1465839830100400200i64,
        })
        .into();
        assert_eq!(Ok(r), parse(s, 0))
    }

    #[test]
    fn parse_int_value() {
        let s = "weather temperature=82i 1465839830100400200";
        let r: Value = json!({
            "measurement": "weather",
            "tags": {},
            "fields": {
                "temperature": 82
            },
            "timestamp": 1465839830100400200i64,
        })
        .into();
        assert_eq!(Ok(r), parse(s, 0))
    }

    #[test]
    fn parse_str_value() {
        let s = "weather,location=us-midwest temperature=\"too warm\" 1465839830100400200";
        let r: Value = json!({
            "measurement": "weather",
            "tags": {
                "location": "us-midwest"
            },
            "fields": {
                "temperature": "too warm"
            },
            "timestamp": 1465839830100400200i64,
        })
        .into();
        assert_eq!(Ok(r), parse(s, 0))
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
        let r: Value = json!({
            "measurement": "weather",
            "tags": {
                "location": "us-midwest"
            },
            "fields": {
                "too_hot": true
            },
            "timestamp": 1465839830100400200i64,
        })
        .into();
        for s in sarr {
            assert_eq!(Ok(r.clone()), parse(s, 0))
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
        let r: Value = json!({
            "measurement": "weather",
            "tags": {
                "location": "us-midwest"
            },
            "fields": {
                "too_hot": false
            },
            "timestamp": 1465839830100400200i64,
        })
        .into();
        for s in sarr {
            assert_eq!(Ok(r.clone()), parse(s, 0))
        }
    }
    // Note: Escapes are escaped twice since we need one level of escaping for rust!
    #[test]
    fn parse_escape1() {
        let s = "weather,location=us\\,midwest temperature=82 1465839830100400200";
        let r: Value = json!({
            "measurement": "weather",
            "tags": {
                "location": "us,midwest"
            },
            "fields": {
                "temperature": 82.0
            },
            "timestamp": 1465839830100400200i64,
        })
        .into();
        assert_eq!(Ok(r), parse(s, 0))
    }

    #[test]
    fn parse_escape2() {
        let s = "weather,location=us-midwest temp\\=rature=82 1465839830100400200";
        let r: Value = json!({
            "measurement": "weather",
            "tags": {
                "location": "us-midwest"
            },
            "fields": {
                "temp=rature": 82.0
            },
            "timestamp": 1465839830100400200i64,
        })
        .into();
        assert_eq!(Ok(r), parse(s, 0))
    }
    #[test]
    fn parse_escape3() {
        let s = "weather,location\\ place=us-midwest temperature=82 1465839830100400200";
        let r: Value = json!({
            "measurement": "weather",
            "tags": {
                "location place": "us-midwest"
            },
            "fields": {
                "temperature": 82.0
            },
            "timestamp": 1465839830100400200i64,
        })
        .into();
        assert_eq!(Ok(r), parse(s, 0))
    }

    #[test]
    fn parse_escape4() {
        let s = "wea\\,ther,location=us-midwest temperature=82 1465839830100400200";
        let r: Value = json!({
            "measurement": "wea,ther",
            "tags": {
                "location": "us-midwest"
            },
            "fields": {
                "temperature": 82.0
            },
            "timestamp": 1465839830100400200i64,
        })
        .into();
        assert_eq!(Ok(r), parse(s, 0))
    }
    #[test]
    fn parse_escape5() {
        let s = "wea\\ ther,location=us-midwest temperature=82 1465839830100400200";
        let r: Value = json!({
            "measurement": "wea ther",
            "tags": {
                "location": "us-midwest"
            },
            "fields": {
                "temperature": 82.0
            },
            "timestamp": 1465839830100400200i64,
        })
        .into();
        assert_eq!(Ok(r), parse(s, 0))
    }

    #[test]
    fn parse_escape6() {
        let s = "weather,location=us-midwest temperature=\"too\\\"hot\\\"\" 1465839830100400200";
        let r: Value = json!({
            "measurement": "weather",
            "tags": {
                "location": "us-midwest"
            },
            "fields": {
                "temperature": "too\"hot\""
            },
            "timestamp": 1465839830100400200i64,
        })
        .into();
        assert_eq!(Ok(r), parse(s, 0))
    }

    #[test]
    fn parse_escape7() {
        let s = "weather,location=us-midwest temperature_str=\"too hot/cold\" 1465839830100400201";
        let r: Value = json!({
            "measurement": "weather",
            "tags": {
                "location": "us-midwest"
            },
            "fields": {
                "temperature_str": "too hot/cold"
            },
            "timestamp": 1465839830100400201i64,
        })
        .into();
        assert_eq!(Ok(r), parse(s, 0));
    }

    #[test]
    fn parse_escape8() {
        let s = "weather,location=us-midwest temperature_str=\"too hot\\cold\" 1465839830100400202";
        let r: Value = json!({
            "measurement": "weather",
            "tags": {
                "location": "us-midwest"
            },
            "fields": {
                "temperature_str": "too hot\\cold"
            },
            "timestamp": 1465839830100400202i64,
        })
        .into();
        assert_eq!(Ok(r), parse(s, 0))
    }

    #[test]
    fn parse_escape9() {
        let s =
            "weather,location=us-midwest temperature_str=\"too hot\\\\cold\" 1465839830100400203";
        let r: Value = json!({
            "measurement": "weather",
            "tags": {
                "location": "us-midwest"
            },
            "fields": {
                "temperature_str": "too hot\\cold"
            },
            "timestamp": 1465839830100400203i64,
        })
        .into();
        assert_eq!(Ok(r), parse(s, 0))
    }

    #[test]
    fn parse_escape10() {
        let s =
            "weather,location=us-midwest temperature_str=\"too hot\\\\\\cold\" 1465839830100400204";
        let r: Value = json!({
            "measurement": "weather",
            "tags": {
                "location": "us-midwest"
            },
            "fields": {
                "temperature_str": "too hot\\\\cold"
            },
            "timestamp": 1465839830100400204i64,
        })
        .into();
        assert_eq!(Ok(r), parse(s, 0))
    }
    #[test]
    fn parse_escape11() {
        let s = "weather,location=us-midwest temperature_str=\"too hot\\\\\\\\cold\" 1465839830100400205";
        let r: Value = json!({
            "measurement": "weather",
            "tags": {


                "location": "us-midwest"
            },
            "fields": {
                "temperature_str": "too hot\\\\cold"
            },
            "timestamp": 1_465_839_830_100_400_205i64,
        })
        .into();
        assert_eq!(Ok(r), parse(s, 0))
    }
    #[test]
    fn parse_escape12() {
        let s = "weather,location=us-midwest temperature_str=\"too hot\\\\\\\\\\cold\" 1465839830100400206";
        let r: Value = json!({
            "measurement": "weather",
            "tags": {
                "location": "us-midwest"
            },
            "fields": {
                "temperature_str": "too hot\\\\\\cold"
            },
            "timestamp": 1465839830100400206i64,
        })
        .into();
        assert_eq!(Ok(r), parse(s, 0))
    }

}
