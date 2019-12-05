// Copyright 2018-2020, Wayfair GmbH
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
use simd_json::value::borrowed::{Object, Value};
use simd_json::value::Value as ValueTrait;
use std::borrow::Cow;
use std::io::Write;
use std::str::Chars;

// taken from https://github.com/simd-lite/simdjson-rs/blob/f9af58334b8e1133f2d27a4f34a57d9576eebfff/src/value/generator.rs#L103

#[inline]
fn write_escaped_value<W: Write>(writer: &mut W, string: &[u8]) -> Option<()> {
    let mut start = 0;

    writer.write_all(&[b'"']).ok()?;
    for (index, ch) in string.iter().enumerate().skip(start) {
        let ch = *ch;
        if ch == b'"' || ch == b'\\' {
            writer.write_all(&string[start..index]).ok()?;
            writer.write_all(&[b'\\', ch]).ok()?;
            start = index + 1;
        }
    }
    writer.write_all(&string[start..]).ok()?;
    writer.write_all(&[b'"']).ok()?;
    Some(())
}

#[inline]
fn write_escaped_key<W: Write>(writer: &mut W, string: &[u8]) -> Option<()> {
    let mut start = 0;

    for (index, ch) in string.iter().enumerate().skip(start) {
        let ch = *ch;
        if ch == b'"' || ch == b'\\' || ch == b',' || ch == b' ' || ch == b'=' {
            writer.write_all(&string[start..index]).ok()?;
            writer.write_all(&[b'\\', ch]).ok()?;
            start = index + 1;
        }
    }
    writer.write_all(&string[start..]).ok()?;
    Some(())
}

pub fn try_to_bytes<'input>(v: &Value<'input>) -> Option<Vec<u8>> {
    let mut output: Vec<u8> = Vec::with_capacity(512);
    write_escaped_key(&mut output, v.get("measurement")?.as_str()?.as_bytes())?;
    //let mut output: String = v.get("measurement")?.as_str()?.escape();

    let mut tag_collection = v
        .get("tags")?
        .as_object()?
        .iter()
        .filter_map(|(key, value)| Some((key, value.as_str()?.to_owned())))
        .collect::<Vec<(&Cow<'input, str>, String)>>();
    tag_collection.sort_by_key(|v| v.0);

    for (key, value) in tag_collection {
        output.write_all(&[b',']).ok()?;
        write_escaped_key(&mut output, key.as_bytes())?;
        output.write_all(&[b'=']).ok()?;
        // For the fields we escape differently then for values ...
        write_escaped_key(&mut output, value.as_bytes())?;
    }

    output.write_all(&[b' ']).ok()?;

    let fields = v.get("fields")?.as_object()?;
    let mut field_collection: Vec<(&Cow<'input, str>, &Value)> = fields.iter().collect();
    field_collection.sort_by_key(|v| v.0);
    let mut first = true;
    for (key, value) in field_collection {
        if first {
            first = false;
        } else {
            output.write_all(&[b',']).ok()?;
        }
        write_escaped_key(&mut output, key.as_bytes())?;
        output.write_all(&[b'=']).ok()?;

        if let Some(s) = value.as_str() {
            write_escaped_value(&mut output, s.as_bytes())?
        } else {
            match value.value_type() {
                ValueType::F64 | ValueType::Bool => value.write(&mut output).ok()?,
                ValueType::U64 | ValueType::I64 => {
                    value.write(&mut output).ok()?;
                    output.write_all(&[b'i']).ok()?
                }
                _ => return None,
            }
        }
    }

    output.write_all(&[b' ']).ok()?;
    let t = v.get("timestamp")?;
    if t.is_u64() {
        t.write(&mut output).ok()?;
        Some(output)
    } else {
        None
    }
}

pub fn parse<'input>(data: &'input str, ingest_ns: u64) -> Result<Option<Value<'input>>> {
    let data = data.trim();

    if data.is_empty() || data.starts_with('#') {
        return Ok(None);
    };

    let mut chars = data.chars();
    let (measurement, c) = parse_to_char2(&mut chars, ',', ' ')?;
    let tags = if c == ',' {
        parse_tags(&mut chars)?
    } else {
        Object::new()
    };

    let fields = parse_fields(&mut chars)?;
    let timestamp_str = chars.as_str();
    let timestamp = if timestamp_str.is_empty() {
        ingest_ns
    } else {
        chars.as_str().parse()?
    };

    let mut m = Object::with_capacity(4);
    m.insert_nocheck("measurement".into(), Value::String(measurement));
    m.insert_nocheck("tags".into(), Value::from(tags));
    m.insert_nocheck("fields".into(), Value::from(fields));
    m.insert_nocheck("timestamp".into(), timestamp.into());
    Ok(Some(Value::from(m)))
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
        _ => Ok(Value::from(
            s.parse::<f64>()
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
                    return Ok((Value::from(res.parse::<i64>()?), c))
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

fn parse_fields<'input>(chars: &mut Chars) -> Result<Object<'input>> {
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
            _ => return Err(ErrorKind::InvalidInfluxData("Failed to parse fields.".into()).into()),
        };
    }
}

fn parse_tags<'input>(chars: &mut Chars) -> Result<Object<'input>> {
    let mut res = HashMap::new();
    loop {
        let (key, c_key) = parse_to_char3(chars, '=', Some(' '), Some(','))?;
        if c_key != '=' {
            return Err(ErrorKind::InvalidInfluxData("Tag without value".into()).into());
        };
        let (val, c_val) = parse_to_char3(chars, '=', Some(' '), Some(','))?;
        if c_val == '=' {
            return Err(ErrorKind::InvalidInfluxData("= found in tag value".into()).into());
        }
        res.insert(key, Value::String(val));
        if c_val == ' ' {
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

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
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
            "timestamp": 1_465_839_830_100_400_200i64,
        })
        .into();
        assert_eq!(Ok(Some(r)), parse(s, 0))
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
            "timestamp": 1_465_839_830_100_400_200i64,
        })
        .into();
        assert_eq!(Ok(Some(r)), parse(s, 0))
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
            "timestamp": 1_465_839_830_100_400_200i64,
        })
        .into();
        assert_eq!(Ok(Some(r)), parse(s, 0))
    }

    #[test]
    fn parse_no_timestamp() {
        let s = "weather temperature=82i";
        let parsed = parse(s, 1_465_839_830_100_400_200u64).expect("failed to parse");
        let r: Value = json!({
            "measurement": "weather",
            "tags": {},
            "fields": {
                "temperature": 82
            },
            "timestamp": 1_465_839_830_100_400_200i64,
        })
        .into();
        assert_eq!(Some(r), parsed);
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
            "timestamp": 1_465_839_830_100_400_200i64,
        })
        .into();
        assert_eq!(Ok(Some(r)), parse(s, 0))
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
            "timestamp": 1_465_839_830_100_400_200i64,
        })
        .into();
        assert_eq!(Ok(Some(r)), parse(s, 0))
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
            "timestamp": 1_465_839_830_100_400_200i64,
        })
        .into();
        assert_eq!(Ok(Some(r)), parse(s, 0))
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
            "timestamp": 1_465_839_830_100_400_200i64,
        })
        .into();
        for s in sarr {
            assert_eq!(Ok(Some(r.clone())), parse(s, 0));
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
            "timestamp": 1_465_839_830_100_400_200i64,
        })
        .into();
        for s in sarr {
            assert_eq!(Ok(Some(r.clone())), parse(s, 0));
        }
    }
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
            "timestamp": 1_465_839_830_100_400_200i64,
        })
        .into();
        assert_eq!(Ok(Some(r)), parse(s, 0))
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
            "timestamp": 1_465_839_830_100_400_200i64,
        })
        .into();
        assert_eq!(Ok(Some(r)), parse(s, 0))
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
            "timestamp": 1_465_839_830_100_400_200i64,
        })
        .into();
        assert_eq!(Ok(Some(r)), parse(s, 0))
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
            "timestamp": 1_465_839_830_100_400_200i64,
        })
        .into();
        assert_eq!(Ok(Some(r)), parse(s, 0))
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
            "timestamp": 1_465_839_830_100_400_200i64,
        })
        .into();
        assert_eq!(Ok(Some(r)), parse(s, 0))
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
            "timestamp": 1_465_839_830_100_400_200i64,
        })
        .into();
        assert_eq!(Ok(Some(r)), parse(s, 0))
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
            "timestamp": 1_465_839_830_100_400_201i64,
        })
        .into();
        assert_eq!(Ok(Some(r)), parse(s, 0));
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
            "timestamp": 1_465_839_830_100_400_202i64,
        })
        .into();
        assert_eq!(Ok(Some(r)), parse(s, 0))
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
            "timestamp": 1_465_839_830_100_400_203i64,
        })
        .into();
        assert_eq!(Ok(Some(r)), parse(s, 0))
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
            "timestamp": 1_465_839_830_100_400_204i64,
        })
        .into();
        assert_eq!(Ok(Some(r)), parse(s, 0))
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
        assert_eq!(Ok(Some(r)), parse(s, 0))
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
            "timestamp": 1_465_839_830_100_400_206i64,
        })
        .into();
        assert_eq!(Ok(Some(r)), parse(s, 0))
    }
}
