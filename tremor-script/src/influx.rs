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
use halfbrown::{hashmap, HashMap};
use simd_json::{BorrowedValue, OwnedValue};
use std::borrow::Cow;
use std::str::Chars;

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct InfluxDatapoint {
    measurement: String,
    tags: HashMap<String, String>,
    fields: HashMap<String, OwnedValue>,
    timestamp: u64,
}

impl InfluxDatapoint {
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
                OwnedValue::String(s) => Ok(process_string(s)),

                OwnedValue::F64(num) => Ok(num.to_string()),
                OwnedValue::I64(num) => Ok(num.to_string()),

                OwnedValue::Bool(b) => Ok(b.to_string()),
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

    pub fn from_parts(
        measurement: &str,
        tags: HashMap<String, String>,
        fields: HashMap<String, OwnedValue>,
        timestamp: u64,
    ) -> InfluxDatapoint {
        InfluxDatapoint {
            measurement: measurement.to_owned(),
            tags,
            fields,
            timestamp,
        }
    }
}

#[allow(clippy::implicit_hasher)]
impl<'influx> From<InfluxDatapoint> for HashMap<Cow<'influx, str>, BorrowedValue<'influx>> {
    fn from(x: InfluxDatapoint) -> HashMap<Cow<'influx, str>, BorrowedValue<'influx>> {
        let tags = x
            .tags
            .into_iter()
            .map(|(x, y)| (x.into(), BorrowedValue::from(y)))
            .collect();
        let fields = x
            .fields
            .into_iter()
            .map(|(x, y)| (x.into(), BorrowedValue::from(y)))
            .collect();
        hashmap!(
            "measurement".into() => x.measurement.into(),
            "tags".into() => BorrowedValue::Object(tags),
            "fields".into() => BorrowedValue::Object(fields),
            "timestamp".into() => x.timestamp.into()
        )
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

pub fn parse(data: &str) -> Result<InfluxDatapoint> {
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

fn parse_string(chars: &mut Chars) -> Result<(OwnedValue, char)> {
    let val = parse_to_char(chars, '"')?;
    match chars.next() {
        Some(',') => Ok((OwnedValue::from(val), ',')),
        Some(' ') => Ok((OwnedValue::from(val), ' ')),
        _ => Err(ErrorKind::InvalidInfluxData("Unexpected character after string".into()).into()),
    }
}

fn float_or_bool(s: &str) -> Result<OwnedValue> {
    match s {
        "t" | "T" | "true" | "True" | "TRUE" => Ok(OwnedValue::from(true)),
        "f" | "F" | "false" | "False" | "FALSE" => Ok(OwnedValue::from(false)),
        _ => Ok(OwnedValue::F64(
            s.parse()
                .map_err(|_| ErrorKind::InvalidInfluxData(s.to_owned()))?,
        )),
    }
}
fn parse_value(chars: &mut Chars) -> Result<(OwnedValue, char)> {
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
                Some(' ') => return Ok((OwnedValue::I64(res.parse()?), ' ')),
                Some(',') => return Ok((OwnedValue::I64(res.parse()?), ',')),
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

fn parse_fields(chars: &mut Chars) -> Result<HashMap<String, OwnedValue>> {
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

trait Escaper {
    type Escaped;
    fn escape(&self) -> Self::Escaped;
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
    use halfbrown::hashmap;
    use simd_json::OwnedValue;

    #[test]
    fn parse_simple() {
        let s = "weather,location=us-midwest temperature=82 1465839830100400200";
        let r = InfluxDatapoint {
            measurement: "weather".to_string(),
            tags: hashmap! {
                "location".to_string() => "us-midwest".to_string()
            },
            fields: hashmap! {
                "temperature".to_string() => OwnedValue::from(82.0)
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
                "temperature".to_string() => OwnedValue::from(82.0)
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
                "temperature".to_string() => OwnedValue::from(82.0),
                "bug_concentration".to_string() => OwnedValue::from(98.0),

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
                "temperature".to_string() => OwnedValue::F64(82.0)
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
                "temperature".to_string() => OwnedValue::I64(82)
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
                "temperature".to_string() => OwnedValue::from("too warm")
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
                "too_hot".to_string() => OwnedValue::Bool(true)
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
                "too_hot".to_string() => OwnedValue::Bool(false)
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
                "temperature".to_string() => OwnedValue::F64(82.0)
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
                "temp=rature".to_string() => OwnedValue::F64(82.0)
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
                "temperature".to_string() => OwnedValue::F64(82.0)
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
                "temperature".to_string() => OwnedValue::F64(82.0)
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
                "temperature".to_string() => OwnedValue::F64(82.0)
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
                "temperature".to_string() => OwnedValue::from("too\"hot\"")
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
                "temperature_str".to_string() => OwnedValue::from("too hot/cold")
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
                "temperature_str".to_string() => OwnedValue::from("too hot\\cold")
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
                "temperature_str".to_string() => OwnedValue::from("too hot\\cold")
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
                "temperature_str".to_string() => OwnedValue::from("too hot\\\\cold")
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
                "temperature_str".to_string() => OwnedValue::from("too hot\\\\cold")
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
                "temperature_str".to_string() => OwnedValue::from("too hot\\\\\\cold")
            },
            timestamp: 1465839830100400206,
        };
        assert_eq!(Ok(r), parse(s))
    }

}
