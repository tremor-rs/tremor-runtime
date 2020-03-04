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

use crate::enumerate::Enumerate;
use crate::{DecoderError as Error, DecoderResult as Result};
use simd_json::prelude::*;
use std::borrow::Cow;
use std::str::Chars;

macro_rules! cant_error {
    ($e:expr) => {
        if $e.is_err() {
            unreachable!()
        }
    };
}

/// Tries to parse a striung as an influx line protocl message
pub fn decode<'input, V>(data: &'input str, ingest_ns: u64) -> Result<Option<V>>
where
    V: ValueTrait + Mutable + Builder<'input> + 'input,
    <V as ValueTrait>::Key: From<Cow<'input, str>> + From<&'input str>,
{
    let data = data.trim();

    if data.is_empty() || data.starts_with('#') {
        return Ok(None);
    };

    let mut chars = Enumerate::new(data.chars());
    let (measurement, c, _) = parse_to_char2(&mut chars, ',', ' ')?;
    let tags = if c == ',' {
        parse_tags(&mut chars)?
    } else {
        V::object()
    };

    let fields: V = parse_fields(&mut chars)?;
    let (idx, timestamp_str) = chars.parts();
    let timestamp_str = timestamp_str.as_str();
    let timestamp = if timestamp_str.is_empty() {
        ingest_ns
    } else {
        timestamp_str
            .parse()
            .map_err(|e| Error::ParseIntError(idx, e))?
    };

    let mut m = V::object_with_capacity(4);
    cant_error!(m.insert("measurement", V::from(measurement)));
    cant_error!(m.insert("tags", tags));
    cant_error!(m.insert("fields", fields));
    cant_error!(m.insert("timestamp", V::from(timestamp)));
    Ok(Some(m))
}

fn parse_string<'input, V>(chars: &mut Enumerate<Chars>) -> Result<(V, Option<char>, usize)>
where
    V: ValueTrait + Mutable + Builder<'input> + 'input + From<Cow<'input, str>>,
{
    let val = parse_to_char(chars, '"')?;
    let idx = chars.current_count();
    match chars.next() {
        Some((i, c @ ',')) | Some((i, c @ ' ')) => Ok((V::from(val), Some(c), i)),
        None => Ok((V::from(val), None, idx)),
        Some((i, _)) => Err(Error::TrailingCharacter(i)),
    }
}

fn float_or_bool<'input, V>(idx: usize, s: &str) -> Result<V>
where
    V: ValueTrait + Mutable + Builder<'input> + 'input + From<Cow<'input, str>>,
{
    match s {
        "t" | "T" | "true" | "True" | "TRUE" => Ok(V::from(true)),
        "f" | "F" | "false" | "False" | "FALSE" => Ok(V::from(false)),
        _ => Ok(V::from(
            s.parse::<f64>()
                .map_err(|e| Error::ParseFloatError(idx, e))?,
        )),
    }
}
fn parse_value<'input, V>(chars: &mut Enumerate<Chars>) -> Result<(V, Option<char>, usize)>
where
    V: ValueTrait + Mutable + Builder<'input> + 'input,
{
    let mut res = String::new();
    let idx = chars.current_count();
    match chars.next() {
        Some((_, '"')) => return parse_string(chars),
        Some((i, ' ')) | Some((i, ',')) => return Err(Error::UnexpectedEnd(i)),
        None => return Err(Error::UnexpectedEnd(idx)),
        Some((_, c)) => res.push(c),
    }
    loop {
        match chars.next() {
            Some((_, c @ ',')) | Some((_, c @ ' ')) => {
                return Ok((float_or_bool(idx, &res)?, Some(c), idx))
            }
            None => return Ok((float_or_bool(idx, &res)?, None, idx)),
            Some((i, 'i')) => match chars.next() {
                Some((_, c @ ' ')) | Some((_, c @ ',')) => {
                    return Ok((
                        V::from(res.parse::<i64>().map_err(|e| Error::ParseIntError(i, e))?),
                        Some(c),
                        idx,
                    ))
                }
                None => {
                    return Ok((
                        V::from(res.parse::<i64>().map_err(|e| Error::ParseIntError(i, e))?),
                        None,
                        idx,
                    ))
                }
                Some((i, _)) => {
                    return Err(Error::Expected(i, ' ', Some(','), None));
                }
            },
            Some((i, '\\')) => {
                if let Some((_, c)) = chars.next() {
                    res.push(c);
                } else {
                    return Err(Error::InvalidEscape(i));
                }
            }
            Some((_, c)) => res.push(c),
        }
    }
}

fn parse_fields<'input, V>(chars: &mut Enumerate<Chars>) -> Result<V>
where
    V: ValueTrait + Mutable + Builder<'input> + 'input,
    <V as ValueTrait>::Key: From<Cow<'input, str>>,
{
    let mut res = V::object();
    loop {
        let key = parse_to_char(chars, '=')?;

        let (val, c, idx): (V, _, _) = parse_value(chars)?;
        match c {
            Some(',') => {
                cant_error!(res.insert(key, val));
            }
            Some(' ') | None => {
                cant_error!(res.insert(key, val));
                return Ok(res);
            }
            _ => return Err(Error::InvalidFields(idx)),
        };
    }
}

fn parse_tags<'input, V>(chars: &mut Enumerate<Chars>) -> Result<V>
where
    V: ValueTrait + Mutable + Builder<'input> + 'input + From<Cow<'input, str>>,
    <V as ValueTrait>::Key: From<Cow<'input, str>>,
{
    let mut res = V::object();
    loop {
        let (key, c_key, idx) = parse_to_char3(chars, '=', Some(' '), Some(','))?;
        if c_key != '=' {
            return Err(Error::MissingTagValue(idx));
        };
        let (val, c_val, idx) = parse_to_char3(chars, '=', Some(' '), Some(','))?;
        if c_val == '=' {
            return Err(Error::EqInTagValue(idx));
        }
        cant_error!(res.insert(key, V::from(val)));
        if c_val == ' ' {
            return Ok(res);
        }
    }
}

fn parse_to_char3<'input>(
    chars: &mut Enumerate<Chars>,
    end1: char,
    end2: Option<char>,
    end3: Option<char>,
) -> Result<(Cow<'input, str>, char, usize)> {
    let mut res = String::new();
    let mut idx = chars.current_count();
    while let Some((i, c)) = chars.next() {
        idx = i;
        match c {
            c if c == end1 => return Ok((res.into(), end1, i)),
            c if Some(c) == end2 => return Ok((res.into(), c, i)),
            c if Some(c) == end3 => return Ok((res.into(), c, i)),
            '\\' => match chars.next() {
                Some((_, c)) if c == '\\' || c == end1 || Some(c) == end2 || Some(c) == end3 => {
                    res.push(c)
                }
                Some((_, c)) => {
                    res.push('\\');
                    res.push(c)
                }
                None => {
                    return Err(Error::InvalidEscape(i));
                }
            },
            _ => res.push(c),
        }
    }
    Err(Error::Expected(idx, end1, end2, end3))
}

fn parse_to_char2<'input>(
    chars: &mut Enumerate<Chars>,
    end1: char,
    end2: char,
) -> Result<(Cow<'input, str>, char, usize)> {
    parse_to_char3(chars, end1, Some(end2), None)
}

fn parse_to_char<'input>(chars: &mut Enumerate<Chars>, end: char) -> Result<Cow<'input, str>> {
    let (res, _, _) = parse_to_char3(chars, end, None, None)?;
    Ok(res)
}
