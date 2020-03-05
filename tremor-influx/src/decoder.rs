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

use crate::{DecoderError as Error, DecoderResult as Result};
use simd_json::prelude::*;
use std::borrow::Cow;

macro_rules! cant_error {
    ($e:expr) => {
        if $e.is_err() {
            // ALLOW: this errors can never happen
            unreachable!()
        }
    };
}

/// Tries to parse a striung as an influx line protocl message
pub fn decode<'input, V>(data: &'input str, ingest_ns: u64) -> Result<Option<V>>
where
    V: ValueTrait + Mutable + Builder<'input> + 'input + std::fmt::Debug,
    <V as ValueTrait>::Key: From<Cow<'input, str>> + From<&'input str>,
{
    let mut data = data.trim();

    if data.is_empty() || data.starts_with('#') {
        return Ok(None);
    };

    let (measurement, idx) = parse_to(&data, |c| c == ',' || c == ' ')?;
    data = &data[idx..];
    let (tags, idx) = if data.starts_with(',') {
        data = &data[1..];
        parse_tags(data)?
    } else {
        (V::object(), 0)
    };
    data = &data[idx..];
    if !data.is_empty() {
        data = &data[1..];
    };
    let (fields, idx): (V, usize) = parse_fields(data)?;
    data = &data[idx..];
    let timestamp = if data.is_empty() {
        ingest_ns
    } else {
        lexical::parse::<u64, _>(data).map_err(|e| Error::ParseIntError(idx, e))?
    };

    let mut m = V::object_with_capacity(4);
    cant_error!(m.insert("measurement", V::from(measurement)));
    cant_error!(m.insert("tags", tags));
    cant_error!(m.insert("fields", fields));
    cant_error!(m.insert("timestamp", V::from(timestamp)));
    Ok(Some(m))
}

fn parse_string<'input, V>(mut input: &'input str) -> Result<(V, Option<char>, usize)>
where
    V: ValueTrait + Mutable + Builder<'input> + 'input + From<Cow<'input, str>> + std::fmt::Debug,
{
    let (val, idx) = parse_to(input, |c| c == '"')?;
    input = &input[idx + 1..];
    if input.starts_with(' ') {
        Ok((V::from(val), Some(' '), idx + 2))
    } else if input.starts_with(',') {
        Ok((V::from(val), Some(','), idx + 2))
    } else if input.is_empty() {
        Ok((V::from(val), None, idx + 1))
    } else {
        Err(Error::TrailingCharacter(idx))
    }
}

fn to_value<'input, V>(idx: usize, s: &str) -> Result<V>
where
    V: ValueTrait + Mutable + Builder<'input> + 'input + From<Cow<'input, str>> + std::fmt::Debug,
{
    match s {
        "t" | "T" | "true" | "True" | "TRUE" => Ok(V::from(true)),
        "f" | "F" | "false" | "False" | "FALSE" => Ok(V::from(false)),
        _ => {
            if s.ends_with('i') && s.starts_with('-') {
                Ok(V::from(
                    lexical::parse::<i64, _>(&s[..s.len() - 1])
                        .map_err(|e| Error::ParseIntError(idx, e))?,
                ))
            } else if s.ends_with('i') {
                Ok(V::from(
                    lexical::parse::<u64, _>(&s[..s.len() - 1])
                        .map_err(|e| Error::ParseIntError(idx, e))?,
                ))
            } else {
                Ok(V::from(
                    lexical::parse::<f64, _>(s).map_err(|e| Error::ParseFloatError(idx, e))?,
                ))
            }
        }
    }
}

fn parse_value<'input, V>(mut input: &'input str) -> Result<(V, Option<char>, usize)>
where
    V: ValueTrait + Mutable + Builder<'input> + 'input + std::fmt::Debug,
{
    let mut offset = 0;
    if input.starts_with('"') {
        parse_string(&input[1..])
    } else if input.starts_with(' ') {
        Err(Error::UnexpectedEnd(0))
    } else if input.is_empty() {
        Err(Error::UnexpectedEnd(0))
    } else if let Some(idx) = input.find(|c| c == ',' || c == ' ' || c == '\\') {
        offset += idx;
        let data = &input[..idx];
        input = &input[idx..];
        if input.starts_with('\\') {
            let mut res = String::with_capacity(256);
            res.push_str(data);
            parse_value_complex(res, &input[1..])
        } else if input.starts_with(',') {
            Ok((to_value(idx, data)?, Some(','), offset))
        } else if input.starts_with(' ') {
            Ok((to_value(idx, data)?, Some(' '), offset))
        } else if input.is_empty() {
            Ok((to_value(idx, data)?, None, offset))
        } else {
            Err(Error::UnexpectedEnd(0))
        }
    } else {
        Ok((to_value(0, input)?, None, input.len() - 1))
    }
}

fn parse_value_complex<'input, V>(
    mut res: String,
    mut input: &'input str,
) -> Result<(V, Option<char>, usize)>
where
    V: ValueTrait + Mutable + Builder<'input> + 'input + std::fmt::Debug,
{
    let mut offset = 0;
    loop {
        if let Some(idx) = input.find(|c| c == ',' || c == ' ' || c == '\\') {
            offset += idx;
            res.push_str(&input[..idx]);
            input = &input[idx..];
            if input.starts_with('\\') {
                input = &input[1..];
                let d = &input[..1];
                res.push_str(&d);
                offset += 1;
            } else if input.starts_with(',') {
                return Ok((to_value(idx, &res)?, Some(','), offset));
            } else if input.starts_with(' ') {
                return Ok((to_value(idx, &res)?, Some(' '), offset));
            } else if input.is_empty() {
                return Ok((to_value(idx, &res)?, None, offset));
            } else {
                return Err(Error::UnexpectedEnd(0));
            }
        } else {
            res.push_str(input);
            return Ok((to_value(0, &res)?, None, input.len() - 1));
        }
    }
}

fn parse_fields<'input, V>(mut input: &'input str) -> Result<(V, usize)>
where
    V: ValueTrait + Mutable + Builder<'input> + 'input + std::fmt::Debug,
    <V as ValueTrait>::Key: From<Cow<'input, str>>,
{
    let mut offset = 0;
    let mut res = V::object_with_capacity(16);
    loop {
        let (key, idx) = parse_to(input, |c| c == '=')?;
        input = &input[idx + 1..];
        offset += idx + 1;
        let (val, c, idx): (V, _, _) = parse_value(input)?;
        input = &input[idx + 1..];
        offset += idx + 1;

        match c {
            Some(',') => {
                cant_error!(res.insert(key, val));
            }
            Some(' ') | None => {
                cant_error!(res.insert(key, val));
                return Ok((res, offset));
            }
            _ => return Err(Error::InvalidFields(idx)),
        };
    }
}

fn parse_tags<'input, V>(mut input: &'input str) -> Result<(V, usize)>
where
    V: ValueTrait + Mutable + Builder<'input> + 'input + From<Cow<'input, str>> + std::fmt::Debug,
    <V as ValueTrait>::Key: From<Cow<'input, str>>,
{
    let mut res = V::object_with_capacity(16);
    let mut offset = 0;
    loop {
        let (key, idx) = parse_to(input, |c| c == '=' || c == ' ' || c == ',')?;
        offset += idx + 1;
        input = &input[idx..];
        if !input.starts_with('=') {
            return Err(Error::MissingTagValue(idx));
        }
        input = &input[1..];
        let (val, idx2) = parse_to(input, |c| c == '=' || c == ' ' || c == ',')?;
        offset += idx2;
        input = &input[idx2..];
        cant_error!(res.insert(key, V::from(val)));
        if input.starts_with(' ') {
            return Ok((res, offset));
        } else if input.starts_with(',') {
            input = &input[1..];
            offset += 1;
        } else if input.starts_with('=') {
            return Err(Error::MissingTagValue(idx2));
        }
    }
}

fn parse_to<'input, F>(mut input: &'input str, p: F) -> Result<(Cow<'input, str>, usize)>
where
    F: Fn(char) -> bool,
{
    let search = |c| p(c) || c == '\\';
    if let Some(idx) = input.find(search) {
        let data = &input[..idx];
        input = &input[idx..];
        if input.starts_with('\\') {
            let mut res = String::with_capacity(256);
            res.push_str(data);
            input = &input[1..];
            // https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_reference/#special-characters
            if !input.starts_with(search) {
                res.push('\\');
            }
            res.push_str(&input[..1]);

            input = &input[1..];
            parse_to_complex(res, input, idx + 2, p)
        } else {
            Ok((data.into(), idx))
        }
    } else {
        Err(Error::Unexpected(0))
    }
}

fn parse_to_complex<'input, F>(
    mut res: String,
    mut input: &'input str,
    mut offset: usize,
    p: F,
) -> Result<(Cow<'input, str>, usize)>
where
    F: Fn(char) -> bool,
{
    let search = |c| p(c) || c == '\\';
    loop {
        if let Some(idx) = input.find(&search) {
            let data = &input[..idx];
            input = &input[idx..];
            offset += idx;
            if input.starts_with('\\') {
                res.push_str(data);
                input = &input[1..];
                // https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_reference/#special-characters
                if !input.starts_with(search) {
                    res.push('\\');
                }
                res.push_str(&input[..1]);
                input = &input[1..];
                offset += 2;
            } else {
                res.push_str(&data);
                return Ok((res.into(), offset));
            }
        } else {
            return Err(Error::Unexpected(0));
        }
    }
}
