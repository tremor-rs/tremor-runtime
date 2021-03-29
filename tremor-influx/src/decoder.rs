// Copyright 2020-2021, The Tremor Team
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
use std::borrow::Cow;
use value_trait::prelude::*;

macro_rules! cant_error {
    ($e:expr) => {
        if $e.is_err() {
            // ALLOW: this errors can never happen
            // unreachable!()
            ()
        }
    };
}

/// Tries to parse a string as an influx line protocol message
/// See: [Influx docs](https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/)
///
/// # Errors
///    * if the input isn't valid influx line protocol
pub fn decode<'input, V>(data: &'input str, ingest_ns: u64) -> Result<Option<V>>
where
    V: Value + Mutable + ValueAccess<Target = V> + Builder<'input> + 'input + std::fmt::Debug,
    <V as ValueAccess>::Key: From<Cow<'input, str>> + From<&'input str>,
{
    let mut data = data.trim();

    if data.is_empty() || data.starts_with('#') {
        return Ok(None);
    };
    let mut total_idx = 0;

    let (measurement, idx1) = parse_to(total_idx, &data, |c| c == ',' || c == ' ')?;
    total_idx += idx1;
    data = get_rest(data, idx1)?;
    let (tags, idx2) = if let Some(rest) = data.strip_prefix(',') {
        total_idx += 1;
        data = rest;
        parse_tags(total_idx, data)?
    } else {
        (V::object(), 0)
    };
    data = get_rest(data, idx2)?;
    if !data.is_empty() {
        total_idx += 1;
        data = get_rest(data, 1)?;
    };
    let (fields, idx): (V, usize) = parse_fields(total_idx, data)?;
    total_idx += idx;
    data = get_rest(data, idx)?;
    let timestamp = if data.is_empty() {
        ingest_ns
    } else {
        lexical::parse::<u64, _>(data).map_err(|e| Error::ParseIntError(total_idx, e))?
    };

    let mut m = V::object_with_capacity(4);
    cant_error!(m.insert("measurement", V::from(measurement)));
    cant_error!(m.insert("tags", tags));
    cant_error!(m.insert("fields", fields));
    cant_error!(m.insert("timestamp", V::from(timestamp)));
    Ok(Some(m))
}

fn parse_string<'input, V>(
    total_index: usize,
    mut input: &'input str,
) -> Result<(V, Option<char>, usize)>
where
    V: Value + Mutable + Builder<'input> + 'input + From<Cow<'input, str>> + std::fmt::Debug,
{
    let (val, idx) = parse_to(total_index, input, |c| c == '"')?;
    input = get_rest(input, idx + 1)?;
    if input.starts_with(' ') {
        Ok((V::from(val), Some(' '), idx + 2))
    } else if input.starts_with(',') {
        Ok((V::from(val), Some(','), idx + 2))
    } else if input.is_empty() {
        Ok((V::from(val), None, idx + 1))
    } else {
        Err(Error::TrailingCharacter(total_index + idx))
    }
}

fn to_value<'input, V>(total_idx: usize, s: &str) -> Result<V>
where
    V: Value + Mutable + Builder<'input> + 'input + From<Cow<'input, str>> + std::fmt::Debug,
{
    match s {
        "t" | "T" | "true" | "True" | "TRUE" => Ok(V::from(true)),
        "f" | "F" | "false" | "False" | "FALSE" => Ok(V::from(false)),
        _ => {
            if let Some(s) = s.strip_suffix('i') {
                if s.starts_with('-') {
                    Ok(V::from(
                        lexical::parse::<i64, _>(s)
                            .map_err(|e| Error::ParseIntError(total_idx, e))?,
                    ))
                } else {
                    Ok(V::from(
                        lexical::parse::<u64, _>(s)
                            .map_err(|e| Error::ParseIntError(total_idx, e))?,
                    ))
                }
            } else {
                Ok(V::from(
                    lexical::parse::<f64, _>(s)
                        .map_err(|e| Error::ParseFloatError(total_idx, e))?,
                ))
            }
        }
    }
}

fn parse_value<'input, V>(
    total_index: usize,
    mut input: &'input str,
) -> Result<(V, Option<char>, usize)>
where
    V: Value + Mutable + Builder<'input> + 'input + std::fmt::Debug,
{
    let mut offset = 0;
    if let Some(rest) = input.strip_prefix('"') {
        parse_string(total_index, rest)
    } else if input.starts_with(' ') || input.is_empty() {
        Err(Error::UnexpectedEnd(total_index))
    } else if let Some((idx, data, rest)) = split_once(input, |c| c == ',' || c == ' ' || c == '\\')
    {
        offset += idx;
        input = rest;
        if let Some(rest) = input.strip_prefix('\\') {
            let mut res = String::with_capacity(256);
            res.push_str(data);
            parse_value_complex(total_index + offset, res, rest)
        } else if input.starts_with(',') {
            Ok((to_value(total_index + offset, data)?, Some(','), offset))
        } else if input.starts_with(' ') {
            Ok((to_value(total_index + offset, data)?, Some(' '), offset))
        } else if input.is_empty() {
            Ok((to_value(total_index + offset, data)?, None, offset))
        } else {
            Err(Error::UnexpectedEnd(total_index + offset))
        }
    } else {
        Ok((
            to_value(total_index + offset, input)?,
            None,
            input.len() - 1,
        ))
    }
}

fn parse_value_complex<'input, V>(
    total_index: usize,
    mut res: String,
    mut input: &'input str,
) -> Result<(V, Option<char>, usize)>
where
    V: Value + Mutable + Builder<'input> + 'input + std::fmt::Debug,
{
    let mut offset = 0;
    loop {
        if let Some((idx, start, rest)) = split_once(input, |c| c == ',' || c == ' ' || c == '\\') {
            offset += idx;
            res.push_str(start);
            input = rest;
            if let Some(rest) = input.strip_prefix('\\') {
                input = rest;
                let (escaped, rest) = split_at(input, 1).ok_or(Error::UnexpectedEnd(offset))?;
                input = rest;
                res.push_str(escaped);
                offset += 1;
            } else if input.starts_with(',') {
                return Ok((to_value(total_index + offset, &res)?, Some(','), offset));
            } else if input.starts_with(' ') {
                return Ok((to_value(total_index + offset, &res)?, Some(' '), offset));
            } else if input.is_empty() {
                return Ok((to_value(total_index + offset, &res)?, None, offset));
            } else {
                return Err(Error::UnexpectedEnd(total_index + offset));
            }
        } else {
            res.push_str(input);
            return Ok((to_value(total_index, &res)?, None, input.len() - 1));
        }
    }
}

fn parse_fields<'input, V>(total_idx: usize, mut input: &'input str) -> Result<(V, usize)>
where
    V: Value + Mutable + ValueAccess + Builder<'input> + 'input + std::fmt::Debug,
    <V as ValueAccess>::Key: From<Cow<'input, str>>,
    <V as ValueAccess>::Target: From<V>,
{
    let mut offset = 0;
    let mut res = V::object_with_capacity(16);
    loop {
        let (key, idx1) = parse_to(total_idx + offset, input, |c| c == '=')?;
        input = get_rest(input, idx1 + 1)?;
        offset += idx1 + 1;
        let (val, c, idx2): (V, _, _) = parse_value(total_idx + offset, input)?;
        input = get_rest(input, idx2 + 1)?;
        offset += idx2 + 1;

        match c {
            Some(',') => {
                cant_error!(res.insert(key, val));
            }
            Some(' ') | None => {
                cant_error!(res.insert(key, val));
                return Ok((res, offset));
            }
            _ => return Err(Error::InvalidFields(total_idx + offset)),
        };
    }
}

fn parse_tags<'input, V>(total_idx: usize, mut input: &'input str) -> Result<(V, usize)>
where
    V: Value
        + Mutable
        + ValueAccess
        + Builder<'input>
        + 'input
        + From<Cow<'input, str>>
        + std::fmt::Debug,
    <V as ValueAccess>::Key: From<Cow<'input, str>>,
    <V as ValueAccess>::Target: From<V>,
{
    let mut res = V::object_with_capacity(16);
    let mut offset = 0;
    loop {
        let (key, idx) = parse_to(total_idx + offset, input, |c| {
            c == '=' || c == ' ' || c == ','
        })?;
        offset += idx + 1;
        input = get_rest(input, idx)?;
        if let Some(rest) = input.strip_prefix('=') {
            input = rest;
        } else {
            return Err(Error::MissingTagValue(total_idx + offset));
        }
        let (val, idx2) = parse_to(total_idx + offset, input, |c| c == ' ' || c == ',')?;
        offset += idx2;
        input = get_rest(input, idx2)?;
        cant_error!(res.insert(key, V::from(val)));
        if input.starts_with(' ') {
            return Ok((res, offset));
        } else if let Some(rest) = input.strip_prefix(',') {
            input = rest;
            offset += 1;
        } else if input.starts_with('=') {
            return Err(Error::MissingTagValue(total_idx + offset));
        }
    }
}

fn parse_to<F>(total_idx: usize, mut input: &str, p: F) -> Result<(Cow<str>, usize)>
where
    F: Fn(char) -> bool,
{
    let search = |c| p(c) || c == '\\';

    #[allow(clippy::option_if_let_else)] // cannot use map_or_else because borrow-checker
    if let Some((idx, data, rest)) = split_once(input, &search) {
        input = rest;
        if let Some(rest) = input.strip_prefix('\\') {
            input = rest;
            let mut res = String::with_capacity(256);
            res.push_str(data);
            // https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_reference/#special-characters
            if !input.starts_with(search) {
                res.push('\\');
            }

            if let Some((s, e)) = split_at(input, 1) {
                res.push_str(s);
                input = e;
            }

            parse_to_complex(total_idx, res, idx + 2, input, p)
        } else {
            Ok((data.into(), idx))
        }
    } else {
        Err(Error::Unexpected(total_idx))
    }
}

fn parse_to_complex<F>(
    total_idx: usize,
    mut res: String,
    mut offset: usize,
    mut input: &str,
    p: F,
) -> Result<(Cow<str>, usize)>
where
    F: Fn(char) -> bool,
{
    let search = |c| p(c) || c == '\\';
    loop {
        if let Some((idx, data, rest)) = split_once(input, &search) {
            input = rest;
            offset += idx;
            if let Some(rest) = input.strip_prefix('\\') {
                input = rest;
                res.push_str(data);
                // https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_reference/#special-characters
                if !input.starts_with(search) {
                    res.push('\\');
                }
                if let Some((s, e)) = split_at(input, 1) {
                    res.push_str(s);
                    input = e;
                }
                offset += 2;
            } else {
                res.push_str(&data);
                return Ok((res.into(), offset));
            }
        } else {
            return Err(Error::Unexpected(total_idx + offset));
        }
    }
}

/// Splits off a string at the first occupance of `p`. The rest string will include the pattern
/// that matched `p`
fn split_once<F>(input: &str, p: F) -> Option<(usize, &str, &str)>
where
    F: Fn(char) -> bool,
{
    let idx = input.find(p)?;
    split_at(input, idx).map(|(s, e)| (idx, s, e))
}

/// Splits a string at a given index
fn split_at(input: &str, idx: usize) -> Option<(&str, &str)> {
    Some((input.get(..idx)?, input.get(idx..)?))
}

fn get_rest(data: &str, start: usize) -> Result<&str> {
    data.get(start..).ok_or(Error::UnexpectedEnd(start))
}

#[cfg(test)]
mod test {

    #[test]
    fn split_once() {
        assert_eq!(None, super::split_once("abc", |c| c == 'x'));
        assert_eq!(
            Some((1, "a", "bcbd")),
            super::split_once("abcbd", |c| c == 'b')
        );
    }

    #[test]
    fn split_at() {
        assert_eq!(None, super::split_at("abc", 5));
        assert_eq!(Some(("a", "bcbd")), super::split_at("abcbd", 1));
    }
    #[test]
    fn gest_rest() {
        assert!(super::get_rest("abc", 5).is_err());
        assert_eq!(Ok("bcbd"), super::get_rest("abcbd", 1));
    }
}
