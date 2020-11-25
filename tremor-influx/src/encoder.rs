// Copyright 2020, The Tremor Team
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

use crate::{EncoderError as Error, EncoderResult as Result};
use simd_json::prelude::*;
use std::hash::Hash;
use std::io::Write;
use std::{borrow::Borrow, slice::SliceIndex};

/// Tries to compile a value to a influx line value
///
/// # Errors
///
///  * if the input doesn't follow the expected schema
pub fn encode<'input, V>(v: &V) -> Result<Vec<u8>>
where
    V: ValueTrait + Writable + 'input,
    <V as ValueTrait>::Key: Borrow<str> + Hash + Eq + Ord + ToString,
{
    let mut output: Vec<u8> = Vec::with_capacity(512);
    write_escaped_key(
        &mut output,
        v.get("measurement")
            .ok_or(Error::MissingField("measurement"))?
            .as_str()
            .ok_or(Error::MissingField("measurement"))?
            .as_bytes(),
    )?;
    //let mut output: String = v.get("measurement")?.as_str()?.escape();

    let mut tag_collection = v
        .get("tags")
        .ok_or(Error::MissingField("tags"))?
        .as_object()
        .ok_or(Error::InvalidField("tags"))?
        .iter()
        .filter_map(|(key, value)| Some((key, value.as_str()?.to_owned())))
        .collect::<Vec<(&<V as ValueTrait>::Key, String)>>();
    tag_collection.sort_by_key(|v| v.0);

    for (key, value) in tag_collection {
        output.write_all(&[b','])?;
        write_escaped_key(&mut output, key.borrow().as_bytes())?;
        output.write_all(&[b'='])?;
        // For the fields we escape differently then for values ...
        write_escaped_key(&mut output, value.as_bytes())?;
    }

    output.write_all(&[b' '])?;

    let fields = v
        .get("fields")
        .ok_or(Error::MissingField("fields"))?
        .as_object()
        .ok_or(Error::InvalidField("fields"))?;
    let mut field_collection: Vec<(&<V as ValueTrait>::Key, &V)> = fields.iter().collect();
    field_collection.sort_by_key(|v| v.0);
    let mut first = true;
    for (key, value) in field_collection {
        if first {
            first = false;
        } else {
            output.write_all(&[b','])?;
        }
        write_escaped_key(&mut output, key.borrow().as_bytes())?;
        output.write_all(&[b'='])?;

        if let Some(s) = value.as_str() {
            write_escaped_value(&mut output, s.as_bytes())?
        } else {
            match value.value_type() {
                ValueType::F64 | ValueType::Bool => value.write(&mut output)?,
                ValueType::U64 | ValueType::I64 => {
                    value.write(&mut output)?;
                    output.write_all(&[b'i'])?
                }
                _ => return Err(Error::InvalidValue(key.to_string(), value.value_type())),
            }
        }
    }

    output.write_all(&[b' '])?;
    let t = v.get("timestamp").ok_or(Error::MissingField("timestamp"))?;
    if t.is_u64() {
        t.write(&mut output)?;
        Ok(output)
    } else {
        Err(Error::InvalidTimestamp(t.value_type()))
    }
}

#[inline]
fn write_escaped_value<W: Write>(writer: &mut W, string: &[u8]) -> Result<()> {
    let mut start = 0;

    writer.write_all(&[b'"'])?;
    for (index, ch) in string.iter().enumerate().skip(start) {
        let ch = *ch;
        if ch == b'"' || ch == b'\\' {
            write_substr(writer, string, start..index)?;
            writer.write_all(&[b'\\', ch])?;
            start = index + 1;
        }
    }
    write_substr(writer, string, start..)?;
    writer.write_all(&[b'"'])?;
    Ok(())
}

#[inline]
fn write_escaped_key<W: Write>(writer: &mut W, string: &[u8]) -> Result<()> {
    let mut start = 0;

    for (index, ch) in string.iter().enumerate().skip(start) {
        let ch = *ch;
        if ch == b'"' || ch == b'\\' || ch == b',' || ch == b' ' || ch == b'=' {
            write_substr(writer, string, start..index)?;
            writer.write_all(&[b'\\', ch])?;
            start = index + 1;
        }
    }
    write_substr(writer, string, start..)?;
    Ok(())
}

fn write_substr<W: Write, I: SliceIndex<[u8], Output = [u8]>>(
    writer: &mut W,
    data: &[u8],
    r: I,
) -> std::io::Result<()> {
    use std::io;
    data.get(r).map_or_else(
        || Err(io::Error::new(io::ErrorKind::Other, "Nothing to write")),
        |s| writer.write_all(s),
    )
}
