// Copyright 2020-2021, The Tremor Team
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

use crate::errors::{Error, ErrorKind, Result};
use crate::prelude::*;
use crate::Value;
use std::{io::prelude::*, path::Path};

/// Fetches a hostname with `tremor-host.local` being the default
#[must_use]
pub fn hostname() -> String {
    hostname::get()
        .map_err(|ioe| Error::from(ErrorKind::Io(ioe)))
        .and_then(|hostname| {
            hostname.into_string().map_err(|os_string| {
                ErrorKind::Msg(format!("Invalid hostname: {}", os_string.to_string_lossy())).into()
            })
        })
        .unwrap_or_else(|_| "tremor_host.local".to_string())
}

/// Serialize a Value in a sorted fashion to allow equality comparing the result
pub fn sorted_serialize(j: &Value) -> Result<String> {
    // ballpark size of a 'sensible' message
    let mut w = Vec::with_capacity(512);
    sorted_serialize_(j, &mut w)?;
    Ok(std::str::from_utf8(&w)?.to_string())
}

fn sorted_serialize_<'v, W: Write>(j: &Value<'v>, w: &mut W) -> Result<()> {
    match j {
        Value::Static(_) | Value::String(_) | Value::Bytes(_) => {
            write!(w, "{}", j.encode())?;
        }
        Value::Array(a) => {
            let mut iter = a.iter();
            write!(w, "[")?;

            if let Some(e) = iter.next() {
                sorted_serialize_(e, w)?
            }

            for e in iter {
                write!(w, ",")?;
                sorted_serialize_(e, w)?
            }
            write!(w, "]")?;
        }
        Value::Object(o) => {
            let mut v: Vec<(String, Value<'v>)> =
                o.iter().map(|(k, v)| (k.to_string(), v.clone())).collect();

            v.sort_by_key(|(k, _)| k.to_string());
            let mut iter = v.into_iter();

            write!(w, "{{")?;

            if let Some((k, v)) = iter.next() {
                sorted_serialize_(&Value::from(k), w)?;
                write!(w, ":")?;
                sorted_serialize_(&v, w)?;
            }

            for (k, v) in iter {
                write!(w, ",")?;
                sorted_serialize_(&Value::from(k), w)?;
                write!(w, ":")?;
                sorted_serialize_(&v, w)?;
            }
            write!(w, "}}")?;
        }
    }
    Ok(())
}

fn is_xz_file(filename: &str) -> bool {
    filename
        .rsplit('.')
        .next()
        .map(|ext| ext.eq_ignore_ascii_case("xz"))
        == Some(true)
}

/// Loads an event file required for tests
/// tries to load file and without .xz suffix
pub fn load_event_file(base_name: &str) -> crate::errors::Result<Vec<Value<'static>>> {
    use tremor_common::file as cfile;
    use xz2::read::XzDecoder;

    let (xz_name, name) = if is_xz_file(&base_name) {
        (base_name.to_owned(), base_name.trim_end_matches(".xz"))
    } else {
        let mut tmp = base_name.to_owned();
        tmp.push_str(".xz");
        (tmp, base_name)
    };
    let (effective_name, mut file) = if Path::new(xz_name.as_str()).exists() {
        (xz_name.as_str(), cfile::open(xz_name.as_str())?)
    } else if Path::new(name).exists() {
        (name, cfile::open(name)?)
    } else {
        return Err(format!("File not found or not readable: {}", base_name).into());
    };
    let mut in_data = Vec::new();
    if is_xz_file(&effective_name) {
        XzDecoder::new(file).read_to_end(&mut in_data)?
    } else {
        file.read_to_end(&mut in_data)?
    };

    let mut in_lines = in_data
        .lines()
        .collect::<std::result::Result<Vec<String>, _>>()?;
    let mut in_bytes = Vec::new();
    unsafe {
        for line in &mut in_lines {
            in_bytes.push(line.as_bytes_mut())
        }
    }
    let mut json = Vec::new();
    for bytes in in_bytes {
        json.push(tremor_value::parse_to_value(bytes)?.into_static())
    }
    Ok(json)
}
