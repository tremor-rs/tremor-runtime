// Copyright 2018-2020, Wayfair GmbH
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
use simd_json::prelude::*;
use simd_json::BorrowedValue as Value;
use std::io::prelude::*;

/// Fetches a hostname with `tremor-host.local` being the default
pub fn hostname() -> String {
    hostname::get()
        .map_err(|_| ())
        .and_then(|s| s.into_string().map_err(|_| ()))
        .unwrap_or_else(|_| "tremor-host.local".to_string())
}

/// Serialize a Value in a sorted fashion to allow equality comparing the result
pub fn sorsorted_serialize(j: &Value) -> Result<String> {
    // ballpark size of a 'sensible' message
    let mut w = Vec::with_capacity(512);
    sorted_serialize_(j, &mut w)?;
    Ok(std::str::from_utf8(&w)?.to_string())
}

fn sorted_serialize_<'v, W: Write>(j: &Value<'v>, w: &mut W) -> Result<()> {
    match j {
        Value::Static(_) | Value::String(_) => {
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

/// Loads an event file
pub fn load_event_file(name: &str) -> crate::errors::Result<Vec<Value<'static>>> {
    use simd_json::to_borrowed_value;
    use std::fs::File;
    use xz2::read::XzDecoder;

    let file = File::open(name)?;
    let mut in_data = Vec::new();
    XzDecoder::new(file).read_to_end(&mut in_data)?;
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
        json.push(to_borrowed_value(bytes)?.into_static())
    }
    Ok(json)
}
