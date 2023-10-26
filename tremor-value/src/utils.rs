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

use std::io::Write;

use value_trait::base::Writable;

use crate::Value;

/// Serialize a Value in a sorted fashion to allow equality comparing the result
///
/// # Errors
/// on IO errors
pub fn sorted_serialize(j: &Value) -> Result<Vec<u8>, std::io::Error> {
    // ballpark size of a 'sensible' message
    let mut w = Vec::with_capacity(512);
    sorted_serialize_(j, &mut w)?;
    Ok(w)
}

fn sorted_serialize_<'v, W: Write>(j: &Value<'v>, w: &mut W) -> Result<(), std::io::Error> {
    match j {
        Value::Static(_) | Value::String(_) | Value::Bytes(_) => {
            write!(w, "{}", j.encode())?;
        }
        Value::Array(a) => {
            let mut iter = a.iter();
            write!(w, "[")?;

            if let Some(e) = iter.next() {
                sorted_serialize_(e, w)?;
            }

            for e in iter {
                write!(w, ",")?;
                sorted_serialize_(e, w)?;
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
