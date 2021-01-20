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
//

use crate::errors::{Error, Result};
use chrono::{DateTime, NaiveDateTime};

#[allow(clippy::cast_sign_loss)]
pub fn _parse(datetime: &str, input_fmt: &str, has_timezone: bool) -> Result<u64> {
    if has_timezone {
        Ok(DateTime::parse_from_str(datetime, input_fmt)
            .map_err(|e| Error::from(format!("Datetime Parse Error: {:?}", e)))?
            .timestamp_nanos() as u64)
    } else {
        Ok(NaiveDateTime::parse_from_str(datetime, input_fmt)
            .map_err(|e| Error::from(format!("Datetime Parse Error: {:?}", e)))?
            .timestamp_nanos() as u64)
    }
}

pub fn has_tz(fmt: &str) -> bool {
    let mut chrs = fmt.chars();
    while let Some(c) = chrs.next() {
        if c == '%' {
            match chrs.next() {
                Some('+') | Some('z') | Some('Z') => return true,
                Some(':') | Some('#') => {
                    if chrs.next() == Some('z') {
                        return true;
                    }
                }
                _ => (),
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_simple_string_with_format() {
        let format = "%Y-%m-%dT%T%.6f%:z";
        let output = _parse("2019-08-07T16:41:12.159975-04:00", format, has_tz(format))
            .expect("parse datetime");
        assert_eq!(output, 1_565_210_472_159_975_000);
    }

    #[test]
    pub fn test_simple_string_without_tz() {
        let format = "%Y-%m-%dT%T%.6f";
        let output = _parse("2019-08-07T20:41:12.159975", format, has_tz(format))
            .expect("cannot parse datetime");
        assert_eq!(output, 1_565_210_472_159_975_000);
    }
}
