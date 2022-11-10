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
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]

use crate::datetime::{_parse, has_tz};
use crate::errors::{Error, Result};
use crate::prelude::*;
use crate::registry::{FResult, FunctionError, Mfa, Registry};
use crate::tremor_const_fn;
use beef::Cow;
use chrono::FixedOffset;
use chrono::{DateTime, Datelike, TimeZone, Timelike};

const TIMESTAMP: Cow<'static, str> = Cow::const_str("timestamp");
const TIMEZONE: Cow<'static, str> = Cow::const_str("tz");
const INVALID_OFFSET_MSG: &str = "Invalid timezone offset";

/// code should return `FunctionResult`
macro_rules! with_datetime_input {
    ($input:ident, $output:ident, $to_function_err:ident, $code:block) => {
        if let Some(timestamp) = $input.as_u64() {
            let timezone = chrono_tz::UTC;
            let $output = nanos_to_datetime(&timezone, timestamp).map_err($to_function_err)?;
            $code
        } else if let Some(map) = $input.as_object() {
            if let Some(timestamp) = map.get(&TIMESTAMP).and_then(|v| v.as_u64()) {
                if let Some(timezone_value) = map.get(&TIMEZONE) {
                    with_timezone!(timezone_value, tz, $to_function_err, {
                        let $output =
                            nanos_to_datetime(&tz, timestamp).map_err($to_function_err)?;
                        $code
                    })
                } else {
                    // timezone is optional
                    let timezone = chrono_tz::UTC;
                    let $output =
                        nanos_to_datetime(&timezone, timestamp).map_err($to_function_err)?;
                    $code
                }
            } else {
                return Err($to_function_err(Error::from(
                    "Missing or invalid \"timestamp\" field",
                )));
            }
        } else {
            return Err($to_function_err(Error::from(format!(
                "Invalid datetime input of type {}",
                $input.value_type()
            ))));
        }
    };
}

/// parse a fixed offset string into number of seconds
///
/// Supports the following formats:
///
/// - `+09:00`
/// - `-12:00`
/// - `08:30`
/// - `02`
/// - `+02`
/// - `0912`
/// - `-0912`
fn parse_fixed_offset(offset: &str) -> Result<i32> {
    let bytes = offset.as_bytes();
    let mut idx = 0_usize;
    let negative = match bytes.get(idx) {
        Some(byte @ (&b'-' | &b'+')) => {
            idx += 1;
            byte == &b'-'
        }
        Some(b) if u8::is_ascii_digit(b) => false,
        Some(_) | None => return Err(Error::from(INVALID_OFFSET_MSG)),
    };
    if bytes.len() < 2 {
        return Err(Error::from(INVALID_OFFSET_MSG));
    }
    let hours = match (bytes.get(idx), bytes.get(idx + 1)) {
        (Some(h1 @ b'0'..=b'9'), Some(h2 @ b'0'..=b'9')) => {
            i32::from((h1 - b'0') * 10 + (h2 - b'0'))
        }
        _ => return Err(Error::from(INVALID_OFFSET_MSG)),
    };
    idx += 2;
    if let Some(&b':') = bytes.get(idx) {
        idx += 1;
    }
    let minutes = match (bytes.get(idx), bytes.get(idx + 1)) {
        (Some(m1 @ b'0'..=b'5'), Some(m2 @ b'0'..=b'9')) => {
            i32::from((m1 - b'0') * 10 + (m2 - b'0'))
        }
        (None, None) => 0,
        _ => return Err(Error::from(INVALID_OFFSET_MSG)),
    };
    let seconds = hours * 3600 + minutes * 60;
    Ok(if negative { -seconds } else { seconds })
}

/// Parses the given `timezone_value` and executes the `code` with the successfully parsed timezone as `tz_ident`.
///
/// requirements:
/// - must be used within a function that returns a `FunctionResult`
macro_rules! with_timezone {
    ($timezone_value:expr, $tz_ident:ident, $to_function_err:ident, $code:block) => {{
        if let Some(timezone_id) = $timezone_value.as_usize() {
            let $tz_ident = chrono_tz::TZ_VARIANTS
                .get(timezone_id)
                .ok_or_else(|| {
                    $to_function_err(Error::from(format!(
                        "Invalid timezone identifier {timezone_id}"
                    )))
                })?
                .clone();
            $code
        } else if let Some(timezone_str) = $timezone_value.as_str() {
            match timezone_str.parse::<chrono_tz::Tz>() {
                Ok(tz) => {
                    let $tz_ident = tz;
                    $code
                }
                Err(_e) => {
                    let offset_secs = parse_fixed_offset(timezone_str).map_err($to_function_err)?;
                    let $tz_ident = FixedOffset::east(offset_secs);
                    $code
                }
            }
        } else {
            return Err($to_function_err(Error::from(format!(
                "Invalid timezone type {}",
                $timezone_value.value_type()
            ))));
        }
    }};
}

macro_rules! datetime_fn {
    ($name:ident, $datetime_ident:ident, $code:expr) => {
        tremor_const_fn! (datetime|$name(_context, _value) {
            with_datetime_input!(_value, $datetime_ident, to_runtime_error, { Ok(Value::from($code)) })
        })
    };
}

pub fn load(registry: &mut Registry) {
    registry
        .insert(
            tremor_const_fn! (datetime|parse(_context, _input : String,  _input_fmt: String) {
             let res = _parse(_input, _input_fmt, has_tz(_input_fmt));
             match res {
                 Ok(x) => Ok(Value::from(x)),
                 Err(e)=> Err(FunctionError::RuntimeError { mfa: mfa( "datetime",  "parse", 1), error: e.to_string() })
        }}))
        .insert(tremor_const_fn!(datetime|format(_context, _datetime, _format) {
            let format = _format.as_str().ok_or_else(||FunctionError::BadType {mfa: this_mfa()})?;
            with_datetime_input!(_datetime, datetime, to_runtime_error, {
                // timestamp and timezone are in scope
                Ok(Value::from(datetime.format(format).to_string()))
            })
        }))
        .insert(tremor_const_fn!(datetime|rfc3339(_context, _datetime) {
            with_datetime_input!(_datetime, datetime, to_runtime_error, {
                Ok(Value::from(datetime.to_rfc3339()))
            })
        }))
        .insert(tremor_const_fn!(datetime|rfc2822(_context, _datetime) {
            with_datetime_input!(_datetime, datetime, to_runtime_error, {
                Ok(Value::from(datetime.to_rfc2822()))
            })
        }))
        .insert(tremor_const_fn!(datetime|with_timezone(_context, _timestamp, _timezone) {
            // this macro ensures we have a valid timezone
            with_timezone!(_timezone, _tz, to_runtime_error, {
                to_timezone_object(*_timestamp, *_timezone, this_mfa)
            })
        }))
        .insert(datetime_fn!(year, datetime, datetime.year()))
        .insert(datetime_fn!(month, datetime, datetime.month()))
        .insert(datetime_fn!(iso_week, datetime, datetime.iso_week().week()))
        .insert(datetime_fn!(day_of_month, datetime, datetime.day()))
        .insert(datetime_fn!(day_of_year, datetime, datetime.ordinal()))
        .insert(datetime_fn!(hour, datetime, datetime.hour()))
        .insert(datetime_fn!(minute, datetime, datetime.minute()))
        .insert(datetime_fn!(second, datetime, datetime.second()))
        .insert(datetime_fn!(subsecond_millis, datetime, datetime.timestamp_subsec_millis()))
        .insert(datetime_fn!(subsecond_micros, datetime, datetime.timestamp_subsec_micros()))
        .insert(datetime_fn!(subsecond_nanos, datetime, datetime.timestamp_subsec_nanos()));
}

fn to_timezone_object<'event>(
    ts: &Value<'event>,
    timezone: &Value<'event>,
    mfa_fn: impl Fn() -> Mfa,
) -> FResult<Value<'event>> {
    if let Some(ts) = ts.as_u64() {
        let mut new_object = Value::object_with_capacity(2);
        new_object.try_insert(TIMESTAMP, Value::from(ts));
        new_object.try_insert(TIMEZONE, (*timezone).clone());
        Ok(new_object)
    } else if ts.is_object() {
        let ts = ts
            .get_u64(&TIMESTAMP)
            .ok_or_else(|| FunctionError::RuntimeError {
                mfa: mfa_fn(),
                error: "Invalid datetime input: Missing or invalid \"timestamp\" field."
                    .to_string(),
            })?;
        let mut new_object = Value::object_with_capacity(2);
        new_object.try_insert(TIMESTAMP, Value::from(ts));
        new_object.try_insert(TIMEZONE, (*timezone).clone());
        Ok(new_object)
    } else {
        Err(FunctionError::BadType { mfa: mfa_fn() })
    }
}

/// proven way to convert nanos to a datetime with the given timezone
fn nanos_to_datetime<TZ: TimeZone>(tz: &TZ, nanos: u64) -> Result<DateTime<TZ>> {
    let (secs, nanos) = (nanos / 1_000_000_000, nanos % 1_000_000_000);
    tz.timestamp_opt(secs as i64, nanos as u32)
        .single()
        .ok_or_else(|| Error::from("invalid nanos timestamp"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::Mfa;
    use chrono::{NaiveDate, Offset, TimeZone};
    use test_case::test_case;

    #[test]
    pub fn parse_at_timestamp() {
        let time = "2019-06-17T13:15:40.752Z";
        let output =
            _parse(time, "%Y-%m-%dT%H:%M:%S%.3fZ", false).expect("cannot parse datetime string");
        assert_eq!(output, 1_560_777_340_752_000_000);
    }
    #[test]
    pub fn parse_parses_it_to_ts() {
        let output = _parse(
            "1983 Apr 13 12:09:14.274 +0000",
            "%Y %b %d %H:%M:%S%.3f %z",
            true,
        )
        .expect("cannot parse datetime string");

        assert_eq!(output, 419_083_754_274_000_000);
    }

    #[test]
    pub fn parse_unix_ts() {
        let output = _parse("1560777212", "%s", false).expect("cannot parse datetime string");

        assert_eq!(output, 1_560_777_212_000_000_000);
    }

    fn to_ferr(e: crate::errors::Error) -> crate::registry::FunctionError {
        crate::registry::to_runtime_error(Mfa::new("datetime", "with_timezone", 2), e)
    }

    #[test_case("Europe/Berlin", 3600; "berlin")]
    #[test_case("Europe/London", 3600; "london")]
    #[test_case("UTC", 0 ; "utc")]
    #[test_case("NZ", 43200; "nz")]
    #[test_case("Pacific/Pitcairn", -30600; "pitcairn")]
    #[test_case("Zulu", 0; "zulu")]
    #[test_case("02", 7200; "only hours")]
    #[test_case("-02", -7200; "negative hours")]
    #[test_case("-0230", -9000; "negative without colon")]
    #[test_case("-02:30", -9000; "negative with colon")]
    #[test_case("+02:30", 9000; "positive with colon")]
    fn with_timezone_str(input: &str, offset_secs: i32) {
        let res = (|| {
            with_timezone!(Value::from(input), tz, to_ferr, {
                let naive = NaiveDate::from_ymd(1970, 1, 1);
                assert_eq!(
                    offset_secs,
                    tz.offset_from_utc_date(&naive).fix().local_minus_utc()
                );
                Ok(())
            })
        })();
        assert!(res.is_ok(), "Expected Ok(()), got: {res:?}");
    }

    #[test]
    fn with_timezone_index() {
        for input in 0_usize..chrono_tz::TZ_VARIANTS.len() {
            let res = (|| with_timezone!(Value::from(input), _tz, to_ferr, { Ok(()) }))();
            assert!(
                res.is_ok(),
                "{}: Expected Ok(()), got: {res:?}",
                chrono_tz::TZ_VARIANTS[input].name()
            );
        }
    }

    #[test]
    fn parse_fixed_offset_err() {
        assert!(parse_fixed_offset("").is_err());
        assert!(parse_fixed_offset("-").is_err());
        assert!(parse_fixed_offset("+1").is_err());
        assert!(parse_fixed_offset("@01:00").is_err());
        assert!(parse_fixed_offset("01ABC").is_err());
        assert!(parse_fixed_offset("+01:79").is_err());
    }

    #[test]
    fn parse_fixed_offset_ok() {
        assert_eq!(parse_fixed_offset("-00"), Ok(0));
        assert_eq!(parse_fixed_offset("0049"), Ok(49 * 60));
        assert_eq!(parse_fixed_offset("+02:30"), Ok(9000));
        assert_eq!(parse_fixed_offset("02:30"), Ok(9000));
        assert_eq!(parse_fixed_offset("0230"), Ok(9000));
        assert_eq!(parse_fixed_offset("-02:30"), Ok(-9000));
        assert_eq!(parse_fixed_offset("-0230"), Ok(-9000));
    }

    #[test]
    fn parse_fixed_offset_all_timezones() {
        for tz in chrono_tz::TZ_VARIANTS {
            let dt = tz.timestamp(0, 0);
            let tz_str1 = dt.format("%z").to_string();
            let tz_str3 = dt.format("%:z").to_string();
            assert!(
                parse_fixed_offset(tz_str1.as_str()).is_ok(),
                "Failed parsing offset {tz_str1}"
            );
            assert!(
                parse_fixed_offset(tz_str3.as_str()).is_ok(),
                "Failed parsing offset {tz_str3}"
            );
        }
    }

    #[test]
    fn nanos_to_datetime_coverage() -> Result<()> {
        let input = 0_u64;
        let dt = nanos_to_datetime(&chrono_tz::UTC, input)?;
        assert_eq!(input as i64, dt.timestamp_nanos());
        Ok(())
    }

    #[test]
    fn to_timezone_object_err() {
        let res = to_timezone_object(&literal!(null), &literal!("Europe/Berlin"), || {
            Mfa::new("datetime", "with_timezone", 2)
        });
        assert!(res.is_err(), "Expected Err, got {res:?}");
        let res = to_timezone_object(&literal!([]), &literal!("Europe/Berlin"), || {
            Mfa::new("datetime", "with_timezone", 2)
        });
        assert!(res.is_err(), "Expected Err, got {res:?}");
        let res = to_timezone_object(&literal!({}), &literal!("Europe/Berlin"), || {
            Mfa::new("datetime", "with_timezone", 2)
        });
        assert!(res.is_err(), "Expected Err, got {res:?}");
        let res = to_timezone_object(
            &literal!({"timestamp": "string"}),
            &literal!("Europe/Berlin"),
            || Mfa::new("datetime", "with_timezone", 2),
        );
        assert!(res.is_err(), "Expected Err, got {res:?}");
    }

    #[test]
    fn to_timezone_object_success() {
        let res = to_timezone_object(&literal!(1_000_000_000), &literal!("Europe/Berlin"), || {
            Mfa::new("datetime", "with_timezone", 2)
        });
        assert_eq!(
            Ok(literal!({
                "timestamp": 1_000_000_000,
                "tz": "Europe/Berlin"
            })),
            res
        );

        let res = to_timezone_object(
            &literal!(1_000_000_000),
            &literal!(chrono_tz::TZ_VARIANTS.len() - 1),
            || Mfa::new("datetime", "with_timezone", 2),
        );
        assert_eq!(
            Ok(literal!({
                "timestamp": 1_000_000_000,
                "tz": chrono_tz::TZ_VARIANTS.len() - 1
            })),
            res
        );
    }
}
