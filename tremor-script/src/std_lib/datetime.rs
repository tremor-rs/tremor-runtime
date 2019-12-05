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
//
// Dylans code ...
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]

use crate::datetime::*;
use crate::registry::Registry;
use crate::{tremor_const_fn, tremor_fn};
use chrono::{offset::Utc, Datelike, NaiveDateTime, SubsecRound, Timelike};
use simd_json::Value as ValueTrait;

macro_rules! time_fn {
    ($name:ident, $fn:ident) => {
        tremor_const_fn! (datetime::$name(_context, _value) {
            _value.as_u64().map($fn).map(Value::from).ok_or_else(||FunctionError::BadType{ mfa: this_mfa() })
        })
    };
}
macro_rules! time_fn_32 {
    ($name:ident, $fn:ident) => {
        tremor_const_fn! (datetime::$name(_context, _value) {
            _value.as_u32().map($fn).map(Value::from).ok_or_else(||FunctionError::BadType{ mfa: this_mfa() })
        })
    };
}

pub fn load(registry: &mut Registry) {
    registry
        .insert(
            tremor_const_fn! (datetime::parse(_context, _input : String,  _input_fmt: String) {
             let res = _parse(&_input, _input_fmt, has_tz(_input_fmt));
             match res {
                 Ok(x) => Ok(Value::from(x)),
                 Err(e)=> Err(FunctionError::RuntimeError { mfa: mfa( "datetime",  "parse", 1), error:  format!("Cannot Parse {} to valid timestamp", e), 
})
}}))

        .insert(time_fn!(iso8601, _iso8601))
        .insert(tremor_const_fn!(datetime::format(_context, _datetime, _fmt) {
            if let (Some(datetime), Some(fmt)) = (_datetime.as_u64(), _fmt.as_str()) {
                Ok(Value::from(_format(datetime, fmt)))
            } else {
                Err(FunctionError::BadType{ mfa: this_mfa() })
            }
        }))
        .insert(time_fn!(year, _year))
        .insert(time_fn!(month, _month))
        .insert(time_fn!(day, _day))
        .insert(time_fn!(hour, _hour))
        .insert(time_fn!(minute, _minute))
        .insert(time_fn!(second, _second))
        .insert(time_fn!(millisecond, _millisecond))
        .insert(time_fn!(microsecond, _microsecond))
        .insert(time_fn!(nanosecond, _nanosecond))
        .insert(tremor_fn!(datetime::today(_context) {
            Ok(Value::from(_today()))
        }))
        .insert(time_fn!(subsecond, _subsecond))
        .insert(time_fn!(to_nearest_millisecond, _to_nearest_millisecond))
        .insert(time_fn!(to_nearest_microsecond, _to_nearest_microsecond))
        .insert(time_fn!(to_nearest_second, _to_nearest_second))
        .insert(tremor_const_fn!(datetime::from_human_format(_context, _value: String) {
            match _from_human_format(_value) {
                Some(x) => Ok(Value::from(x)),
                None => Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("The human format {} is invalid", _value)})
            }
        }))
        .insert(time_fn_32!(with_nanoseconds, _with_nanoseconds))
        .insert(time_fn_32!(with_microseconds, _with_microseconds))
        .insert(time_fn_32!(with_milliseconds, _with_milliseconds))
        .insert(time_fn_32!(with_seconds, _with_seconds))
        .insert(time_fn_32!(with_minutes, _with_minutes))
        .insert(time_fn_32!(with_hours, _with_hours))
        .insert(time_fn_32!(with_days, _with_days))
        .insert(time_fn_32!(with_weeks, _with_weeks))
        .insert(time_fn_32!(with_years, _with_years))
        .insert(time_fn!(without_subseconds, _without_subseconds));
}

pub fn _iso8601(datetime: u64) -> String {
    _format(datetime, "%Y-%m-%dT%H:%M:%S%.9f+00:00")
}

pub fn _format(value: u64, fmt: &str) -> String {
    format!("{}", to_naive_datetime(value).format(fmt))
}
pub fn _second(value: u64) -> u8 {
    to_naive_datetime(value).second() as u8
}

pub fn _minute(value: u64) -> u8 {
    to_naive_datetime(value).minute() as u8
}

pub fn _hour(value: u64) -> u8 {
    to_naive_datetime(value).hour() as u8
}

pub fn _day(value: u64) -> u8 {
    to_naive_datetime(value).day() as u8
}

pub fn _month(value: u64) -> u8 {
    to_naive_datetime(value).month() as u8
}

pub fn _year(value: u64) -> u32 {
    to_naive_datetime(value).year() as u32
}

pub fn _millisecond(value: u64) -> u32 {
    to_naive_datetime(value).nanosecond() / 1_000_000
}

pub fn _microsecond(value: u64) -> u32 {
    to_naive_datetime(value).nanosecond() / 1_000 % 1_000
}

pub fn _nanosecond(value: u64) -> u32 {
    to_naive_datetime(value).nanosecond() % 1000
}

pub fn _subsecond(value: u64) -> u32 {
    to_naive_datetime(value).nanosecond()
}

fn to_naive_datetime(value: u64) -> NaiveDateTime {
    NaiveDateTime::from_timestamp(
        (value / 1_000_000_000) as i64,
        (value % 1_000_000_000) as u32,
    )
}

pub fn _to_nearest_millisecond(value: u64) -> u64 {
    let ndt = to_naive_datetime(value);
    let ns = ndt.round_subsecs(3).nanosecond();
    value / 1_000_000_000 * 1_000_000_000 + u64::from(ns)
}

pub fn _to_nearest_microsecond(value: u64) -> u64 {
    let ns = to_naive_datetime(value).round_subsecs(6).nanosecond();
    value / 1_000_000_000 * 1_000_000_000 + u64::from(ns)
}

pub fn _to_nearest_second(value: u64) -> u64 {
    let ms = _subsecond(value);
    if ms < 500_000_000 {
        value - u64::from(ms)
    } else {
        value - u64::from(ms) + 1_000_000_000
    }
}

pub fn _without_subseconds(value: u64) -> u64 {
    value / 1_000_000_000
}

pub fn _today() -> u64 {
    Utc::today().and_hms(0, 0, 0).timestamp_nanos() as u64
}

pub fn _from_human_format(human: &str) -> Option<u64> {
    let tokens = human.split(' ').collect::<Vec<&str>>();

    let res = tokens
        .chunks(2)
        .filter_map(|sl| Some((sl[0].parse::<u32>().ok(), sl.get(1)?)))
        .fold(0, |acc, sl| match sl {
            (Some(n), &"years") => acc + _with_years(n),
            (Some(n), &"weeks") => acc + _with_weeks(n),
            (Some(n), &"days") => acc + _with_days(n),
            (Some(n), &"minutes") => acc + _with_minutes(n),
            (Some(n), &"seconds") => acc + _with_seconds(n),
            (Some(n), &"milliseconds") => acc + _with_milliseconds(n),
            (Some(n), &"microseconds") => acc + _with_microseconds(n),
            (Some(n), &"nanoseconds") => acc + _with_nanoseconds(n),
            (Some(1), &"year") => acc + _with_years(1),
            (Some(1), &"week") => acc + _with_weeks(1),
            (Some(1), &"day") => acc + _with_days(1),
            (Some(1), &"minute") => acc + _with_minutes(1),
            (Some(1), &"second") => acc + _with_seconds(1),
            (Some(1), &"millisecond") => acc + _with_milliseconds(1),
            (Some(1), &"microsecond") => acc + _with_microseconds(1),
            (Some(1), &"nanosecond") => acc + _with_nanoseconds(1),
            _ => acc,
        });

    if res > 0 {
        Some(res)
    } else {
        None
    }
}

pub const fn _with_nanoseconds(n: u32) -> u64 {
    n as u64
}

pub const fn _with_microseconds(n: u32) -> u64 {
    (n * 1_000) as u64
}

pub const fn _with_milliseconds(n: u32) -> u64 {
    (n * 1_000_000) as u64
}

pub const fn _with_seconds(n: u32) -> u64 {
    (n as u64 * 1_000_000_000)
}

pub const fn _with_minutes(n: u32) -> u64 {
    _with_seconds(n * 60)
}

pub const fn _with_hours(n: u32) -> u64 {
    _with_minutes(n * 60)
}

pub const fn _with_days(n: u32) -> u64 {
    _with_hours(n * 24)
}

pub const fn _with_weeks(n: u32) -> u64 {
    _with_days(n * 7)
}

pub const fn _with_years(n: u32) -> u64 {
    _with_days(n * 365) + _with_days(n / 4)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn parse_at_timestamp() {
        let time = "2019-06-17T13:15:40.752Z";
        //let time = "2019-06-17T13:15:40.752";
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

    #[test]
    pub fn hms_returns_the_corresponding_components() {
        let input = 1_559_655_782_123_456_789u64;
        let output = _hour(input);
        assert_eq!(output, 13);
        assert_eq!(_minute(input), 43);
        assert_eq!(_second(input), 2);
        assert_eq!(_millisecond(input), 123);
        assert_eq!(_microsecond(input), 456);
        assert_eq!(_nanosecond(input), 789);
    }

    #[test]
    pub fn millisecond_returns_the_ms() {
        let input = 1_559_655_782_987_654_321u64;
        let output = _millisecond(input);
        assert_eq!(output, 987u32);
    }

    #[test]
    pub fn to_next_millisecond_rounds_it() {
        let input = 1_559_655_782_123_456_789u64;
        let output = _to_nearest_millisecond(input);
        assert_eq!(output, 1_559_655_782_123_000_000u64);
        assert_eq!(_to_nearest_millisecond(123_789_654u64), 124_000_000);
    }

    #[test]
    pub fn to_nearest_microsecond_rounds_it() {
        assert_eq!(
            _to_nearest_microsecond(1_559_655_782_123_456_789u64),
            1_559_655_782_123_457_000
        );
        assert_eq!(_to_nearest_microsecond(123_456_123u64), 123_456_000);
    }

    #[test]
    pub fn to_next_second_rounds_it() {
        let input = 1_559_655_782_123_456_789u64;
        let output = _to_nearest_second(input);
        assert_eq!(output, 1_559_655_782_000_000_000);
    }

    #[test]
    pub fn day_month_and_year_works() {
        let input = 1_555_767_782_123_456_789u64;
        assert_eq!(_day(input), 20);
        assert_eq!(_month(input), 4);
        assert_eq!(_year(input), 2019);
    }

    #[test]
    pub fn test_human_format() {
        assert_eq!(_from_human_format("3 days"), Some(259_200_000_000_000));
        assert_eq!(_from_human_format("59 seconds"), Some(59_000_000_000));
        assert_eq!(
            _from_human_format("21 days 3 minutes 5 seconds"),
            Some(1_814_585_000_000_000)
        );
        assert_eq!(_from_human_format("3"), None);
        assert_eq!(_from_human_format("1 nanosecond"), Some(1));
    }

    #[test]
    pub fn iso8601_format() {
        let input = 1_559_655_782_123_456_789u64;
        let output = _iso8601(input);
        assert_eq!(output, "2019-06-04T13:43:02.123456789+00:00".to_string());
    }

    #[test]
    pub fn test_format() {
        assert_eq!(
            _format(1_559_655_782_123_567_892u64, "%Y-%m-%d"),
            "2019-06-04".to_owned()
        );
        assert_eq!(_format(123_567_892u64, "%Y-%m-%d"), "1970-01-01".to_owned());
    }

    #[test]
    pub fn without_subseconds() {
        let input = 1_559_655_782_123_456_789u64;
        assert_eq!(_without_subseconds(input), 1_559_655_782u64);
    }
}
