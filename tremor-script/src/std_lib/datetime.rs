// Copyright 2018-2019, Wayfair GmbH
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

use crate::datetime::*;
use crate::registry::Registry;
use crate::tremor_fn;
use chrono::{offset::Utc, Datelike, NaiveDateTime, SubsecRound, Timelike};

pub fn load(registry: &mut Registry) {
    registry
        .insert(
        tremor_fn! (datetime::parse(_context, _input : String,  _input_fmt: String) {
             let res = _parse(&_input, _input_fmt, has_tz(_input_fmt));
             match res {
                 Ok(x) => Ok(Value::from(x)),
                 Err(e)=> Err(FunctionError::RuntimeError { mfa: mfa( "datetime",  "parse", 1), error:  format!("Cannot Parse {} to valid timestamp", e), 
})
}}))

        .insert(
            tremor_fn!(datetime::iso8601(_context, _datetime: I64) {
              let res = _iso8601(* _datetime as u64);
                 Ok(Value::from(res))
            }))
        .insert(tremor_fn!(datetime::format(_context, _datetime: I64, _fmt: String) {
            let res = _format(* _datetime as u64, _fmt);
            Ok(Value::from(res))
        }))
        .insert(tremor_fn!(datetime::second(_context, _value: I64) {
            Ok(Value::from(_second(*_value as u64)))
        }))
        .insert(tremor_fn!(datetime::minute(_context, _value: I64) {
            Ok(Value::from(_minute(*_value as u64)))
        }))
        .insert(tremor_fn!(datetime::hour(_context, _value: I64) {
            Ok(Value::from(_hour(*_value as u64)))
        }))
        .insert(tremor_fn!(datetime::millisecond(_context, _value: I64) {
            Ok(Value::from(_millisecond(*_value as u64)))
        }))
        .insert(tremor_fn!(datetime::microsecond(_context, _value: I64) {
            Ok(Value::from(_microsecond(*_value as u64)))
        }))
        .insert(tremor_fn!(datetime::nanosecond(_context, _value: I64) {
            Ok(Value::from(_nanosecond(*_value as u64)))
        }))
        .insert(tremor_fn!(datetime::day(_context, _value: I64) {
            Ok(Value::from(_day(*_value as u64)))
        }))
        .insert(tremor_fn!(datetime::month(_context, _value: I64) {
            Ok(Value::from(_month(*_value as u64)))
        }))
        .insert(tremor_fn!(datetime::year(_context, _value: I64) {
            Ok(Value::from(_year(*_value as u64)))
        }))
        .insert(tremor_fn!(datetime::today(_context) {
            Ok(Value::from(_today()))
        }))
        .insert(tremor_fn!(datetime::subsecond(_context, _value: I64) {
            Ok(Value::from(_subsecond(*_value as u64)))
        }))
        .insert(tremor_fn!(datetime::to_nearest_millisecond(_context, _value: I64) {
            Ok(Value::from(_to_nearest_millisecond(*_value as u64)))
        }))
        .insert(tremor_fn!(datetime::to_nearest_microsecond(_context, _value: I64) {
            Ok(Value::from(_to_nearest_microsecond(*_value as u64)))
        }))
        .insert(tremor_fn!(datetime::to_nearest_second(_context, _value: I64) {
              Ok(Value::from(_to_nearest_second(*_value as u64)))
        }))
        .insert(tremor_fn!(datetime::from_human_format(_context, _value: String) {
            match _from_human_format(_value) {
                Some(x) => Ok(Value::from(x)),
                None => Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("The human format {} is invalid", _value)})
        }
        }))
        .insert(tremor_fn!(datetime::with_nanoseconds(_context, _n: I64) {
            Ok(Value::from(_with_nanoseconds(*_n as u32)))
        }))
        .insert(tremor_fn!(datetime::with_microseconds(_context, _n: I64) {
            Ok(Value::from(_with_microseconds(*_n as u32)))
        }))
        .insert(tremor_fn!(datetime::with_milliseconds(_context, _n: I64) {
            Ok(Value::from(_with_milliseconds(*_n as u32)))
        }))
        .insert(tremor_fn!(datetime::with_seconds(_context, _n: I64) {
            Ok(Value::from(_with_seconds(*_n as u32)))
        }))
        .insert(tremor_fn!(datetime::with_minutes(_context, _n: I64) {
            Ok(Value::from(_with_minutes(*_n as u32)))
        }))
        .insert(tremor_fn!(datetime::with_hours(_context, _n: I64) {
            Ok(Value::from(_with_hours(*_n as u32)))
        }))
        .insert(tremor_fn!(datetime::with_days(_context, _n: I64) {
            Ok(Value::from(_with_days(*_n as u32)))
        }))
        .insert(tremor_fn!(datetime::with_weeks(_context, _n: I64) {
            Ok(Value::from(_with_weeks(*_n as u32)))
        }))
        .insert(tremor_fn!(datetime::with_years(_context, _n: I64) {
            Ok(Value::from(_with_years(*_n as u32)))
        }))
        .insert(tremor_fn!(datetime::without_subseconds(_context, _n: I64) {
            Ok(Value::from(_without_subseconds(*_n as u64)))
        }));
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
            (Some(1), &"minute") => acc + _with_days(1),
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
        assert_eq!(output, 1560777340752000000);
    }
    #[test]
    pub fn parse_parses_it_to_ts() {
        let output = _parse(
            "1983 Apr 13 12:09:14.274 +0000",
            "%Y %b %d %H:%M:%S%.3f %z",
            true,
        )
        .expect("cannot parse datetime string");

        assert_eq!(output, 419083754274000000);
    }

    #[test]
    pub fn parse_unix_ts() {
        let output = _parse("1560777212", "%s", false).expect("cannot parse datetime string");

        assert_eq!(output, 1560777212000000000);
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
        let input = 1559655782_987_654_321u64;
        let output = _millisecond(input);
        assert_eq!(output, 987u32);
    }

    #[test]
    pub fn to_next_millisecond_rounds_it() {
        let input = 1_559_655_782_123_456_789u64;
        let output = _to_nearest_millisecond(input);
        assert_eq!(output, 1559655782_123_000_000u64);
        assert_eq!(_to_nearest_millisecond(123_789_654u64), 124_000_000);
    }

    #[test]
    pub fn to_nearest_microsecond_rounds_it() {
        assert_eq!(
            _to_nearest_microsecond(1_559_655_782_123_456_789u64),
            1559655782_123_457_000
        );
        assert_eq!(_to_nearest_microsecond(123_456_123u64), 123_456_000);
    }

    #[test]
    pub fn to_next_second_rounds_it() {
        let input = 1_559_655_782_123_456_789u64;
        let output = _to_nearest_second(input);
        assert_eq!(output, 1559655782_000_000_000);
    }

    #[test]
    pub fn day_month_and_year_works() {
        let input = 1555767782_123_456_789u64;
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
            Some(1814585_000_000_000)
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
        assert_eq!(_without_subseconds(input), 1559655782u64);
    }

}
