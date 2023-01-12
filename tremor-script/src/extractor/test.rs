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

use super::Result::{Match, MatchNull, NoMatch};
use super::*;
use crate::Value;
use halfbrown::hashmap;

use matches::assert_matches;
#[test]
fn test_reg_extractor() {
    let ex = Extractor::new("rerg", "(?P<key>[^=]+)=(?P<val>[^&]+)&").expect("bad extractor");
    match ex {
        Extractor::Rerg { .. } => {
            assert_eq!(
                ex.extract(
                    true,
                    &Value::from("foo=bar&baz=bat&"),
                    &EventContext::new(0, None)
                ),
                Match(Value::from(hashmap! {
                    "key".into() => Value::from(vec!["foo", "baz"]),
                    "val".into() => Value::from(vec!["bar", "bat"])
                }))
            );
        }
        _ => unreachable!(),
    };
}
#[test]
fn test_re_extractor() {
    let ex = Extractor::new("re", "(snot)?foo(?P<snot>.*)").expect("bad extractor");
    match ex {
        Extractor::Re { .. } => {
            assert_eq!(
                ex.extract(true, &Value::from("foobar"), &EventContext::new(0, None)),
                Match(Value::from(hashmap! {
                "snot".into() => Value::from("bar") }))
            );
        }
        _ => unreachable!(),
    };
}
#[test]
fn test_kv_extractor() {
    let ex = Extractor::new("kv", "").expect("bad extractor");
    match ex {
        Extractor::Kv { .. } => {
            assert_eq!(
                ex.extract(true, &Value::from("a:b c:d"), &EventContext::new(0, None)),
                Match(Value::from(hashmap! {
                    "a".into() => "b".into(),
                   "c".into() => "d".into()
                }))
            );
        }
        _ => unreachable!(),
    };
}

#[test]
fn test_json_extractor() {
    let ex = Extractor::new("json", "").expect("bad extractor");
    match ex {
        Extractor::Json => {
            assert_eq!(
                ex.extract(
                    true,
                    &Value::from(r#"{"a":"b", "c":"d"}"#),
                    &EventContext::new(0, None)
                ),
                Match(Value::from(hashmap! {
                    "a".into() => "b".into(),
                    "c".into() => "d".into()
                }))
            );
        }
        _ => unreachable!(),
    };
}

#[test]
fn test_glob_extractor() {
    let ex = Extractor::new("glob", "*INFO*").expect("bad extractor");
    match ex {
        Extractor::Glob { .. } => {
            assert_eq!(
                ex.extract(true, &Value::from("INFO"), &EventContext::new(0, None)),
                MatchNull
            );
        }
        _ => unreachable!(),
    };
}

#[test]
fn test_base64_extractor() {
    let ex = Extractor::new("base64", "").expect("bad extractor");
    match ex {
        Extractor::Base64 => {
            assert_eq!(
                ex.extract(
                    true,
                    &Value::from("8J+agHNuZWFreSByb2NrZXQh"),
                    &EventContext::new(0, None)
                ),
                Match("\u{1f680}sneaky rocket!".into())
            );
        }
        _ => unreachable!(),
    };
}

#[test]
fn test_dissect_extractor() {
    let ex = Extractor::new("dissect", "%{name}").expect("bad extractor");
    match ex {
        Extractor::Dissect { .. } => {
            assert_eq!(
                ex.extract(true, &Value::from("John"), &EventContext::new(0, None)),
                Match(literal!({
                    "name": "John"
                }))
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_grok_extractor() {
    let pattern = r#"^<%%{POSINT:syslog_pri}>(?:(?<syslog_version>\d{1,3}) )?(?:%{SYSLOGTIMESTAMP:syslog_timestamp0}|%{TIMESTAMP_ISO8601:syslog_timestamp1}) %{SYSLOGHOST:syslog_hostname}  ?(?:%{TIMESTAMP_ISO8601:syslog_ingest_timestamp} )?(%{WORD:wf_pod} %{WORD:wf_datacenter} )?%{GREEDYDATA:syslog_message}"#;

    let ex = Extractor::new("grok", pattern).expect("bad extractor");
    match ex {
        Extractor::Grok { .. } => {
            let output = ex.extract(
                true,
                &Value::from(
                    "<%1>123 Jul   7 10:51:24 hostname 2019-04-01T09:59:19+0010 pod dc foo bar baz",
                ),
                &EventContext::new(0, None),
            );

            assert_eq!(
                output,
                Match(literal!({
                          "syslog_ingest_timestamp": "2019-04-01T09:59:19+0010",
                          "wf_datacenter": "dc",
                          "syslog_hostname": "hostname",
                          "syslog_pri": "1",
                          "wf_pod": "pod",
                          "syslog_message": "foo bar baz",
                          "syslog_version": "123",
                          "syslog_timestamp0": "Jul   7 10:51:24"
                }))
            );
        }

        _ => unreachable!(),
    }
}
#[test]
fn test_cidr_extractor() {
    let ex = Extractor::new("cidr", "").expect("");
    match ex {
        Extractor::Cidr { .. } => {
            assert_eq!(
                ex.extract(
                    true,
                    &Value::from("192.168.1.0"),
                    &EventContext::new(0, None)
                ),
                Match(Value::from(hashmap! (
                    "prefix".into() => Value::from(vec![Value::from(192), 168.into(), 1.into(), 0.into()]),
                    "mask".into() => Value::from(vec![Value::from(255), 255.into(), 255.into(), 255.into()])


                )))
            );
            assert_eq!(
                ex.extract(
                    true,
                    &Value::from("192.168.1.0/24"),
                    &EventContext::new(0, None)
                ),
                Match(Value::from(hashmap! (
                                    "prefix".into() => Value::from(vec![Value::from(192), 168.into(), 1.into(), 0.into()]),
                                    "mask".into() => Value::from(vec![Value::from(255), 255.into(), 255.into(), 0.into()])


                )))
            );

            assert_eq!(
                ex.extract(
                    true,
                    &Value::from("192.168.1.0"),
                    &EventContext::new(0, None)
                ),
                Match(Value::from(hashmap!(
                            "prefix".into() => Value::from(vec![Value::from(192), 168.into(), 1.into(), 0.into()]),
                            "mask".into() => Value::from(vec![Value::from(255), 255.into(), 255.into(), 255.into()])
                )))
            );

            assert_eq!(
                ex.extract(
                    true,
                    &Value::from("2001:4860:4860:0000:0000:0000:0000:8888"),
                    &EventContext::new(0, None)
                ),
                Match(Value::from(hashmap!(
                            "prefix".into() => Value::from(vec![Value::from(8193),  18528.into(), 18528.into(), 0.into(), 0.into(), 0.into(), 0.into(), 34952.into()]),
                            "mask".into() => Value::from(vec![Value::from(65535), 65535.into(), 65535.into(), 65535.into(), 65535.into(), 65535.into(), 65535.into(), 65535.into()])
                )))
            );
        }
        _ => unreachable!(),
    }

    let rex = Extractor::new("cidr", "10.22.0.0/24, 10.22.1.0/24").expect("bad rex");
    match rex {
        Extractor::Cidr { .. } => {
            assert_eq!(
                rex.extract(
                    true,
                    &Value::from("10.22.0.254"),
                    &EventContext::new(0, None)
                ),
                Match(Value::from(hashmap! (
                        "prefix".into() => Value::from(vec![Value::from(10), 22.into(), 0.into(), 254.into()]),
                        "mask".into() => Value::from(vec![Value::from(255), 255.into(), 255.into(), 255.into()]),
                )))
            );

            assert_eq!(
                rex.extract(
                    true,
                    &Value::from("99.98.97.96"),
                    &EventContext::new(0, None)
                ),
                NoMatch
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_influx_extractor() {
    let ex = Extractor::new("influx", "").expect("bad extractor");
    match ex {
        Extractor::Influx => assert_eq!(
            ex.extract(
                true,
                &Value::from("wea\\ ther,location=us-midwest temperature=82 1465839830100400200"),
                &EventContext::new(0, None)
            ),
            Match(Value::from(hashmap! (
                   "measurement".into() => "wea ther".into(),
                   "tags".into() => Value::from(hashmap!("location".into() => "us-midwest".into())),
                   "fields".into() => Value::from(hashmap!("temperature".into() => 82.0_f64.into())),
                   "timestamp".into() => Value::from(1_465_839_830_100_400_200_u64)
            )))
        ),
        _ => unreachable!(),
    }
}

#[test]
fn test_datetime_extractor() {
    let ex = Extractor::new("datetime", "%Y-%m-%d %H:%M:%S").expect("bad extractor");
    match ex {
        Extractor::Datetime { .. } => assert_eq!(
            ex.extract(
                true,
                &Value::from("2019-06-20 00:00:00"),
                &EventContext::new(0, None)
            ),
            Match(Value::from(1_560_988_800_000_000_000_u64))
        ),
        _ => unreachable!(),
    }
}

#[test]
fn opt_glob() -> StdResult<(), Error> {
    assert_eq!(
        Extractor::new("glob", "snot*")?,
        Extractor::Prefix("snot".into())
    );
    assert_eq!(
        Extractor::new("glob", "*badger")?,
        Extractor::Suffix("badger".into())
    );
    assert_matches!(
        Extractor::new("glob", "sont*badger")?,
        Extractor::Glob { .. }
    );
    Ok(())
}

#[test]
fn text_exclusive_glob() -> StdResult<(), Error> {
    let e = Extractor::new("glob", "snot*")?;
    assert!(!e.is_exclusive_to(&Value::from("snot")));
    assert!(!e.is_exclusive_to(&Value::from("snot badger")));
    assert!(e.is_exclusive_to(&Value::from("badger snot")));

    let e = Extractor::new("glob", "*badger")?;
    assert!(e.is_exclusive_to(&Value::from("snot")));
    assert!(e.is_exclusive_to(&Value::from("badger snot")));
    assert!(!e.is_exclusive_to(&Value::from("snot badger")));

    let e = Extractor::new("glob", "snot*badger")?;
    assert!(e.is_exclusive_to(&Value::from("snot")));
    assert!(e.is_exclusive_to(&Value::from("badger snot")));
    assert!(!e.is_exclusive_to(&Value::from("snot badger")));
    assert!(!e.is_exclusive_to(&Value::from("snot snot badger")));
    Ok(())
}

#[test]
fn text_exclusive_re() -> StdResult<(), Error> {
    let e = Extractor::new("re", "^snot.*badger$")?;
    assert!(e.is_exclusive_to(&Value::from("snot")));
    assert!(e.is_exclusive_to(&Value::from("badger snot")));
    assert!(!e.is_exclusive_to(&Value::from("snot badger")));
    assert!(!e.is_exclusive_to(&Value::from("snot snot badger")));

    let e = Extractor::new("rerg", "^snot.*badger$")?;
    assert!(e.is_exclusive_to(&Value::from("snot")));
    assert!(e.is_exclusive_to(&Value::from("badger snot")));
    assert!(!e.is_exclusive_to(&Value::from("snot badger")));
    assert!(!e.is_exclusive_to(&Value::from("snot snot badger")));
    Ok(())
}

#[test]
fn text_exclusive_base64() -> StdResult<(), Error> {
    let e = Extractor::new("base64", "")?;
    assert!(e.is_exclusive_to(&Value::from("sn!ot")));
    assert!(e.is_exclusive_to(&Value::from("badger snot")));
    assert!(!e.is_exclusive_to(&Value::from("abc=")));
    assert!(!e.is_exclusive_to(&Value::from("124=")));
    Ok(())
}

#[test]
fn text_exclusive_kv() -> StdResult<(), Error> {
    let e = Extractor::new("kv", "")?;
    assert!(e.is_exclusive_to(&Value::from("sn!ot")));
    assert!(e.is_exclusive_to(&Value::from("badger snot")));
    assert!(!e.is_exclusive_to(&Value::from("a:2")));
    assert!(!e.is_exclusive_to(&Value::from("ip:1.2.3.4 error:REFUSED")));
    Ok(())
}

#[test]
fn text_exclusive_json() -> StdResult<(), Error> {
    let e = Extractor::new("json", "")?;
    assert!(e.is_exclusive_to(&Value::from("\"sn!ot")));
    assert!(e.is_exclusive_to(&Value::from("{badger snot")));
    assert!(!e.is_exclusive_to(&Value::from("2")));
    assert!(!e.is_exclusive_to(&Value::from("[]")));
    Ok(())
}

#[test]
fn text_exclusive_dissect() -> StdResult<(), Error> {
    let e = Extractor::new("dissect", "snot%{name}")?;
    assert!(!e.is_exclusive_to(&Value::from("snot")));
    assert!(!e.is_exclusive_to(&Value::from("snot badger")));
    assert!(e.is_exclusive_to(&Value::from("badger snot")));
    Ok(())
}

#[test]
fn text_exclusive_grok() -> StdResult<(), Error> {
    let e = Extractor::new("grok", "%{NUMBER:duration}")?;
    assert!(e.is_exclusive_to(&Value::from("snot")));
    assert!(!e.is_exclusive_to(&Value::from("snot 123 badger")));
    assert!(!e.is_exclusive_to(&Value::from("123")));
    Ok(())
}

#[test]
fn text_exclusive_cidr() -> StdResult<(), Error> {
    let e = Extractor::new("cidr", "")?;
    assert!(!e.is_exclusive_to(&Value::from("1")));
    assert!(!e.is_exclusive_to(&Value::from("127.0.0.1")));
    assert!(!e.is_exclusive_to(&Value::from("snot")));
    assert!(!e.is_exclusive_to(&Value::from("123")));
    Ok(())
}
#[test]
fn text_exclusive_influx() -> StdResult<(), Error> {
    let e = Extractor::new("influx", "")?;
    assert!(!e.is_exclusive_to(&Value::from(
        "weather,location=us-midwest temperature=82 1465839830100400200"
    )));
    assert!(!e.is_exclusive_to(&Value::from("weather,location=us-midwest temperature=82")));
    assert!(e.is_exclusive_to(&Value::from("snot")));
    assert!(e.is_exclusive_to(&Value::from("123")));
    Ok(())
}

#[test]
fn text_exclusive_datetime() -> StdResult<(), Error> {
    let e = Extractor::new("datetime", "%Y-%m-%d %H:%M:%S")?;
    assert!(!e.is_exclusive_to(&Value::from("2019-06-20 00:00:00")));
    assert!(e.is_exclusive_to(&Value::from("snot")));
    assert!(e.is_exclusive_to(&Value::from("123")));
    assert!(e.is_exclusive_to(&Value::from("2019-06-20 00:00:71")));
    Ok(())
}
