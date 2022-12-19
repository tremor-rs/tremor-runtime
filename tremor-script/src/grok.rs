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

use crate::errors::Result;
use crate::Value;
use grok::Grok;
use std::path::Path;
use std::str;
use std::{
    io::{BufRead, BufReader},
    sync::Arc,
};
use tremor_common::file;
use value_trait::{Builder, Mutable};

const PATTERNS_FILE_TUPLE: &str = "%{NOTSPACE:alias} %{GREEDYDATA:pattern}";
pub(crate) const PATTERNS_FILE_DEFAULT_PATH: &str = "/etc/tremor/grok.patterns";

/// A GROK pattern
#[derive(Debug, Clone)]
pub struct Pattern {
    pub(crate) pattern: Arc<grok::Pattern>,
}

impl Pattern {
    /// Reads a pattern from a file
    ///
    /// # Errors
    /// * if the file can't be opned
    /// * a pattern can not be compiled
    /// * an io error happens
    /// * a non existing pattern is referenced
    pub fn from_file<S>(file_path: &S, definition: &str) -> Result<Self>
    where
        S: AsRef<Path> + ?Sized,
    {
        let file = file::open(file_path)?;
        let input: Box<dyn BufRead> = Box::new(BufReader::new(file));

        let mut grok = Grok::default();
        let recognizer = grok.compile(PATTERNS_FILE_TUPLE, true)?;

        let mut result = Grok::default();

        for (num, line) in input.lines().enumerate() {
            let l = line?;
            if l.is_empty() || l.starts_with('#') {
                continue;
            }

            if let Some(m) = recognizer.match_against(&l) {
                if let Some((alias, pattern)) = m
                    .get("alias")
                    .and_then(|alias| Some((alias, m.get("pattern")?)))
                {
                    result.add_pattern(alias.to_string(), pattern.to_string());
                } else {
                    return Err(format!("{}: {:?}", "Expected a non-NONE value", (num, &l)).into());
                }
            } else {
                return Err(format!("{}: {:?}", "Error in pattern on line", (num, &l)).into());
            }
        }

        Ok(Self {
            pattern: Arc::new(result.compile(definition, true)?),
        })
    }

    /// Creates a pattern from a string
    ///
    /// # Errors
    /// if the pattern can not be compiled
    pub fn new(definition: &str) -> Result<Self> {
        let mut grok = Grok::with_default_patterns();
        if let Ok(pattern) = grok.compile(definition, true) {
            Ok(Self {
                pattern: Arc::new(pattern),
            })
        } else {
            Err(format!("Failed to compile logstash grok pattern `{definition}`").into())
        }
    }

    /// Tests if a pattern matches
    ///
    /// TODO: make this return an Option<Value> so we can distinguish between failure
    ///       and non match
    /// # Errors
    /// if the data isn't valid utf8 or doesn't match
    pub fn matches(&self, data: &[u8]) -> Result<Value<'static>> {
        let text: String = str::from_utf8(data)?.to_string();
        match self.pattern.match_against(&text) {
            Some(m) => {
                let mut o = Value::object();
                for (a, b) in m.iter() {
                    o.try_insert(a.to_string(), b.to_string());
                }
                Ok(o)
            }
            None => Err(format!("No match for log text: {}", &text).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tremor_value::literal;

    fn assert_grok_ok<I1, I2, V>(pattern: &I1, raw: &I2, json: V) -> bool
    where
        I1: ToString + ?Sized,
        I2: ToString + ?Sized,
        V: core::fmt::Debug,
        tremor_value::Value<'static>: PartialEq<V>,
    {
        let pat: String = pattern.to_string();
        let raw: String = raw.to_string();
        let codec = Pattern::new(&pat).expect("bad pattern");
        let decoded = codec.matches(raw.as_bytes());
        decoded
            .map(|j| {
                assert_eq!(j, json);
                true
            })
            .unwrap_or_default()
    }

    #[test]
    fn load_file() -> Result<()> {
        let file = tempfile::NamedTempFile::new().expect("can't create temp file");
        {
            use std::io::Write;
            let mut f1 = file.reopen()?;
            writeln!(f1, "# the snots")?;
            writeln!(f1)?;
            writeln!(f1, "SNOT %{{USERNAME:snot}} %{{USERNAME:snot}}")?;
            writeln!(f1, "#the badgers")?;
            writeln!(f1, "SNOTBADGER %{{USERNAME:snot}} %{{USERNAME:badger}}")?;
        }
        Pattern::from_file(file.path(), "%{SNOT} %{SNOTBADGER}").map(|_| ())
    }

    #[test]
    fn no_match() {
        let codec = Pattern::new("{}").expect("bad pattern");
        let decoded = codec.matches(b"cookie monster");
        assert!(decoded.is_err());
    }

    #[test]
    fn decode_no_alias_does_not_map() {
        assert_grok_ok("%{USERNAME}", "foobar", literal!({}));
    }

    #[test]
    fn decode_clashing_alias_maps_with_default_unknowable_name() {
        assert_grok_ok(
            "%{USERNAME:snot} %{USERNAME:snot}",
            "foobar badger",
            literal!({"snot": "badger", "name0": "foobar"}),
        );
        assert_grok_ok(
            "%{USERNAME:snot} %{USERNAME:name0}",
            "foobar badger",
            literal!({"snot": "foobar", "name0": "badger"}),
        );
    }

    #[test]
    fn decode_simple_ok() {
        assert_grok_ok(
            "%{USERNAME:username}",
            "foobar",
            literal!({"username": "foobar"}),
        );
    }

    #[test]
    fn decode_syslog_esx_hypervisors() {
        let pattern = r#"^<%%{POSINT:syslog_pri}> %{TIMESTAMP_ISO8601:syslog_timestamp} (?<esx_uid>[-0-9a-f]+) %{SYSLOGHOST:syslog_hostname} (?:(?<syslog_program>[\x21-\x39\x3b-\x5a\x5c\x5e-\x7e]+)(?:\[%{POSINT:syslog_pid}\])?:?)? (?:%{TIMESTAMP_ISO8601:syslog_ingest_timestamp} )?(%{WORD:wf_pod} %{WORD:wf_datacenter} )?%{GREEDYDATA:syslog_message}"#;
        assert_grok_ok(pattern,"<%1> 2019-04-01T09:59:19+0000 deadbeaf01234 example program_name[1234] 2019-04-01T09:59:19+0010 pod dc foo bar baz", literal!({
           "syslog_program": "program_name",
           "esx_uid": "deadbeaf01234",
           "wf_datacenter": "dc",
           "syslog_timestamp": "2019-04-01T09:59:19+0000",
           "wf_pod": "pod",
           "syslog_message": "foo bar baz",
           "syslog_hostname": "example",
           "syslog_pid": "1234",
           "syslog_ingest_timestamp": "2019-04-01T09:59:19+0010",
           "syslog_pri": "1"
        }));
    }

    #[test]
    fn decode_syslog_artifactory() {
        let pattern = r#"^<%%{POSINT:syslog_pri}>(?:(?<syslog_version>\d{1,3}) )?(?:%{SYSLOGTIMESTAMP:syslog_timestamp1}|%{TIMESTAMP_ISO8601:syslog_timestamp}) %{SYSLOGHOST:syslog_hostname} (?:(?<syslog_program>[\x21-\x39\x3b-\x5a\x5c\x5e-\x7e]+)(?:\[%{POSINT:syslog_pid}\])?:?)? (?:%{TIMESTAMP_ISO8601:syslog_ingest_timestamp} )?(%{WORD:wf_pod} %{WORD:wf_datacenter} )?%{GREEDYDATA:syslog_message}"#;
        assert_grok_ok(pattern, "<%1>123 Jul   7 10:51:24 hostname program_name[1234] 2019-04-01T09:59:19+0010 pod dc foo bar baz", literal!({
           "wf_pod": "pod",
           "syslog_timestamp1": "Jul   7 10:51:24",
           "syslog_message": "foo bar baz",
           "syslog_pri": "1",
           "syslog_hostname": "hostname",
           "wf_datacenter": "dc",
           "syslog_program": "program_name",
           "syslog_ingest_timestamp": "2019-04-01T09:59:19+0010",
           "syslog_pid": "1234",
           "syslog_version": "123"
        }));
    }

    #[test]
    fn decode_standard_syslog() {
        let pattern = r#"^<%%{POSINT:syslog_pri}>(?:(?<syslog_version>\d{1,3}) )?(?:%{SYSLOGTIMESTAMP:syslog_timestamp0}|%{TIMESTAMP_ISO8601:syslog_timestamp1}) %{SYSLOGHOST:syslog_hostname}  ?(?:%{TIMESTAMP_ISO8601:syslog_ingest_timestamp} )?(%{WORD:wf_pod} %{WORD:wf_datacenter} )?%{GREEDYDATA:syslog_message}"#;

        assert_grok_ok(
            pattern,
            "<%1>123 Jul   7 10:51:24 hostname 2019-04-01T09:59:19+0010 pod dc foo bar baz",
            literal!({
               "syslog_ingest_timestamp": "2019-04-01T09:59:19+0010",
               "wf_datacenter": "dc",
               "syslog_hostname": "hostname",
               "syslog_pri": "1",
               "wf_pod": "pod",
               "syslog_message": "foo bar baz",
               "syslog_version": "123",
               "syslog_timestamp0": "Jul   7 10:51:24"
            }),
        );
    }

    #[test]
    fn decode_nonstd_syslog() {
        let pattern = r#"^<%%{POSINT:syslog_pri}>(?:(?<syslog_version>\d{1,3}) )?(?:%{SYSLOGTIMESTAMP:syslog_timestamp}|%{TIMESTAMP_ISO8601:syslog_timestamp})  ?(?:%{TIMESTAMP_ISO8601:syslog_ingest_timestamp} )?(%{WORD:wf_pod} %{WORD:wf_datacenter} )?%{GREEDYDATA:syslog_message}"#;

        assert_grok_ok(
            pattern,
            "<%1>123 Jul   7 10:51:24  2019-04-01T09:59:19+0010 pod dc foo bar baz",
            literal!({
               "syslog_version": "123",
               "syslog_message": "foo bar baz",
               "syslog_ingest_timestamp": "2019-04-01T09:59:19+0010",
               "syslog_pri": "1",
               "wf_pod": "pod",
               "wf_datacenter": "dc",
               "name1": "Jul   7 10:51:24"
            }),
        );
    }

    #[test]
    fn decode_syslog_noversion() {
        let pattern = r#"^<%%{POSINT:syslog_pri}>(?:(?<syslog_version>\d{1,3}))? ?(?:%{TIMESTAMP_ISO8601:syslog_ingest_timestamp} )?(%{WORD:wf_pod} %{WORD:wf_datacenter} )?%{GREEDYDATA:syslog_message}"#;

        assert_grok_ok(
            pattern,
            "<%1>123 2019-04-01T09:59:19+0010 pod dc foo bar baz",
            literal!({
               "wf_pod": "pod",
               "syslog_pri": "1",
               "wf_datacenter": "dc",
               "syslog_version": "123",
               "syslog_message": "foo bar baz",
               "syslog_ingest_timestamp": "2019-04-01T09:59:19+0010"
            }),
        );
        assert_grok_ok(
            pattern,
            "<%1>12 2019-04-01T09:59:19+0010 pod dc foo bar baz",
            literal!({
               "wf_pod": "pod",
               "syslog_pri": "1",
               "wf_datacenter": "dc",
               "syslog_version": "12",
               "syslog_message": "foo bar baz",
               "syslog_ingest_timestamp": "2019-04-01T09:59:19+0010"
            }),
        );
        assert_grok_ok(
            pattern,
            "<%1>1 2019-04-01T09:59:19+0010 pod dc foo bar baz",
            literal!({
               "wf_pod": "pod",
               "syslog_pri": "1",
               "wf_datacenter": "dc",
               "syslog_version": "1",
               "syslog_message": "foo bar baz",
               "syslog_ingest_timestamp": "2019-04-01T09:59:19+0010"
            }),
        );
        assert_grok_ok(
            pattern,
            "<%1> 2019-04-01T09:59:19+0010 pod dc foo bar baz",
            literal!({
               "wf_pod": "pod",
               "syslog_pri": "1",
               "wf_datacenter": "dc",
               "syslog_message": "foo bar baz",
               "syslog_ingest_timestamp": "2019-04-01T09:59:19+0010"
            }),
        );
    }

    #[allow(clippy::too_many_lines)]
    #[test]
    fn decode_syslog_isilon() {
        let pattern = r#"^<invld>%{GREEDYDATA:syslog_error_prefix}>(%{TIMESTAMP_ISO8601:syslog_timestamp}(?: %{TIMESTAMP_ISO8601:syslog_ingest_timestamp})? )?(%{WORD:wf_pod} %{WORD:wf_datacenter} )?%{SYSLOGHOST:syslog_hostname} ?(%{SYSLOGPROG:syslog_program}:)?%{GREEDYDATA:syslog_message}"#;
        assert_grok_ok(
            pattern,
            "<invld>x>hostname.com bar baz",
            literal!({
               "syslog_message": "bar baz",
               "syslog_hostname": "hostname.com",
               "syslog_error_prefix": "x"
            }),
        );

        assert_grok_ok(
            pattern,
            "<invld>x y z>2019-04-01T09:59:19+0010 2019-04-01T09:59:19+0010 hostname.com bar baz",
            literal!({
               "syslog_timestamp": "2019-04-01T09:59:19+0010",
               "syslog_ingest_timestamp": "2019-04-01T09:59:19+0010",
               "syslog_hostname": "hostname.com",
               "syslog_message": "bar baz",
               "syslog_error_prefix": "x y z",
            }),
        );

        assert_grok_ok(
            pattern,
            "<invld>x y z>2019-04-01T09:59:19+0010 hostname.com bar baz",
            literal!({
               "syslog_timestamp": "2019-04-01T09:59:19+0010",
               "syslog_hostname": "hostname.com",
               "syslog_message": "bar baz",
               "syslog_error_prefix": "x y z",
            }),
        );

        assert_grok_ok(
            pattern,
            "<invld>x y z>2019-04-01T09:59:19+0010 pod dc hostname.com bar baz",
            literal!({
               "syslog_timestamp": "2019-04-01T09:59:19+0010",
               "wf_datacenter": "dc",
               "wf_pod": "pod",
               "syslog_hostname": "hostname.com",
               "syslog_message": "bar baz",
               "syslog_error_prefix": "x y z",
            }),
        );

        assert_grok_ok(
            pattern,
            "<invld>x y z>pod dc hostname.com bar baz",
            literal!({
               "wf_datacenter": "dc",
               "wf_pod": "pod",
               "syslog_hostname": "hostname.com",
               "syslog_message": "bar baz",
               "syslog_error_prefix": "x y z",
            }),
        );

        // prog / pid
        assert_grok_ok(
            pattern,
            "<invld>x>hostname.com program_name[1234]: bar baz",
            literal!({
               "pid": "1234",
               "syslog_program": "program_name[1234]",
               "syslog_message": " bar baz",
               "syslog_hostname": "hostname.com",
               "syslog_error_prefix": "x",
               "program": "program_name"
            }),
        );

        assert_grok_ok(pattern, "<invld>x y z>2019-04-01T09:59:19+0010 2019-04-01T09:59:19+0010 hostname.com program_name: bar baz", literal!({
           "syslog_timestamp": "2019-04-01T09:59:19+0010",
           "syslog_ingest_timestamp": "2019-04-01T09:59:19+0010",
           "syslog_hostname": "hostname.com",
           "syslog_program": "program_name",
           "syslog_message": " bar baz",
           "syslog_error_prefix": "x y z",
           "program": "program_name"
        }));

        assert_grok_ok(
            pattern,
            "<invld>x y z>2019-04-01T09:59:19+0010 hostname.com program_name[1234]: bar baz",
            literal!({
               "syslog_timestamp": "2019-04-01T09:59:19+0010",
               "syslog_hostname": "hostname.com",
               "syslog_program": "program_name[1234]",
               "syslog_message": " bar baz",
               "pid": "1234",
               "syslog_error_prefix": "x y z",
               "program": "program_name"
            }),
        );

        assert_grok_ok(
            pattern,
            "<invld>x y z>2019-04-01T09:59:19+0010 pod dc hostname.com program_name: bar baz",
            literal!({
               "syslog_timestamp": "2019-04-01T09:59:19+0010",
               "wf_datacenter": "dc",
               "wf_pod": "pod",
               "syslog_hostname": "hostname.com",
               "syslog_program": "program_name",
               "syslog_message": " bar baz",
               "syslog_error_prefix": "x y z",
               "program": "program_name"
            }),
        );

        assert_grok_ok(
            pattern,
            "<invld>x y z>pod dc hostname.com program_name[1234]:bar baz",
            literal!({
               "wf_datacenter": "dc",
               "wf_pod": "pod",
               "syslog_hostname": "hostname.com",
               "syslog_program": "program_name[1234]",
               "syslog_message": "bar baz",
               "pid": "1234",
               "syslog_error_prefix": "x y z",
               "program": "program_name"
            }),
        );
    }
}
