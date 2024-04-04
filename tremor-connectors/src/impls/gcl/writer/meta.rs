// Copyright 2022, The Tremor Team
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

use crate::prelude::*;
use googapis::google::logging::{
    r#type::HttpRequest,
    v2::{LogEntryOperation, LogEntrySourceLocation},
};
use prost_types::Timestamp;

pub(crate) fn get_or_default(meta: Option<&Value>, key: &str) -> String {
    meta.get_str(key).unwrap_or_default().to_string()
}

pub(crate) fn insert_id(meta: Option<&Value>) -> String {
    get_or_default(meta, "insert_id")
}

#[allow(clippy::cast_possible_wrap, clippy::cast_precision_loss)]
pub(crate) fn timestamp(ingest_ns: u64, meta: Option<&Value>) -> Timestamp {
    let timestamp = if let Some(timestamp) = meta.get_u64("timestamp") {
        timestamp
    } else {
        ingest_ns
    };
    let mut timestamp = Timestamp {
        seconds: (timestamp / 1_000_000_000) as i64,
        nanos: (timestamp % 1_000_000_000) as i32,
    };
    timestamp.normalize();
    timestamp
}

pub(crate) fn http_request(meta: Option<&Value>) -> Option<HttpRequest> {
    // Override for a specific per event trace
    let meta = meta?;
    let http_request = meta.get("http_request")?;

    Some(HttpRequest {
        request_method: http_request
            .get("request_method")
            .as_str()
            .unwrap_or("")
            .to_string(),
        request_url: http_request
            .get("request_url")
            .as_str()
            .unwrap_or("")
            .to_string(),
        request_size: http_request.get("request_size").as_i64().unwrap_or(0),
        status: http_request.get("status").as_i32().unwrap_or(0),
        response_size: http_request.get("response_size").as_i64().unwrap_or(0),
        user_agent: http_request
            .get("user_agent")
            .as_str()
            .unwrap_or("")
            .to_string(),
        remote_ip: http_request
            .get("remote_ip")
            .as_str()
            .unwrap_or("")
            .to_string(),
        server_ip: http_request
            .get("server_ip")
            .as_str()
            .unwrap_or("")
            .to_string(),
        referer: http_request
            .get("referer")
            .as_str()
            .unwrap_or("")
            .to_string(),
        latency: match http_request.get("latency").as_u64().unwrap_or(0) {
            0 => None,
            otherwise => Some(std::time::Duration::from_nanos(otherwise).into()),
        },
        cache_lookup: http_request.get("cache_lookup").as_bool().unwrap_or(false),
        cache_hit: http_request.get("cache_hit").as_bool().unwrap_or(false),
        cache_validated_with_origin_server: http_request
            .get("cache_validated_with_origin_server")
            .as_bool()
            .unwrap_or(false),
        cache_fill_bytes: http_request.get("cache_fill_bytes").as_i64().unwrap_or(0),
        protocol: http_request
            .get("protocol")
            .as_str()
            .unwrap_or("")
            .to_string(),
    })
}

pub(crate) fn operation(meta: Option<&Value>) -> Option<LogEntryOperation> {
    let meta = meta?;
    // Override for a specific per event trace
    if let Some(operation @ Value::Object(_)) = meta.get("operation") {
        return Some(LogEntryOperation {
            id: operation.get_str("id").unwrap_or("").to_string(),
            producer: operation.get_str("producer").unwrap_or("").to_string(),
            first: operation.get_bool("first").unwrap_or(false),
            last: operation.get_bool("last").unwrap_or(false),
        });
    }

    // Otherwise, None as mapping is optional
    None
}

pub(crate) fn trace(meta: Option<&Value>) -> String {
    get_or_default(meta, "trace")
}

pub(crate) fn span_id(meta: Option<&Value>) -> String {
    get_or_default(meta, "span_id")
}

pub(crate) fn trace_sampled(meta: Option<&Value>) -> Result<bool, TryTypeError> {
    // Override for a specific per event severity
    if let Some(trace_sampled) = meta.get("trace_sampled") {
        return trace_sampled.try_as_bool();
    };

    Ok(false)
}

pub(crate) fn source_location(meta: Option<&Value>) -> Option<LogEntrySourceLocation> {
    let has_meta = meta?;
    // Override for a specific per event trace
    if let Some(loc @ Value::Object(_)) = has_meta.get("source_location") {
        return Some(LogEntrySourceLocation {
            file: loc.get("file").as_str().unwrap_or("").to_string(),
            line: loc.get("line").as_i64().unwrap_or(0),
            function: loc.get("function").as_str().unwrap_or("").to_string(),
        });
    }

    // Otherwise, None as mapping is optional
    None
}

#[cfg(test)]
mod test {
    use super::super::Config;
    use super::*;

    use crate::impls::gcl::writer::default_log_severity;
    use googapis::google::logging::r#type::LogSeverity;
    use std::collections::HashMap as StdHashMap;
    use tremor_config::Impl;

    use tremor_value::literal;
    use tremor_value::structurize;

    #[test]
    fn config_with_defaults_no_overrides() -> anyhow::Result<()> {
        let config: Config = Config::new(&literal!({
            "token": {"file": file!().to_string()},
        }))?;

        assert_eq!(None, config.log_name);
        assert_eq!(None, config.resource);
        assert!(!config.partial_success);
        assert!(!config.dry_run);
        assert_eq!(1_000_000_000, config.connect_timeout);
        assert_eq!(10_000_000_000, config.request_timeout);
        assert_eq!(LogSeverity::Default as i32, config.default_severity);
        assert_eq!(std::collections::HashMap::new(), config.labels);

        Ok(())
    }

    #[test]
    fn config_with_defaults_and_overrides() -> anyhow::Result<()> {
        let config: Config = Config::new(&literal!({
            "token": {"file": file!().to_string()},
        }))?;
        let meta = literal!({}); // no overrides
        assert_eq!("default".to_string(), config.log_name(Some(&meta)));
        assert_eq!(
            LogSeverity::Default as i32,
            config.log_severity(Some(&meta))?
        );
        assert_eq!(String::new(), insert_id(Some(&meta)));
        assert_eq!(None, http_request(Some(&meta)));
        assert_eq!(StdHashMap::new(), Config::labels(Some(&meta)));
        assert_eq!(None, operation(Some(&meta)));
        assert_eq!(String::new(), trace(Some(&meta)));
        assert_eq!(String::new(), span_id(Some(&meta)));
        assert!(!trace_sampled(Some(&meta))?);
        assert_eq!(None, source_location(Some(&meta)));

        Ok(())
    }

    #[test]
    fn default_log_name_test() -> anyhow::Result<()> {
        let empty_config: Config = Config::new(&literal!({
            "token": {"file": file!().to_string()},
        }))?;
        assert_eq!("default", &empty_config.log_name(None));

        let ok_config: Config = Config::new(&literal!({
            "token": {"file": file!().to_string()},
            "log_name": "snot",
        }))?;
        assert_eq!("snot", &ok_config.log_name(None));

        let ko_config: std::result::Result<Config, tremor_value::Error> =
            structurize(literal!({ "log_name": 42 }));
        assert!(ko_config.is_err());

        Ok(())
    }

    #[test]
    fn log_name_overrides() -> anyhow::Result<()> {
        let empty_config: Config = Config::new(&literal!({
            "token": {"file": file!().to_string()},
        }))?;
        let meta = literal!({
            "token": {"file": file!().to_string()},
            "log_name": "snot",
        });
        assert_eq!("snot".to_string(), empty_config.log_name(Some(&meta)));
        Ok(())
    }

    #[test]
    fn log_severity_overrides() -> anyhow::Result<()> {
        let mut empty_config: Config = Config::new(&literal!({
            "token": {"file": file!().to_string()},
        }))?;
        let meta = literal!({
            "log_severity": LogSeverity::Debug as i32,
        });
        assert_eq!(
            LogSeverity::Debug as i32,
            empty_config.log_severity(Some(&meta))?
        );

        let err_meta = literal!({
            "log_severity": ["snot"],
        });
        let result = empty_config.log_severity(Some(&err_meta));
        assert!(result.is_err());

        let no_meta = literal!({});
        empty_config.default_severity = default_log_severity();
        let result = empty_config.log_severity(Some(&no_meta));
        assert!(result.is_ok());

        Ok(())
    }

    #[test]
    fn insert_id_overrides() {
        let meta = literal!({
            "insert_id": "1234",
        });
        assert_eq!("1234".to_string(), insert_id(Some(&meta)));
    }

    #[test]
    fn http_request_overrides() {
        let mut ok_count = 0;

        let meta = literal!({
            "http_request": {
                "request_method": "GET",
                "request_url": "https://www.tremor.rs/",
                "request_size": 0,
                "status": 200,
                "response_size": 1024,
                "user_agent": "Tremor/v12",
                "remote_ip": "3.125.16.34",
                "server_ip": "127.0.0.1",
                "referer": "",
                "latency": 100_000_000u64,
                "cache_lookup": false,
                "cache_hit": false,
                "cache_validated_with_origin_server": false,
                "cache_fill_bytes": 0,
                "protocol": "websocket"
            }
        });
        if let Some(http_request) = http_request(Some(&meta)) {
            assert_eq!("GET", http_request.request_method);
            assert_eq!("https://www.tremor.rs/", http_request.request_url);
            assert_eq!(0, http_request.request_size);
            assert_eq!(200, http_request.status);
            assert_eq!(1024, http_request.response_size);
            assert_eq!("Tremor/v12", http_request.user_agent);
            assert_eq!("3.125.16.34", http_request.remote_ip);
            assert_eq!("127.0.0.1", http_request.server_ip);
            assert_eq!("", http_request.referer);
            //                assert_eq!(100_000_000u64, _http_request.latency.into());
            assert!(!http_request.cache_lookup);
            assert!(!http_request.cache_hit);
            assert!(!http_request.cache_validated_with_origin_server);
            assert_eq!(0, http_request.cache_fill_bytes);
            assert_eq!("websocket", http_request.protocol);
            ok_count += 1;
        };

        let meta = literal!({
            "http_request": {
                "request_method": "GET",
                "request_url": "https://www.tremor.rs/",
                "request_size": 0,
                "status": 200,
                "response_size": 1024,
                "user_agent": "Tremor/v12",
                "remote_ip": "3.125.16.34",
                "server_ip": "127.0.0.1",
                "referer": "",
                "cache_lookup": false,
                "cache_hit": false,
                "cache_validated_with_origin_server": false,
                "cache_fill_bytes": 0,
                "protocol": "websocket"
            }
        });
        if let Some(http_request) = http_request(Some(&meta)) {
            assert_eq!(None, http_request.latency);
            ok_count += 1;
        }

        assert_eq!(2, ok_count);
    }

    #[test]
    fn default_labels_test() -> anyhow::Result<()> {
        // NOTE labels are disjoin in GCL
        //      Common labels are sent once per batch of events
        //      Metadata override ( per event ) labels are per event
        //      So, although odd, this test is as intended
        assert_eq!(StdHashMap::new(), Config::labels(None));

        let ok_config = Config::new(&literal!({
            "token": {"file": file!().to_string()},
            "labels": { "snot": "badger" } }
        ))?;
        assert_eq!(1, ok_config.labels.len());

        let ko_config: std::result::Result<Config, _> = Config::new(&literal!({
            "token": {"file": file!().to_string()},
            "labels": "snot"
        }));
        assert!(ko_config.is_err());

        Ok(())
    }

    #[test]
    fn label_overrides() {
        let meta = literal!({
            "labels": {
                "badger": "snake"
            }
        });
        let labels = Config::labels(Some(&meta));
        let badger = labels.get("badger");
        assert_eq!(None, labels.get("snot"));
        assert_eq!(
            "snake".to_string(),
            badger.unwrap_or(&"fail".to_string()).to_string()
        );
    }

    #[test]
    fn operation_overrides() {
        let meta = literal!({
            "operation": {
                "id": "snot",
                "producer": "badger",
                "first": true,
                "last": true,
            },
        });
        assert_eq!(
            Some(LogEntryOperation {
                id: "snot".to_string(),
                producer: "badger".to_string(),
                first: true,
                last: true
            }),
            operation(Some(&meta))
        );
    }

    #[test]
    fn trace_overrides() {
        let meta = literal!({
            "trace": "snot"
        });
        let meta = trace(Some(&meta));
        assert_eq!("snot", meta);
    }

    #[test]
    fn span_id_overrides() {
        let meta = literal!({
            "span_id": "snot"
        });
        let meta = span_id(Some(&meta));
        assert_eq!("snot", meta);
    }

    #[test]
    fn trace_sampled_overrides() -> anyhow::Result<()> {
        let meta = literal!({
            "trace_sampled": true
        });
        let meta_trace_ok = trace_sampled(Some(&meta))?;
        assert!(meta_trace_ok);

        let meta = literal!({
            "trace_sampled": [ "snot" ]
        });
        let trace_err = trace_sampled(Some(&meta));
        assert!(trace_err.is_err());

        Ok(())
    }

    #[test]
    fn source_location_overrides() {
        let meta = literal!({
            "source_location": {
                "file": "snot",
                "line": 42,
                "function": "badger"
            }
        });
        let sl = source_location(Some(&meta));
        assert_eq!(
            Some(LogEntrySourceLocation {
                file: "snot".to_string(),
                line: 42i64,
                function: "badger".to_string()
            }),
            sl
        );
    }

    #[test]
    fn timestamp_overrides() {
        let meta = literal!({
            "timestamp": 42
        });
        let ts = timestamp(0, Some(&meta));

        let mut expected = Timestamp {
            seconds: 0,
            nanos: 42,
        };
        expected.normalize();

        assert_eq!(expected, ts);
    }
}
