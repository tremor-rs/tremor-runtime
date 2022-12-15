// Copyright 2020-2022, The Tremor Team
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

#![allow(dead_code)]

use super::{
    common::{self},
    id,
    resource::{self, resource_to_pb},
};
use crate::connectors::{
    impls::otel::common::{key_value_list_to_json, maybe_instrumentation_scope_to_json},
    utils::pb,
};
use crate::errors::Result;
use simd_json::ValueAccess;

use tremor_otelapis::opentelemetry::proto::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::InstrumentationScope,
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use tremor_value::{literal, prelude::*, Value};

fn affirm_traceflags_valid(traceflags: u32) -> Result<u32> {
    if (traceflags == 128) || (traceflags == 0) {
        Ok(traceflags)
    } else {
        Err(format!(
            "The `traceflags` is invalid, expected: 0b10000000, actual: {}",
            traceflags
        )
        .into())
    }
}

fn affirm_severity_number_valid(severity_number: i32) -> Result<i32> {
    if (0..=24).contains(&severity_number) {
        // NOTE `0` implies unspecified severity
        Ok(severity_number)
    } else {
        Err(format!(
            "The `severity_number` is NOT in the valid range 0 <= {} <= 24",
            severity_number
        )
        .into())
    }
}

fn scope_log_to_json(log: ScopeLogs) -> Result<Value<'static>> {
    let mut scope_json = literal!({
        "log_records": log_records_to_json(log.log_records),
        "schema_url": log.schema_url.clone()
    });
    if let Some(scope) = maybe_instrumentation_scope_to_json(log.scope) {
        scope_json.insert("scope", scope.clone())?;
    }

    Ok(scope_json)
}

pub(crate) fn scope_logs_to_json(log: Vec<ScopeLogs>) -> Result<Vec<Value<'static>>> {
    log.into_iter().map(scope_log_to_json).collect()
}

pub(crate) fn log_record_to_json(pb: LogRecord) -> Value<'static> {
    literal!({
        "observed_time_unix_nano": pb.observed_time_unix_nano,
        "time_unix_nano": pb.time_unix_nano,
        "severity_number": pb.severity_number,
        "severity_text": pb.severity_text,
        "body": common::maybe_any_value_to_json(pb.body),
        "flags": pb.flags,
        "span_id": id::hex_span_id_to_json(&pb.span_id),
        "trace_id": id::hex_trace_id_to_json(&pb.trace_id),
        "attributes": key_value_list_to_json(pb.attributes),
        "dropped_attributes_count": pb.dropped_attributes_count,
    })
}

pub(crate) fn log_records_to_json(log: Vec<LogRecord>) -> Vec<Value<'static>> {
    log.into_iter().map(log_record_to_json).collect()
}

pub(crate) fn log_record_to_pb(log: &Value<'_>) -> Result<LogRecord> {
    Ok(LogRecord {
        observed_time_unix_nano: pb::maybe_int_to_pbu64(log.get("observed_time_unix_nano"))
            .unwrap_or_default(),
        // value of 0 indicates unknown or missing timestamp
        time_unix_nano: pb::maybe_int_to_pbu64(log.get("time_unix_nano")).unwrap_or_default(),

        // severity value is optional - default to 0 if not specified
        severity_number: pb::maybe_int_to_pbi32(log.get("severity_number"))
            .ok()
            .and_then(|sn| affirm_severity_number_valid(sn).ok())
            .unwrap_or_default(),
        // defined as optional - fallback to an empty string
        severity_text: pb::maybe_string_to_pb(log.get("severity_text")).unwrap_or_default(),
        body: log.get("body").map(common::any_value_to_pb),
        flags: affirm_traceflags_valid(
            pb::maybe_int_to_pbu32(log.get("flags")).unwrap_or_default(),
        )?,
        // span_id and trace_id are optional - fallback to empty bytes
        span_id: id::hex_span_id_to_pb(log.get("span_id")).unwrap_or_default(),
        trace_id: id::hex_trace_id_to_pb(log.get("trace_id")).unwrap_or_default(),
        dropped_attributes_count: pb::maybe_int_to_pbu32(log.get("dropped_attributes_count"))
            .unwrap_or_default(),
        attributes: common::maybe_key_value_list_to_pb(log.get("attributes")).unwrap_or_default(),
    })
}

pub(crate) fn resource_logs_to_json(request: ExportLogsServiceRequest) -> Result<Value<'static>> {
    let logs = request
        .resource_logs
        .into_iter()
        .map(|log| {
            let mut base = literal!({ "schema_url": log.schema_url});
            if let Some(r) = log.resource {
                base.try_insert("resource", resource::resource_to_json(r));
            };
            base.try_insert("scope_logs", scope_logs_to_json(log.scope_logs)?);
            Ok(base)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(literal!({ "logs": logs }))
}

pub(crate) fn scope_to_pb(json: &Value<'_>) -> Result<InstrumentationScope> {
    let _json = json
        .as_object()
        .ok_or("Invalid json mapping for InstrumentationScope")?;
    Ok(InstrumentationScope {
        name: pb::maybe_string_to_pb(json.get("name")).unwrap_or_default(),
        version: pb::maybe_string_to_pb(json.get("version")).unwrap_or_default(),
        attributes: Vec::new(),
        dropped_attributes_count: 0u32,
    })
}

pub(crate) fn log_records_to_pb(json: Option<&Value<'_>>) -> Result<Vec<LogRecord>> {
    if let Some(json) = json {
        json.as_array()
            .ok_or("Invalid json mapping for [LogRecord, ...]")?
            .iter()
            .map(log_record_to_pb)
            .collect()
    } else {
        Ok(vec![])
    }
}

pub(crate) fn scope_logs_to_pb(json: Option<&Value<'_>>) -> Result<Vec<ScopeLogs>> {
    if let Some(json) = json {
        json.as_array()
            .ok_or("Invalid json mapping for ScopeLogs")?
            .iter()
            .filter_map(Value::as_object)
            .map(|item| {
                Ok(ScopeLogs {
                    scope: item.get("scope").map(scope_to_pb).transpose()?,
                    log_records: log_records_to_pb(item.get("log_records"))?,
                    schema_url: item
                        .get("schema_url")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                })
            })
            .collect()
    } else {
        Ok(vec![])
    }
}

pub(crate) fn resources_to_pb(json: Option<&Value<'_>>) -> Result<Vec<Resource>> {
    if let Some(json) = json {
        json.as_array()
            .ok_or("Invalid json mapping for [Resource,...]")?
            .iter()
            .map(resource_to_pb)
            .collect()
    } else {
        Ok(vec![])
    }
}

pub(crate) fn resource_logs_to_pb(json: &Value<'_>) -> Result<Vec<ResourceLogs>> {
    json.get_array("logs")
        .ok_or("Missing `logs` array")?
        .iter()
        .map(|data| {
            Ok(ResourceLogs {
                schema_url: data
                    .get("schema_url")
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                resource: match data.get("resource") {
                    Some(resource) => Some(resource_to_pb(resource)?),
                    None => None,
                },
                scope_logs: scope_logs_to_pb(data.get("scope_logs"))?,
            })
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::unnecessary_wraps)] // Don't error highlight this in tests - we do not care
mod tests {
    use tremor_otelapis::opentelemetry::proto::{
        common::v1::{any_value, AnyValue},
        resource::v1::Resource,
    };
    use tremor_script::utils::sorted_serialize;

    use super::*;

    #[test]
    fn resource_logs() -> Result<()> {
        let nanos = tremor_common::time::nanotime();
        let span_id_pb = id::random_span_id_bytes(nanos);
        let span_id_json = id::test::pb_span_id_to_json(&span_id_pb);
        let trace_id_json = id::random_trace_id_value(nanos);
        let trace_id_pb = id::test::json_trace_id_to_pb(Some(&trace_id_json))?;

        let pb = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                schema_url: "schema_url".into(),
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 8,
                }),
                scope_logs: vec![ScopeLogs {
                    schema_url: "schema_url".into(),
                    scope: Some(InstrumentationScope {
                        name: "snot".to_string(),
                        version: "v1.2.3.4".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    log_records: vec![LogRecord {
                        observed_time_unix_nano: 0,
                        time_unix_nano: 0,
                        severity_number: 9,
                        severity_text: "INFO".into(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("snot".into())),
                        }), // TODO For now its an error for this to be None - may need to revisit
                        attributes: vec![],
                        dropped_attributes_count: 100,
                        flags: 128,
                        span_id: span_id_pb.clone(),
                        trace_id: trace_id_pb,
                    }],
                }],
            }],
        };
        let json = resource_logs_to_json(pb.clone())?;
        let back_again = resource_logs_to_pb(&json)?;
        let expected: Value = literal!({
            "logs": [{
                "resource": {
                    "attributes": {},
                    "dropped_attributes_count": 8
                },
                "schema_url": "schema_url",
                "scope_logs":[{
                    "log_records": [{
                        "attributes": {},
                        "body": "snot",
                        "dropped_attributes_count": 100,
                        "flags": 128,
                        "observed_time_unix_nano": 0,
                        "severity_number": 9,
                        "severity_text": "INFO",
                        "span_id": span_id_json,
                        "time_unix_nano": 0,
                        "trace_id": trace_id_json,
                    }],
                    "schema_url": "schema_url",
                    "scope": {
                        "attributes": {},
                        "dropped_attributes_count": 0,
                        "name": "snot",
                        "version": "v1.2.3.4"
                    }
                }]
            }]
        });

        assert_eq!(sorted_serialize(&expected)?, sorted_serialize(&json)?);
        assert_eq!(pb.resource_logs, back_again);

        Ok(())
    }

    #[test]
    fn resource_logs_severity_unspecified_regression() -> Result<()> {
        let nanos = tremor_common::time::nanotime();
        let span_id_pb = id::random_span_id_bytes(nanos);
        let span_id_json = id::test::pb_span_id_to_json(&span_id_pb);
        let trace_id_json = id::random_trace_id_value(nanos);
        let trace_id_pb = id::test::json_trace_id_to_pb(Some(&trace_id_json))?;

        let pb = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                schema_url: "schema_url".into(),
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 8,
                }),
                scope_logs: vec![ScopeLogs {
                    schema_url: "schema_url2".to_string(),
                    scope: Some(InstrumentationScope {
                        name: "name".to_string(),
                        version: "v1.2.3.4".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    log_records: vec![LogRecord {
                        time_unix_nano: 0,
                        observed_time_unix_nano: 0,
                        flags: 0,
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("snot".into())),
                        }),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        severity_number: 0,
                        severity_text: "not scarey".to_string(),
                        span_id: span_id_pb.clone(),
                        trace_id: trace_id_pb.clone(),
                    }],
                }],
            }],
        };
        let json = resource_logs_to_json(pb.clone())?;
        let back_again = resource_logs_to_pb(&json)?;
        let expected: Value = literal!({
            "logs": [{
                "resource": {
                      "attributes": {},
                      "dropped_attributes_count": 8
                    },
                "schema_url": "schema_url",
                "scope_logs": [{
                    "log_records": [{
                        "attributes": {},
                        "body": "snot",
                        "dropped_attributes_count": 0,
                        "flags": 0,
                        "observed_time_unix_nano": 0,
                        "severity_number": 0,
                        "severity_text": "not scarey",
                        "span_id": span_id_json,
                        "time_unix_nano": 0,
                        "trace_id": trace_id_json,
                    }],
                    "scope":{
                        "attributes": {},
                        "dropped_attributes_count": 0,
                        "name": "name",
                        "version": "v1.2.3.4"
                    },
                    "schema_url": "schema_url2",
                }]
            }]
        });

        assert_eq!(sorted_serialize(&expected)?, sorted_serialize(&json)?);
        assert_eq!(pb.resource_logs, back_again);

        Ok(())
    }

    #[test]
    fn minimal_logs() {
        let log = literal!({"logs": [
                {
                    "scope_logs": [],
                    "schema_url": ""
                }
            ]
        });
        assert_eq!(
            Ok(vec![ResourceLogs {
                resource: None,
                schema_url: String::from(""),
                scope_logs: vec![],
            }]),
            resource_logs_to_pb(&log)
        );
    }

    #[test]
    fn minimal_log_record() -> Result<()> {
        let lr = literal!({});
        assert_eq!(
            Ok(LogRecord {
                observed_time_unix_nano: 0,
                time_unix_nano: 0,
                severity_number: 0,
                severity_text: String::new(),
                body: None,
                attributes: vec![],
                dropped_attributes_count: 0,
                flags: 0,
                trace_id: vec![],
                span_id: vec![]
            }),
            log_record_to_pb(&lr)
        );
    }
}
