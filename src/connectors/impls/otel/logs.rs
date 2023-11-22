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
    common::{self, instrumentation_library_to_pb, maybe_instrumentation_library_to_json, EMPTY},
    id,
    resource::{self, resource_to_pb},
};
use crate::connectors::utils::pb;
use crate::errors::Result;

use anyhow::Context;
use tremor_otelapis::opentelemetry::proto::{
    collector::logs::v1::ExportLogsServiceRequest,
    logs::v1::{InstrumentationLibraryLogs, LogRecord, ResourceLogs},
};
use tremor_value::{literal, prelude::*, Value};

#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum Error {
    #[error("The `traceflags` is invalid, expected: 0b10000000, actual: {0}")]
    InvalidTraceFlags(u32),
    #[error("The `severity_number` is NOT in the valid range 0 <= {0} <= 24")]
    InvalidSeverityNumber(i32),
    #[error("Missing `logs` array")]
    MissingLogs,
}

fn affirm_traceflags_valid(traceflags: u32) -> Result<u32> {
    if (traceflags == 128) || (traceflags == 0) {
        Ok(traceflags)
    } else {
        Err(Error::InvalidTraceFlags(traceflags).into())
    }
}

fn affirm_severity_number_valid(severity_number: i32) -> Result<i32> {
    if (0..=24).contains(&severity_number) {
        // NOTE `0` implies unspecified severity
        Ok(severity_number)
    } else {
        Err(Error::InvalidSeverityNumber(severity_number).into())
    }
}

fn log_record_to_json(log: LogRecord) -> Result<Value<'static>> {
    Ok(literal!({
        "name": log.name,
        "time_unix_nano": log.time_unix_nano,
        "severity_number": affirm_severity_number_valid(log.severity_number)?,
        "severity_text": log.severity_text.to_string(),
        "flags": affirm_traceflags_valid(log.flags)?,
        "span_id": id::hex_id_to_json(&log.span_id),
        "trace_id": id::hex_id_to_json(&log.trace_id),
        "attributes": common::key_value_list_to_json(log.attributes),
        "dropped_attributes_count": log.dropped_attributes_count,
        "body": common::maybe_any_value_to_json(log.body),
    }))
}
pub(crate) fn instrumentation_library_logs_to_json(
    pb: Vec<InstrumentationLibraryLogs>,
) -> Result<Value<'static>> {
    pb.into_iter()
        .map(|data| {
            let logs = data
                .logs
                .into_iter()
                .map(log_record_to_json)
                .collect::<Result<Value>>()?;

            let mut e = literal!({ "logs": logs, "schema_url": data.schema_url });
            if let Some(il) = data.instrumentation_library {
                let il = maybe_instrumentation_library_to_json(il);
                e.try_insert("instrumentation_library", il);
            }
            Ok(e)
        })
        .collect()
}

pub(crate) fn log_record_to_pb(log: &Value<'_>) -> Result<LogRecord> {
    Ok(LogRecord {
        // value of 0 indicates unknown or missing timestamp
        time_unix_nano: pb::maybe_int_to_pbu64(log.get("time_unix_nano")).unwrap_or_default(),

        // severity value is optional - default to 0 if not specified
        severity_number: pb::maybe_int_to_pbi32(log.get("severity_number"))
            .ok()
            .map(affirm_severity_number_valid)
            .transpose()?
            .unwrap_or_default(),

        // defined as optional - fallback to an empty string
        severity_text: pb::maybe_string_to_pb(log.get("severity_text")).unwrap_or_default(),
        // name is defined as optional - fallback to empty string
        name: pb::maybe_string_to_pb(log.get("name")).unwrap_or_default(),
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

pub(crate) fn maybe_instrumentation_library_logs_to_pb(
    data: Option<&Value<'_>>,
) -> Result<Vec<InstrumentationLibraryLogs>> {
    data.try_as_array()
        .context("Invalid json mapping for InstrumentationLibraryLogs")?
        .iter()
        .filter_map(Value::as_object)
        .map(|ill| {
            let logs = ill
                .get("logs")
                .and_then(Value::as_array)
                .unwrap_or(&EMPTY)
                .iter()
                .map(log_record_to_pb)
                .collect::<Result<Vec<_>>>()?;

            Ok(InstrumentationLibraryLogs {
                schema_url: ill
                    .get("schema_url")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                instrumentation_library: ill
                    .get("instrumentation_library")
                    .map(instrumentation_library_to_pb)
                    .transpose()?,
                logs,
            })
        })
        .collect()
}

pub(crate) fn resource_logs_to_json(request: ExportLogsServiceRequest) -> Result<Value<'static>> {
    let logs = request
        .resource_logs
        .into_iter()
        .map(|log| {
            let ill = instrumentation_library_logs_to_json(log.instrumentation_library_logs)?;

            let mut base =
                literal!({ "instrumentation_library_logs": ill, "schema_url": log.schema_url});
            if let Some(r) = log.resource {
                base.try_insert("resource", resource::resource_to_json(r));
            };
            Ok(base)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(literal!({ "logs": logs }))
}

pub(crate) fn resource_logs_to_pb(json: &Value<'_>) -> Result<Vec<ResourceLogs>> {
    json.get_array("logs")
        .ok_or(Error::MissingLogs)?
        .iter()
        .filter_map(Value::as_object)
        .map(|data| {
            Ok(ResourceLogs {
                schema_url: data
                    .get("schema_url")
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                instrumentation_library_logs: maybe_instrumentation_library_logs_to_pb(
                    data.get("instrumentation_library_logs"),
                )?,
                resource: data.get("resource").map(resource_to_pb).transpose()?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use tremor_otelapis::opentelemetry::proto::{
        common::v1::{any_value, AnyValue, InstrumentationLibrary},
        resource::v1::Resource,
    };
    use tremor_value::utils::sorted_serialize;

    use super::*;

    #[test]
    fn instrumentation_library_logs() -> Result<()> {
        let nanos = tremor_common::time::nanotime();
        let span_id_pb = id::random_span_id_bytes(nanos);
        let span_id_json = id::hex_id_to_json(&span_id_pb);
        let trace_id_json = id::random_trace_id_value(nanos);
        let trace_id_pb = id::hex_trace_id_to_pb(Some(&trace_id_json))?;

        let pb = vec![InstrumentationLibraryLogs {
            schema_url: "schema_url".into(),
            instrumentation_library: Some(InstrumentationLibrary {
                name: "name".into(),
                version: "v0.1.2".into(),
            }), // TODO For now its an error for this to be None - may need to revisit
            logs: vec![LogRecord {
                time_unix_nano: 0,
                severity_number: 9,
                severity_text: "INFO".into(),
                name: "test".into(),
                body: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("snot".into())),
                }), // TODO For now its an error for this to be None - may need to revisit
                attributes: vec![],
                dropped_attributes_count: 100,
                flags: 128,
                span_id: span_id_pb.clone(),
                trace_id: trace_id_pb,
            }],
        }];
        let json = instrumentation_library_logs_to_json(pb.clone())?;
        let back_again = maybe_instrumentation_library_logs_to_pb(Some(&json))?;
        let expected: Value = literal!([{
            "instrumentation_library": { "name": "name", "version": "v0.1.2" },
            "schema_url": "schema_url",
            "logs": [
                { "severity_number": 9,
                  "flags": 128,
                  "span_id": span_id_json,
                  "trace_id": trace_id_json,
                  "dropped_attributes_count": 100,
                  "time_unix_nano": 0,
                  "severity_text": "INFO",
                  "name": "test",
                  "attributes": {},
                  "body": "snot"
                }
            ]
        }]);

        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        Ok(())
    }

    #[test]
    fn resource_logs() -> Result<()> {
        let nanos = tremor_common::time::nanotime();
        let span_id_pb = id::random_span_id_bytes(nanos);
        let span_id_json = id::hex_id_to_json(&span_id_pb);
        let trace_id_json = id::random_trace_id_value(nanos);
        let trace_id_pb = id::hex_trace_id_to_pb(Some(&trace_id_json))?;

        let pb = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                schema_url: "schema_url".into(),
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 8,
                }),
                instrumentation_library_logs: vec![InstrumentationLibraryLogs {
                    schema_url: "schema_url".into(),
                    instrumentation_library: Some(InstrumentationLibrary {
                        name: "name".into(),
                        version: "v0.1.2".into(),
                    }), // TODO For now its an error for this to be None - may need to revisit
                    logs: vec![LogRecord {
                        time_unix_nano: 0,
                        severity_number: 9,
                        severity_text: "INFO".into(),
                        name: "test".into(),
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
            "logs": [
                {
                    "resource": { "attributes": {}, "dropped_attributes_count": 8 },
                    "schema_url": "schema_url",
                    "instrumentation_library_logs": [
                        {
                            "instrumentation_library": { "name": "name", "version": "v0.1.2" },
                            "schema_url": "schema_url",
                            "logs": [{
                                "severity_number": 9,
                                "flags": 128,
                                "span_id": span_id_json,
                                "trace_id": trace_id_json,
                                "dropped_attributes_count": 100,
                                "time_unix_nano": 0,
                                "severity_text": "INFO",
                                "name": "test",
                                "attributes": {},
                                "body": "snot"
                            }]
                        }
                    ]
                }
            ]
        });

        assert_eq!(sorted_serialize(&expected)?, sorted_serialize(&json)?);
        assert_eq!(pb.resource_logs, back_again);

        Ok(())
    }

    #[test]
    fn resource_logs_severity_unspecified_regression() -> Result<()> {
        let nanos = tremor_common::time::nanotime();
        let span_id_pb = id::random_span_id_bytes(nanos);
        let span_id_json = id::hex_id_to_json(&span_id_pb);
        let trace_id_json = id::random_trace_id_value(nanos);
        let trace_id_pb = id::hex_trace_id_to_pb(Some(&trace_id_json))?;

        let pb = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                schema_url: "schema_url".into(),
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 8,
                }),
                instrumentation_library_logs: vec![InstrumentationLibraryLogs {
                    schema_url: "schema_url".into(),
                    instrumentation_library: Some(InstrumentationLibrary {
                        name: "name".into(),
                        version: "v0.1.2".into(),
                    }), // TODO For now its an error for this to be None - may need to revisit
                    logs: vec![LogRecord {
                        time_unix_nano: 0,
                        severity_number: 0,
                        severity_text: String::new(),
                        name: "test".into(),
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
            "logs": [
                {
                    "resource": { "attributes": {}, "dropped_attributes_count": 8 },
                    "schema_url": "schema_url",
                    "instrumentation_library_logs": [
                        {
                            "instrumentation_library": { "name": "name", "version": "v0.1.2" },
                            "schema_url": "schema_url",
                            "logs": [{
                                "severity_number": 0,
                                "flags": 128,
                                "span_id": span_id_json,
                                "trace_id": trace_id_json,
                                "dropped_attributes_count": 100,
                                "time_unix_nano": 0,
                                "severity_text": "",
                                "name": "test",
                                "attributes": {},
                                "body": "snot"
                            }]
                        }
                    ]
                }
            ]
        });

        assert_eq!(sorted_serialize(&expected)?, sorted_serialize(&json)?);
        assert_eq!(pb.resource_logs, back_again);

        Ok(())
    }

    #[test]
    fn minimal_logs() {
        let log = literal!({"logs": [
                {
                    "instrumentation_library_logs": [
                    ],
                    "schema_url": ""
                }
            ]
        });
        assert_eq!(
            Some(vec![ResourceLogs {
                instrumentation_library_logs: vec![],
                resource: None,
                schema_url: String::new()
            }]),
            resource_logs_to_pb(&log).ok()
        );
    }

    #[test]
    fn minimal_instrumentation_library_logs() {
        let ill = literal!([
            {
                "logs": [],
                "schema_url": ""
            }
        ]);
        assert_eq!(
            Some(vec![InstrumentationLibraryLogs {
                instrumentation_library: None,
                logs: vec![],
                schema_url: String::new()
            }]),
            maybe_instrumentation_library_logs_to_pb(Some(&ill)).ok()
        );
    }

    #[test]
    fn minimal_log_record() {
        let lr = literal!({});
        assert_eq!(
            Some(LogRecord {
                time_unix_nano: 0,
                severity_number: 0,
                severity_text: String::new(),
                name: String::new(),
                body: None,
                attributes: vec![],
                dropped_attributes_count: 0,
                flags: 0,
                trace_id: vec![],
                span_id: vec![]
            }),
            log_record_to_pb(&lr).ok()
        );
    }
}
