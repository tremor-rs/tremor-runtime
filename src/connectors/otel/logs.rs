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

use super::super::pb;
use super::common;
use super::resource;
use super::trace;
use crate::errors::Result;
use simd_json::json;
use tremor_otelapis::opentelemetry::proto::{
    collector::logs::v1::ExportLogsServiceRequest,
    logs::v1::{InstrumentationLibraryLogs, LogRecord, ResourceLogs},
};

use simd_json::Value as SimdJsonValue;
use tremor_value::Value;

pub(crate) fn instrumentation_library_logs_to_json<'event>(
    pb: Vec<tremor_otelapis::opentelemetry::proto::logs::v1::InstrumentationLibraryLogs>,
) -> Result<Value<'event>> {
    let mut json = Vec::new();
    for data in pb {
        let mut logs = Vec::new();
        for log in data.logs {
            logs.push(json!({
                "name": log.name,
                "time_unix_nano": log.time_unix_nano,
                "severity_number": log.severity_number,
                "severity_text": log.severity_text,
                "flags": log.flags,
                "span_id": log.span_id,
                "trace_id": log.trace_id,
                "attributes": common::key_value_list_to_json(log.attributes)?,
                "dropped_attributes_count": log.dropped_attributes_count,
                "body": common::maybe_any_value_to_json(log.body)?,
            }));
        }
        json.push(json!({
            "instrumentation_library": common::maybe_instrumentation_library_to_json(data.instrumentation_library),
            "logs": logs
        }));
    }

    Ok(json!(json).into())
}

pub(crate) fn maybe_instrumentation_library_logs_to_pb(
    data: Option<&Value<'_>>,
) -> Result<Vec<InstrumentationLibraryLogs>> {
    let mut pb = Vec::new();
    if let Some(Value::Array(data)) = data {
        for ill in data {
            if let Value::Object(data) = ill {
                let mut logs = Vec::new();
                if let Some(Value::Array(data)) = data.get("logs") {
                    for log in data {
                        let name: String = pb::maybe_string_to_pb(log.get("name"))?;
                        let time_unix_nano: u64 =
                            pb::maybe_int_to_pbu64(log.get("time_unix_nano"))?;
                        let severity_number: i32 =
                            pb::maybe_int_to_pbi32(log.get("severity_number"))?;
                        let severity_text: String =
                            pb::maybe_string_to_pb(log.get("severity_text"))?;
                        let flags = pb::maybe_int_to_pbu32(log.get("flags"))?;
                        let span_id = trace::span_id_to_pb(log.get("span_id"))?;
                        let trace_id = trace::trace_id_to_pb(log.get("trace_id"))?;
                        let dropped_attributes_count: u32 =
                            pb::maybe_int_to_pbu32(log.get("dropped_attributes_count"))?;
                        let attributes = common::maybe_key_value_list_to_pb(log.get("attributes"))?;
                        let body = Some(common::maybe_any_value_to_pb(log.get("body"))?);
                        logs.push(LogRecord {
                            time_unix_nano,
                            severity_number,
                            severity_text,
                            name,
                            attributes,
                            dropped_attributes_count,
                            flags,
                            trace_id,
                            span_id,
                            body,
                        });
                    }
                }
                let il = data.get("instrumentation_library");
                let e = InstrumentationLibraryLogs {
                    instrumentation_library: common::maybe_instrumentation_library_to_pb(il)?,
                    logs,
                };
                pb.push(e);
            }
        }
        return Ok(pb);
    }

    Err("Invalid json mapping for InstrumentationLibraryLogs".into())
}

pub(crate) fn resource_logs_to_json<'event>(
    request: ExportLogsServiceRequest,
) -> Result<Value<'event>> {
    let mut json = Vec::new();
    for log in request.resource_logs {
        json.push(json!({
                "instrumentation_library_logs":
                    instrumentation_library_logs_to_json(log.instrumentation_library_logs)?,
                "resource": resource::resource_to_json(log.resource)?
        }));
    }

    Ok(json!({ "logs": json }).into())
}

pub(crate) fn resource_logs_to_pb(json: &Value<'_>) -> Result<Vec<ResourceLogs>> {
    if let Value::Object(json) = json {
        let mut pb = Vec::new();
        if let Some(Value::Array(json)) = json.get("logs") {
            for json in json {
                if let Value::Object(json) = json {
                    let instrumentation_library_logs = maybe_instrumentation_library_logs_to_pb(
                        json.get("instrumentation_library_logs"),
                    )?;
                    let resource = Some(resource::maybe_resource_to_pb(json.get("resource"))?);
                    let item = ResourceLogs {
                        resource,
                        instrumentation_library_logs,
                    };
                    pb.push(item);
                }
            }
        }
        return Ok(pb);
    }

    Err("Invalid json mapping for otel logs message - cannot convert to pb".into())
}

#[cfg(test)]
mod tests {
    use tremor_otelapis::opentelemetry::proto::{
        common::v1::{any_value, AnyValue, InstrumentationLibrary},
        resource::v1::Resource,
    };

    use super::*;

    #[test]
    fn instrumentation_library_logs() -> Result<()> {
        let pb = vec![InstrumentationLibraryLogs {
            instrumentation_library: Some(InstrumentationLibrary {
                name: "name".into(),
                version: "v0.1.2".into(),
            }), // TODO For now its an error for this to be None - may need to revisit
            logs: vec![LogRecord {
                time_unix_nano: 0,
                severity_number: 0,
                severity_text: "woo - not good at all".into(),
                name: "test".into(),
                body: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("snot".into())),
                }), // TODO For now its an error for this to be None - may need to revisit
                attributes: vec![],
                dropped_attributes_count: 100,
                flags: 0,
                span_id: vec![],  // TODO enforce length
                trace_id: vec![], // TODO enforce length
            }],
        }];
        let json = instrumentation_library_logs_to_json(pb.clone())?;
        let back_again = maybe_instrumentation_library_logs_to_pb(Some(&json))?;
        let expected: Value = json!([{
            "instrumentation_library": { "name": "name", "version": "v0.1.2" },
            "logs": [
                { "severity_number": 0,
                  "flags": 0,
                  "span_id": [], // TODO proper span id assertions
                  "trace_id": [], // TODO proper trace id assertions
                  "dropped_attributes_count": 100,
                  "time_unix_nano": 0,
                  "severity_text": "woo - not good at all",
                  "name": "test",
                  "attributes": [],
                  "body": "snot"
                }
            ]
        }])
        .into();

        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        Ok(())
    }

    #[test]
    fn resource_logs() -> Result<()> {
        let pb = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 8,
                }),
                instrumentation_library_logs: vec![InstrumentationLibraryLogs {
                    instrumentation_library: Some(InstrumentationLibrary {
                        name: "name".into(),
                        version: "v0.1.2".into(),
                    }), // TODO For now its an error for this to be None - may need to revisit
                    logs: vec![LogRecord {
                        time_unix_nano: 0,
                        severity_number: 0,
                        severity_text: "woo - not good at all".into(),
                        name: "test".into(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("snot".into())),
                        }), // TODO For now its an error for this to be None - may need to revisit
                        attributes: vec![],
                        dropped_attributes_count: 100,
                        flags: 0,
                        span_id: vec![],  // TODO enforce length
                        trace_id: vec![], // TODO enforce length
                    }],
                }],
            }],
        };
        let json = resource_logs_to_json(pb.clone())?;
        let back_again = resource_logs_to_pb(&json)?;
        let expected: Value = json!({
            "logs": [
                {
                    "resource": { "attributes": [], "dropped_attributes_count": 8 },
                    "instrumentation_library_logs": [
                        {
                            "instrumentation_library": { "name": "name", "version": "v0.1.2" },
                            "logs": [
                                { "severity_number": 0,
                                "flags": 0,
                                "span_id": [], // TODO proper span id assertions
                                "trace_id": [], // TODO proper trace id assertions
                                "dropped_attributes_count": 100,
                                "time_unix_nano": 0,
                                "severity_text": "woo - not good at all",
                                "name": "test",
                                "attributes": [],
                                "body": "snot"
                                }
                            ]
                        }
                    ]
                }
            ]
        })
        .into();

        assert_eq!(expected, json);
        assert_eq!(pb.resource_logs, back_again);

        Ok(())
    }
}
