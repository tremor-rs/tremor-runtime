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
use super::id;
use super::resource;
use crate::errors::Result;
use simd_json::json;
use tremor_otelapis::opentelemetry::proto::{
    collector::logs::v1::ExportLogsServiceRequest,
    logs::v1::{InstrumentationLibraryLogs, LogRecord, ResourceLogs},
};

use tremor_value::Value;
use value_trait::ValueAccess;

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
    if severity_number > 0 && severity_number <= 24 {
        Ok(severity_number)
    } else {
        Err(format!(
            "The `severity_number` is in the valid range 0 < {} <= 24",
            severity_number
        )
        .into())
    }
}

#[allow(clippy::unnecessary_wraps)] // NOTE until const / const fn support improves - see TODO
fn affirm_severity_text_valid<T>(severity_text: &T) -> Result<String>
where
    T: ToString,
{
    let text = severity_text.to_string();
    // TODO find a nice way to do this without RT regex comp
    //
    // if SEVERITY_TEXT_RE.is_match(&text) {
    //     return Ok(text);
    // } else {
    //     Err(format!("The `severity_text` is invalid {}", text).into())
    // }
    Ok(text)
}

pub(crate) fn instrumentation_library_logs_to_json<'event>(
    pb: Vec<tremor_otelapis::opentelemetry::proto::logs::v1::InstrumentationLibraryLogs>,
) -> Result<Value<'event>> {
    let mut json = Vec::new();
    for data in pb {
        let mut logs = Vec::new();
        for log in data.logs {
            // TODO This is going to be pretty slow going from Owned -> Value - consider json! for borrowed
            logs.push(json!({
                "name": log.name,
                "time_unix_nano": log.time_unix_nano,
                "severity_number": affirm_severity_number_valid(log.severity_number)?,
                "severity_text": affirm_severity_text_valid(&log.severity_text)?,
                "flags": affirm_traceflags_valid(log.flags)?,
                "span_id": id::hex_span_id_to_json(&log.span_id),
                "trace_id": id::hex_trace_id_to_json(&log.trace_id),
                "attributes": common::key_value_list_to_json(log.attributes)?,
                "dropped_attributes_count": log.dropped_attributes_count,
                "body": common::maybe_any_value_to_json(log.body)?,
            }));
        }
        // TODO This is going to be pretty slow going from Owned -> Value - consider json! for borrowed
        json.push(json!({
            "instrumentation_library": common::maybe_instrumentation_library_to_json(data.instrumentation_library),
            "logs": logs
        }));
    }

    // TODO This is going to be pretty slow going from Owned -> Value - consider json! for borrowed
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
                        let severity_number: i32 = affirm_severity_number_valid(
                            pb::maybe_int_to_pbi32(log.get("severity_number"))?,
                        )?;
                        let severity_text: String = affirm_severity_text_valid(
                            &pb::maybe_string_to_pb(log.get("severity_text"))?,
                        )?;
                        let flags =
                            affirm_traceflags_valid(pb::maybe_int_to_pbu32(log.get("flags"))?)?;
                        let span_id = id::hex_span_id_to_pb(log.get("span_id"))?;
                        let trace_id = id::hex_trace_id_to_pb(log.get("trace_id"))?;
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
        // TODO This is going to be pretty slow going from Owned -> Value - consider json! for borrowed
        json.push(json!({
                "instrumentation_library_logs":
                    instrumentation_library_logs_to_json(log.instrumentation_library_logs)?,
                "resource": resource::resource_to_json(log.resource)?
        }));
    }
    // TODO This is going to be pretty slow going from Owned -> Value - consider json! for borrowed
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
        let nanos = tremor_common::time::nanotime();
        let span_id_pb = id::random_span_id_bytes(nanos);
        let span_id_json = id::test::pb_span_id_to_json(&span_id_pb);
        let trace_id_json = id::random_trace_id_value(nanos);
        let trace_id_pb = id::test::json_trace_id_to_pb(Some(&trace_id_json))?;

        let pb = vec![InstrumentationLibraryLogs {
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
        let expected: Value = json!([{
            "instrumentation_library": { "name": "name", "version": "v0.1.2" },
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
        }])
        .into();

        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        Ok(())
    }

    #[test]
    fn resource_logs() -> Result<()> {
        let nanos = tremor_common::time::nanotime();
        let span_id_pb = id::random_span_id_bytes(nanos);
        let span_id_json = id::test::pb_span_id_to_json(&span_id_pb);
        let trace_id_json = id::random_trace_id_value(nanos);
        let trace_id_pb = id::test::json_trace_id_to_pb(Some(&trace_id_json))?;

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
        let expected: Value = json!({
            "logs": [
                {
                    "resource": { "attributes": {}, "dropped_attributes_count": 8 },
                    "instrumentation_library_logs": [
                        {
                            "instrumentation_library": { "name": "name", "version": "v0.1.2" },
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
        })
        .into();

        assert_eq!(expected, json);
        assert_eq!(pb.resource_logs, back_again);

        Ok(())
    }
}
