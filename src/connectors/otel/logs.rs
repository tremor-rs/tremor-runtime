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
            "instrumentation_library": common::maybe_instrumentation_library_to_json(data.instrumentation_library)?,
            "logs": logs
        }));
    }

    Ok(json!(json).into())
}

pub(crate) fn maybe_instrumentation_library_logs_to_pb<'event>(
    data: Option<&Value<'event>>,
) -> Result<InstrumentationLibraryLogs> {
    let mut logs = Vec::new();
    if let Some(Value::Object(data)) = data {
        if let Some(Value::Array(data)) = data.get("logs") {
            for log in data {
                let name: String = pb::maybe_string_to_pb(log.get("name"))?;
                let time_unix_nano: u64 = pb::maybe_int_to_pbu64(log.get("time_unix_nano"))?;
                let severity_number: i32 = pb::maybe_int_to_pbi32(log.get("severity_number"))?;
                let severity_text: String = pb::maybe_string_to_pb(log.get("severity_text"))?;
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
        return Ok(e);
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

pub(crate) fn resource_logs_to_pb<'event>(json: &Value<'event>) -> Result<Vec<ResourceLogs>> {
    if let Value::Object(json) = json {
        if let Some(Value::Array(json)) = json.get("logs") {
            let mut pb = Vec::new();
            for json in json {
                if let Value::Object(json) = json {
                    let mut instrumentation_library_logs = Vec::new();
                    if let Some(Value::Array(json)) = json.get("instrumentation_library_logs") {
                        for data in json {
                            let item = maybe_instrumentation_library_logs_to_pb(Some(data))?;
                            instrumentation_library_logs.push(item);
                        }
                    }
                    let resource = Some(resource::maybe_resource_to_pb(json.get("resource"))?);
                    let item = ResourceLogs {
                        resource,
                        instrumentation_library_logs,
                    };
                    pb.push(item);
                }
            }
            return Ok(pb);
        }
    }

    Err("Invalid json mapping for otel logs message - cannot convert to pb".into())
}
