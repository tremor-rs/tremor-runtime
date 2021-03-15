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
use crate::errors::Result;
use simd_json::{json, StaticNode};

use tremor_otelapis::opentelemetry::proto::{
    collector::trace::v1::ExportTraceServiceRequest,
    trace::v1::{
        span::{Event, Link},
        InstrumentationLibrarySpans, ResourceSpans, Span, Status,
    },
};

use simd_json::Value as SimdJsonValue;
use tremor_value::Value;

#[allow(deprecated)]
pub(crate) fn status_to_json<'event>(data: Option<Status>) -> Result<Value<'event>> {
    if let Some(data) = data {
        Ok(json!({
            "code": data.code,
            "deprecated_code": data.deprecated_code,
            "message": data.message
        })
        .into())
    } else {
        Ok(json!({ "code": 0, "deprecated_code": 0, "message": "status code unset" }).into())
    }
}

pub(crate) fn span_events_to_json<'event>(pb: Vec<Event>) -> Result<Value<'event>> {
    let mut json = Vec::new();
    for data in pb {
        json.push(
            json!({
                "time_unix_nano" : data.time_unix_nano,
                "name" : data.name.to_string(),
                "attributes" : common::key_value_list_to_json(data.attributes)?,
                "dropped_attributes_count" : data.dropped_attributes_count
            })
            .into(),
        );
    }
    Ok(Value::Array(json))
}

pub(crate) fn span_events_to_pb<'event>(json: Option<&Value<'event>>) -> Result<Vec<Event>> {
    let mut pb = Vec::new();
    if let Some(Value::Array(json)) = json {
        for json in json {
            let name: String = pb::maybe_string_to_pb(json.get("name"))?;
            let time_unix_nano: u64 = pb::maybe_int_to_pbu64(json.get("time_unix_nano"))?;
            let attributes = common::maybe_key_value_list_to_pb(json.get("attributes"))?;
            let dropped_attributes_count: u32 =
                pb::maybe_int_to_pbu32(json.get("dropped_attributes_count"))?;
            pb.push(Event {
                name,
                time_unix_nano,
                attributes,
                dropped_attributes_count,
            })
        }
    }
    Ok(pb)
}

pub(crate) fn span_links_to_json<'event>(pb: Vec<Link>) -> Result<Value<'event>> {
    let mut json = Vec::new();
    for data in pb {
        json.push(
            json!({
                "trace_id": data.trace_id,
                "span_id": data.span_id,
                "trace_state": data.trace_state,
                "attributes": common::key_value_list_to_json(data.attributes)?,
                "dropped_attributes_count" : data.dropped_attributes_count
            })
            .into(),
        );
    }
    Ok(Value::Array(json))
}

pub(crate) fn span_links_to_pb<'event>(json: Option<&Value<'event>>) -> Result<Vec<Link>> {
    let mut pb = Vec::new();
    if let Some(Value::Array(json)) = json {
        for json in json {
            let span_id = span_id_to_pb(json.get("span_id"))?;
            let trace_id = trace_id_to_pb(json.get("trace_id"))?;
            let trace_state: String = pb::maybe_string_to_pb(json.get("trace_state"))?;
            let attributes = common::maybe_key_value_list_to_pb(json.get("attributes"))?;
            let dropped_attributes_count: u32 =
                pb::maybe_int_to_pbu32(json.get("dropped_attributes_count"))?;
            pb.push(Link {
                span_id,
                trace_id,
                trace_state,
                attributes,
                dropped_attributes_count,
            });
        }
    }
    Ok(pb)
}

pub(crate) fn span_id_to_pb<'event>(data: Option<&Value<'event>>) -> Result<Vec<u8>> {
    let mut span_id = Vec::new();
    if let Some(Value::Array(a)) = data {
        for i in a {
            if let &Value::Static(StaticNode::I64(i)) = i {
                span_id.push(i as u8) // TODO check i <= 255
            } else if let &Value::Static(StaticNode::U64(i)) = i {
                span_id.push(i as u8) // TODO check i <= 255
            }
        }
        // TODO check span_id.len
        return Ok(span_id);
    }
    Err("Invalid json mapping of otel span id".into())
}

pub(crate) fn trace_id_to_pb<'event>(data: Option<&Value<'event>>) -> Result<Vec<u8>> {
    let mut trace_id = Vec::new();
    if let Some(Value::Array(a)) = data {
        for i in &*a {
            if let &Value::Static(StaticNode::I64(i)) = i {
                trace_id.push(i as u8) // TODO check i <= 255
            } else if let &Value::Static(StaticNode::U64(i)) = i {
                trace_id.push(i as u8) // TODO check i <= 255
            }
        }
        // TODO check trace_id.len
        return Ok(trace_id);
    }
    Err("Invalid json mapping of otel trace id".into())
}

pub(crate) fn status_to_pb<'event>(json: Option<&Value<'event>>) -> Result<Option<Status>> {
    match json {
        Some(Value::Object(json)) => {
            let code = pb::maybe_int_to_pbi32(json.get("code"))?;
            let deprecated_code = pb::maybe_int_to_pbi32(json.get("deprecated_code"))?;
            let message: String = pb::maybe_string_to_pb(json.get("message"))?;

            // This is generated code in the pb stub code deriving from otel proto files
            #[allow(deprecated)]
            Ok(Some(Status {
                deprecated_code,
                message,
                code,
            }))
        }
        Some(_other) => Err("Unable to map json value to pb trace status".into()),
        None => Ok(None),
    }
}

pub(crate) fn instrumentation_library_spans_to_pb<'event>(
    data: Option<&Value<'event>>,
) -> Result<InstrumentationLibrarySpans> {
    let mut spans = Vec::new();
    if let Some(Value::Object(data)) = data {
        if let Some(Value::Array(data)) = data.get("spans") {
            for span in data {
                let name: String = pb::maybe_string_to_pb(span.get("name"))?;
                let start_time_unix_nano: u64 =
                    pb::maybe_int_to_pbu64(span.get("start_time_unix_nano"))?;
                let end_time_unix_nano: u64 =
                    pb::maybe_int_to_pbu64(span.get("end_time_unix_nano"))?;
                let status = status_to_pb(span.get("status"))?;
                let kind = pb::maybe_int_to_pbi32(span.get("kind"))?;
                let parent_span_id = span_id_to_pb(span.get("parent_span_id"))?;
                let span_id = span_id_to_pb(span.get("span_id"))?;
                let trace_id = trace_id_to_pb(span.get("trace_id"))?;
                let trace_state: String = pb::maybe_string_to_pb(span.get("trace_state"))?;
                let attributes = common::maybe_key_value_list_to_pb(span.get("attributes"))?;
                let dropped_attributes_count: u32 =
                    pb::maybe_int_to_pbu32(span.get("dropped_attributes_count"))?;
                let dropped_events_count: u32 =
                    pb::maybe_int_to_pbu32(span.get("dropped_events_count"))?;
                let dropped_links_count: u32 =
                    pb::maybe_int_to_pbu32(span.get("dropped_links_count"))?;
                let events = span_events_to_pb(span.get("events"))?;
                let links = span_links_to_pb(span.get("links"))?;
                spans.push(Span {
                    status,
                    kind,
                    start_time_unix_nano,
                    end_time_unix_nano,
                    name,
                    trace_id,
                    parent_span_id,
                    span_id,
                    trace_state,
                    attributes,
                    dropped_attributes_count,
                    events,
                    dropped_events_count,
                    links,
                    dropped_links_count,
                });
            }
        }
        let il = data.get("instrumentation_library");
        let e = InstrumentationLibrarySpans {
            instrumentation_library: common::maybe_instrumentation_library_to_pb(il)?,
            spans,
        };
        return Ok(e);
    }
    Err("Invalid json mapping for InstrumentationLibrarySpans".into())
}

pub(crate) fn resource_spans_to_json<'event>(
    request: ExportTraceServiceRequest,
) -> Result<Value<'event>> {
    let mut spans = Vec::new();
    for span in request.resource_spans {
        let mut ia = Vec::new();
        let mut il = Vec::new();
        for el in span.instrumentation_library_spans {
            il.push(common::maybe_instrumentation_library_to_json(
                el.instrumentation_library,
            )?);
            for span in el.spans {
                il.push(
                    json!({
                        "attributes": common::key_value_list_to_json(span.attributes)?,
                        "events": span_events_to_json(span.events)?,
                        "links": span_links_to_json(span.links)?,
                        "span_id": span.span_id,
                        "parent_span_id": span.parent_span_id,
                        "trace_id": span.trace_id,
                        "start_time_unix_nano": span.start_time_unix_nano,
                        "trace_state": span.trace_state,
                        "dropped_attributes_count": span.dropped_attributes_count,
                        "dropped_events_count": span.dropped_events_count,
                        "dropped_links_count": span.dropped_links_count,
                        "status": status_to_json(span.status)?,
                        "kind": span.kind
                    })
                    .into(),
                )
            }
        }
        for trace in span.resource {
            ia.push(json!({"log": {
                "attributes": common::key_value_list_to_json(trace.attributes)?,
                "dropped_attribute_count": trace.dropped_attributes_count,
            }}));
        }
        spans.push(json!({
            "instrumentation_library_spans": il,
            "resource": ia,
        }));
    }
    Ok(json!({ "trace": { "resource_spans": spans }}).into())
}

pub(crate) fn resource_spans_to_pb<'event>(
    json: Option<&Value<'event>>,
) -> Result<Vec<ResourceSpans>> {
    if let Some(Value::Object(json)) = json {
        if let Some(Value::Array(json)) = json.get("spans") {
            let mut pb = Vec::new();
            for json in json {
                if let Value::Object(json) = json {
                    let mut instrumentation_library_spans = Vec::new();
                    if let Some(Value::Array(json)) = json.get("instrumentation_library_spans") {
                        for data in json {
                            let item = instrumentation_library_spans_to_pb(Some(data))?;
                            instrumentation_library_spans.push(item);
                        }
                    }
                    let resource = Some(resource::maybe_resource_to_pb(json.get("resource"))?);
                    let item = ResourceSpans {
                        resource,
                        instrumentation_library_spans,
                    };
                    pb.push(item);
                }
            }
            return Ok(pb);
        }
    }

    Err("Invalid json mapping for otel trace message - cannot convert to pb".into())
}
