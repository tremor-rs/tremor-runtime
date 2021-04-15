// Copyright 2020-2021, The Tremor Team
//
// Licensed under the Apache trace_id: (), span_id: (), trace_state: () License, Version 2.0 (the "License");
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
use tremor_value::literal;

use tremor_otelapis::opentelemetry::proto::{
    collector::trace::v1::ExportTraceServiceRequest,
    trace::v1::{
        span::{Event, Link},
        InstrumentationLibrarySpans, ResourceSpans, Span, Status,
    },
};

use tremor_value::Value;
use value_trait::ValueAccess;

#[allow(deprecated)]
pub(crate) fn status_to_json<'event>(data: Option<Status>) -> Value<'event> {
    if let Some(data) = data {
        literal!({
            "code": data.code,
            "deprecated_code": data.deprecated_code,
            "message": data.message
        })
    } else {
        literal!({ "code": 0, "deprecated_code": 0, "message": "status code unset" })
    }
}

pub(crate) fn span_events_to_json<'event>(pb: Vec<Event>) -> Result<Value<'event>> {
    let mut json = Vec::with_capacity(pb.len());
    for data in pb {
        json.push(literal!({
            "time_unix_nano" : data.time_unix_nano,
            "name" : data.name.to_string(),
            "attributes" : common::key_value_list_to_json(data.attributes)?,
            "dropped_attributes_count" : data.dropped_attributes_count
        }));
    }
    Ok(Value::Array(json))
}

pub(crate) fn span_events_to_pb(json: Option<&Value<'_>>) -> Result<Vec<Event>> {
    if let Some(Value::Array(json)) = json {
        let mut pb = Vec::with_capacity(json.len());
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
        return Ok(pb);
    }
    Ok(vec![])
}

pub(crate) fn span_links_to_json<'event>(pb: Vec<Link>) -> Result<Value<'event>> {
    let mut json = Vec::with_capacity(pb.len());
    for data in pb {
        json.push(literal!({
            "trace_id": id::hex_trace_id_to_json(&data.trace_id),
            "span_id": id::hex_span_id_to_json(&data.span_id),
            "trace_state": data.trace_state,
            "attributes": common::key_value_list_to_json(data.attributes)?,
            "dropped_attributes_count" : data.dropped_attributes_count
        }));
    }
    Ok(Value::Array(json))
}

pub(crate) fn span_links_to_pb(json: Option<&Value<'_>>) -> Result<Vec<Link>> {
    if let Some(Value::Array(json)) = json {
        let mut pb = Vec::with_capacity(json.len());
        for json in json {
            let span_id = id::hex_span_id_to_pb(json.get("span_id"))?;
            let trace_id = id::hex_trace_id_to_pb(json.get("trace_id"))?;
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
        return Ok(pb);
    }
    Ok(vec![])
}

pub(crate) fn status_to_pb(json: Option<&Value<'_>>) -> Result<Option<Status>> {
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

pub(crate) fn instrumentation_library_spans_to_json<'event>(
    data: Vec<InstrumentationLibrarySpans>,
) -> Result<Value<'event>> {
    let mut json: Vec<Value> = Vec::with_capacity(data.len());
    for data in data {
        let mut spans: Vec<Value> = Vec::with_capacity(data.spans.len());
        for span in data.spans {
            spans.push(literal!({
                "attributes": common::key_value_list_to_json(span.attributes)?,
                "events": span_events_to_json(span.events)?,
                "links": span_links_to_json(span.links)?,
                "span_id": id::hex_span_id_to_json(&span.span_id),
                "parent_span_id": id::hex_span_id_to_json(&span.parent_span_id),
                "trace_id": id::hex_trace_id_to_json(&span.trace_id),
                "start_time_unix_nano": span.start_time_unix_nano,
                "end_time_unix_nano": span.end_time_unix_nano,
                "trace_state": span.trace_state,
                "dropped_attributes_count": span.dropped_attributes_count,
                "dropped_events_count": span.dropped_events_count,
                "dropped_links_count": span.dropped_links_count,
                "status": status_to_json(span.status),
                "kind": span.kind,
                "name": span.name
            }));
        }

        json.push(literal!({
            "instrumentation_library": common::maybe_instrumentation_library_to_json(data.instrumentation_library),
            "spans": spans,

        }));
    }

    Ok(Value::Array(json))
}

pub(crate) fn instrumentation_library_spans_to_pb(
    data: Option<&Value<'_>>,
) -> Result<Vec<InstrumentationLibrarySpans>> {
    if let Some(Value::Array(data)) = data {
        let mut pb = Vec::with_capacity(data.len());
        for ill in data {
            if let Value::Object(data) = ill {
                let json_spans = data.get("spans");
                let mut spans = Vec::new();
                if let Some(Value::Array(data)) = json_spans {
                    for span in data {
                        let name: String = pb::maybe_string_to_pb(span.get("name"))?;
                        let start_time_unix_nano: u64 =
                            pb::maybe_int_to_pbu64(span.get("start_time_unix_nano"))?;
                        let end_time_unix_nano: u64 =
                            pb::maybe_int_to_pbu64(span.get("end_time_unix_nano"))?;
                        let status = status_to_pb(span.get("status"))?;
                        let kind = pb::maybe_int_to_pbi32(span.get("kind"))?;
                        let parent_span_id =
                            id::hex_parent_span_id_to_pb(span.get("parent_span_id"))?;
                        let span_id = id::hex_span_id_to_pb(span.get("span_id"))?;
                        let trace_id = id::hex_trace_id_to_pb(span.get("trace_id"))?;
                        let trace_state: String = pb::maybe_string_to_pb(span.get("trace_state"))?;
                        let attributes =
                            common::maybe_key_value_list_to_pb(span.get("attributes"))?;
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
                pb.push(e);
            }
        }
        return Ok(pb);
    }

    Err("Invalid json mapping for InstrumentationLibrarySpans".into())
}

pub(crate) fn resource_spans_to_json<'event>(
    request: ExportTraceServiceRequest,
) -> Result<Value<'event>> {
    let mut json = Vec::with_capacity(request.resource_spans.len());
    for span in request.resource_spans {
        json.push(literal!({
            "instrumentation_library_spans": instrumentation_library_spans_to_json(span.instrumentation_library_spans)?,
            "resource": resource::resource_to_json(span.resource)?,
        }));
    }
    Ok(literal!({ "trace": json }))
}

pub(crate) fn resource_spans_to_pb(json: Option<&Value<'_>>) -> Result<Vec<ResourceSpans>> {
    if let Some(Value::Object(json)) = json {
        if let Some(Value::Array(json)) = json.get("trace") {
            let mut pb = Vec::with_capacity(json.len());
            for json in json {
                if let Value::Object(json) = json {
                    let instrumentation_library_spans = instrumentation_library_spans_to_pb(
                        json.get("instrumentation_library_spans"),
                    )?;
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

#[cfg(test)]
mod tests {
    use tremor_otelapis::opentelemetry::proto::{
        common::v1::InstrumentationLibrary, resource::v1::Resource,
    };

    use super::*;

    #[test]
    #[allow(deprecated)]
    fn status() -> Result<()> {
        let pb = Status {
            deprecated_code: 0,
            message: "everything is snot".into(),
            code: 1,
        };
        let json = status_to_json(Some(pb.clone()));
        let back_again = status_to_pb(Some(&json))?;
        let expected: Value =
            literal!({"deprecated_code": 0, "message": "everything is snot", "code": 1});

        assert_eq!(expected, json);
        assert_eq!(Some(pb), back_again);

        Ok(())
    }

    #[test]
    fn span_event() -> Result<()> {
        let pb = vec![Event {
            time_unix_nano: 0,
            name: "badger".into(),
            attributes: vec![],
            dropped_attributes_count: 44,
        }];
        let json = span_events_to_json(pb.clone())?;
        let back_again = span_events_to_pb(Some(&json))?;
        let expected: Value = literal!([
            {
                "time_unix_nano": 0,
                "name": "badger",
                "attributes": {},
                "dropped_attributes_count": 44,
            }
        ]);

        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        Ok(())
    }

    #[test]
    fn span_link() -> Result<()> {
        let nanotime = tremor_common::time::nanotime();
        let span_id_pb = id::random_span_id_bytes(nanotime);
        let span_id_json = id::test::pb_span_id_to_json(&span_id_pb);
        let trace_id_json = id::random_trace_id_value(nanotime);
        let trace_id_pb = id::test::json_trace_id_to_pb(Some(&trace_id_json))?;

        let pb = vec![Link {
            attributes: vec![],
            dropped_attributes_count: 42,
            span_id: span_id_pb.clone(),
            trace_id: trace_id_pb,
            trace_state: "snot:badger".into(),
        }];
        let json = span_links_to_json(pb.clone())?;
        let back_again = span_links_to_pb(Some(&json))?;
        let expected: Value = literal!([
            {
                "span_id": span_id_json,
                "trace_id": trace_id_json,
                "trace_state": "snot:badger",
                "attributes": {},
                "dropped_attributes_count": 42,
            }
        ]);

        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        Ok(())
    }

    #[test]
    #[allow(deprecated)]
    fn instrument_library_spans() -> Result<()> {
        let nanotime = tremor_common::time::nanotime();
        let parent_span_id_json = id::random_span_id_value(nanotime);
        let parent_span_id_pb = id::test::json_span_id_to_pb(Some(&parent_span_id_json))?;
        let span_id_pb = id::random_span_id_bytes(nanotime);
        let span_id_json = id::test::pb_span_id_to_json(&span_id_pb);
        let trace_id_json = id::random_trace_id_value(nanotime);
        let trace_id_pb = id::test::json_trace_id_to_pb(Some(&trace_id_json))?;

        let pb = vec![InstrumentationLibrarySpans {
            instrumentation_library: Some(InstrumentationLibrary {
                name: "name".into(),
                version: "v0.1.2".into(),
            }), // TODO For now its an error for this to be None - may need to revisit
            spans: vec![Span {
                start_time_unix_nano: 0,
                end_time_unix_nano: 0,
                name: "test".into(),
                attributes: vec![],
                dropped_attributes_count: 100,
                trace_state: "snot:badger".into(),
                parent_span_id: parent_span_id_pb.clone(),
                span_id: span_id_pb.clone(),
                trace_id: trace_id_pb,
                kind: 0,
                status: Some(Status {
                    code: 0,
                    deprecated_code: 0,
                    message: "woot".into(),
                }),
                events: vec![],
                dropped_events_count: 11,
                links: vec![],
                dropped_links_count: 13,
            }],
        }];
        let json = instrumentation_library_spans_to_json(pb.clone())?;
        let back_again = instrumentation_library_spans_to_pb(Some(&json))?;
        let expected: Value = literal!([{
            "instrumentation_library": { "name": "name", "version": "v0.1.2" },
            "spans": [{
                  "start_time_unix_nano": 0,
                  "end_time_unix_nano": 0,
                  "name": "test",
                  "dropped_attributes_count": 100,
                  "attributes": {},
                  "trace_state": "snot:badger",
                  "parent_span_id": parent_span_id_json,
                  "span_id": span_id_json,
                  "trace_id": trace_id_json,
                  "kind": 0,
                  "status": {
                      "code": 0,
                      "deprecated_code": 0,
                      "message": "woot"
                  },
                  "events": [],
                  "links": [],
                  "dropped_events_count": 11,
                  "dropped_links_count": 13,
                }
            ]
        }]);

        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        Ok(())
    }

    #[test]
    fn resource_spans() -> Result<()> {
        let nanotime = tremor_common::time::nanotime();
        let parent_span_id_json = id::random_span_id_value(nanotime);
        let parent_span_id_pb = id::test::json_span_id_to_pb(Some(&parent_span_id_json))?;
        let span_id_pb = id::random_span_id_bytes(nanotime);
        let span_id_json = id::test::pb_span_id_to_json(&span_id_pb);
        let trace_id_json = id::random_trace_id_value(nanotime);
        let trace_id_pb = id::test::json_trace_id_to_pb(Some(&trace_id_json))?;

        #[allow(deprecated)]
        let pb = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 8,
                }),
                instrumentation_library_spans: vec![InstrumentationLibrarySpans {
                    instrumentation_library: Some(InstrumentationLibrary {
                        name: "name".into(),
                        version: "v0.1.2".into(),
                    }), // TODO For now its an error for this to be None - may need to revisit
                    spans: vec![Span {
                        start_time_unix_nano: 0,
                        end_time_unix_nano: 0,
                        name: "test".into(),
                        attributes: vec![],
                        dropped_attributes_count: 100,
                        trace_state: "snot:badger".into(),
                        parent_span_id: parent_span_id_pb.clone(),
                        span_id: span_id_pb.clone(),
                        trace_id: trace_id_pb,
                        kind: 0,
                        status: Some(Status {
                            code: 0,
                            deprecated_code: 0,
                            message: "woot".into(),
                        }),
                        events: vec![],
                        dropped_events_count: 11,
                        links: vec![],
                        dropped_links_count: 13,
                    }],
                }],
            }],
        };
        let json = resource_spans_to_json(pb.clone())?;
        let back_again = resource_spans_to_pb(Some(&json))?;
        let expected: Value = literal!({
            "trace": [
                {
                    "resource": { "attributes": {}, "dropped_attributes_count": 8 },
                    "instrumentation_library_spans": [{
                        "instrumentation_library": { "name": "name", "version": "v0.1.2" },
                        "spans": [{
                            "start_time_unix_nano": 0,
                            "end_time_unix_nano": 0,
                            "name": "test",
                            "dropped_attributes_count": 100,
                            "attributes": {},
                            "trace_state": "snot:badger",
                            "parent_span_id": parent_span_id_json,
                            "span_id": span_id_json,
                            "trace_id": trace_id_json,
                            "kind": 0,
                            "status": {
                                "code": 0,
                                "deprecated_code": 0,
                                "message": "woot"
                            },
                            "events": [],
                            "links": [],
                            "dropped_events_count": 11,
                            "dropped_links_count": 13,
                            }
                        ]
                    }]
                }
            ]
        });

        assert_eq!(expected, json);
        assert_eq!(pb.resource_spans, back_again);

        Ok(())
    }
}
