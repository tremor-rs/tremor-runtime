// Copyright 2020-2022, The Tremor Team
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

#![allow(dead_code)]

use super::{
    common::{self, maybe_instrumentation_scope_to_pb, EMPTY},
    id,
    resource::{self, resource_to_pb},
};
use crate::connectors::{
    impls::otel::common::maybe_instrumentation_scope_to_json,
    utils::pb::{maybe_int_to_pbi32, maybe_int_to_pbu32, maybe_int_to_pbu64, maybe_string_to_pb},
};
use crate::errors::Result;
use simd_json::Mutable;
use tremor_otelapis::opentelemetry::proto::trace::v1::ScopeSpans;
use tremor_value::literal;

use tremor_otelapis::opentelemetry::proto::{
    collector::trace::v1::ExportTraceServiceRequest,
    trace::v1::{
        span::{Event, Link},
        ResourceSpans, Span, Status,
    },
};

use tremor_value::Value;
use value_trait::ValueAccess;

pub(crate) fn status_to_json<'event>(data: Option<Status>) -> Value<'event> {
    data.map_or_else(
        || literal!({ "code": 0, "message": "status code unset" }),
        |data| {
            literal!({
                "code": data.code,
                "message": data.message
            })
        },
    )
}

pub(crate) fn span_events_to_json(pb: Vec<Event>) -> Value<'static> {
    pb.into_iter()
        .map(|data| {
            literal!({
                "time_unix_nano" : data.time_unix_nano,
                "name" : data.name.to_string(),
                "attributes" : common::key_value_list_to_json(data.attributes),
                "dropped_attributes_count" : data.dropped_attributes_count
            })
        })
        .collect()
}

pub(crate) fn span_events_to_pb(json: Option<&Value<'_>>) -> Result<Vec<Event>> {
    json.as_array()
        .unwrap_or(&EMPTY)
        .iter()
        .map(|json| {
            Ok(Event {
                name: maybe_string_to_pb(json.get("name"))?,
                time_unix_nano: maybe_int_to_pbu64(json.get("time_unix_nano"))?,
                attributes: common::maybe_key_value_list_to_pb(json.get("attributes"))
                    .unwrap_or_default(),
                dropped_attributes_count: maybe_int_to_pbu32(json.get("dropped_attributes_count"))
                    .unwrap_or_default(),
            })
        })
        .collect()
}

pub(crate) fn span_links_to_json(pb: Vec<Link>) -> Value<'static> {
    pb.into_iter()
        .map(|data| {
            literal!({
                "trace_id": id::hex_trace_id_to_json(&data.trace_id),
                "span_id": id::hex_span_id_to_json(&data.span_id),
                "trace_state": data.trace_state,
                "attributes": common::key_value_list_to_json(data.attributes),
                "dropped_attributes_count" : data.dropped_attributes_count
            })
        })
        .collect()
}

pub(crate) fn span_links_to_pb(json: Option<&Value<'_>>) -> Result<Vec<Link>> {
    json.as_array()
        .unwrap_or(&EMPTY)
        .iter()
        .map(|json| {
            Ok(Link {
                span_id: id::hex_span_id_to_pb(json.get("span_id"))?,
                trace_id: id::hex_trace_id_to_pb(json.get("trace_id"))?,
                trace_state: maybe_string_to_pb(json.get("trace_state"))?,
                attributes: common::maybe_key_value_list_to_pb(json.get("attributes"))?,
                dropped_attributes_count: maybe_int_to_pbu32(json.get("dropped_attributes_count"))?,
            })
        })
        .collect()
}

pub(crate) fn status_to_pb(json: Option<&Value<'_>>) -> Result<Option<Status>> {
    if json.is_none() {
        return Ok(None);
    }
    let json = json
        .as_object()
        .ok_or("Unable to map json value to pb trace status")?;

    Ok(Some(Status {
        code: maybe_int_to_pbi32(json.get("code"))?,
        message: maybe_string_to_pb(json.get("message"))?,
    }))
}

fn span_to_json(span: Span) -> Value<'static> {
    literal!({
        "attributes": common::key_value_list_to_json(span.attributes),
        "events": span_events_to_json(span.events),
        "links": span_links_to_json(span.links),
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
    })
}

pub(crate) fn span_to_pb(span: &Value<'_>) -> Result<Span> {
    Ok(Span {
        name: maybe_string_to_pb(span.get("name"))?,
        start_time_unix_nano: maybe_int_to_pbu64(span.get("start_time_unix_nano"))?,
        end_time_unix_nano: maybe_int_to_pbu64(span.get("end_time_unix_nano"))?,
        status: status_to_pb(span.get("status"))?,
        kind: maybe_int_to_pbi32(span.get("kind")).unwrap_or_default(),
        parent_span_id: id::hex_parent_span_id_to_pb(span.get("parent_span_id"))
            .unwrap_or_default(),
        span_id: id::hex_span_id_to_pb(span.get("span_id"))?,
        trace_id: id::hex_trace_id_to_pb(span.get("trace_id"))?,
        trace_state: maybe_string_to_pb(span.get("trace_state"))?,
        attributes: common::maybe_key_value_list_to_pb(span.get("attributes"))?,
        dropped_attributes_count: maybe_int_to_pbu32(span.get("dropped_attributes_count"))
            .unwrap_or_default(),
        dropped_events_count: maybe_int_to_pbu32(span.get("dropped_events_count"))
            .unwrap_or_default(),
        dropped_links_count: maybe_int_to_pbu32(span.get("dropped_links_count"))
            .unwrap_or_default(),
        events: span_events_to_pb(span.get("events"))?,
        links: span_links_to_pb(span.get("links"))?,
    })
}

pub(crate) fn scope_spans_to_json(data: Vec<ScopeSpans>) -> Vec<Value<'static>> {
    data.into_iter()
        .map(|pb| {
            literal!({
                "schema_url": pb.schema_url,
                "scope": maybe_instrumentation_scope_to_json(pb.scope),
                "spans": pb.spans.into_iter().map(span_to_json).collect::<Vec<Value>>(),
            })
        })
        .collect()
}

pub(crate) fn scope_spans_to_pb(data: Option<&Value<'_>>) -> Result<Vec<ScopeSpans>> {
    data.as_array()
        .ok_or("Invalid json mapping for ScopeSpans")?
        .iter()
        .filter_map(Value::as_object)
        .map(|data| {
            let spans = data
                .get("spans")
                .and_then(Value::as_array)
                .unwrap_or(&EMPTY)
                .iter()
                .map(span_to_pb)
                .collect::<Result<_>>()?;

            Ok(ScopeSpans {
                schema_url: data
                    .get("schema_url")
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                spans,
                scope: maybe_instrumentation_scope_to_pb(data.get("scope"))?,
            })
        })
        .collect()
}

pub(crate) fn resource_spans_to_json(request: ExportTraceServiceRequest) -> Result<Value<'static>> {
    let json: Result<Vec<Value<'static>>> = request
        .resource_spans
        .into_iter()
        .map(|span| {
            let mut base = literal!({
                    "schema_url": span.schema_url,
            });
            if let Some(r) = span.resource {
                base.try_insert("resource", resource::resource_to_json(r));
            };
            base.try_insert("scope_spans", scope_spans_to_json(span.scope_spans));
            Ok(base)
        })
        .collect();

    Ok(literal!({ "trace": Value::Array(json?) }))
}

pub(crate) fn resource_spans_to_pb(json: &Value<'_>) -> Result<Vec<ResourceSpans>> {
    json.get_array("trace")
        .ok_or("Invalid json mapping for otel trace message - cannot convert to pb")?
        .iter()
        .filter_map(Value::as_object)
        .map(|json| {
            Ok(ResourceSpans {
                schema_url: json
                    .get("schema_url")
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                resource: json.get("resource").map(resource_to_pb).transpose()?,
                scope_spans: scope_spans_to_pb(json.get("scope_spans"))?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tremor_otelapis::opentelemetry::proto::common::v1::InstrumentationScope;
    use tremor_otelapis::opentelemetry::proto::resource::v1::Resource;
    use tremor_script::utils::sorted_serialize;

    use super::*;

    #[test]
    fn status() -> Result<()> {
        let pb = Status {
            message: "everything is snot".into(),
            code: 1,
        };
        let json = status_to_json(Some(pb.clone()));
        let back_again = status_to_pb(Some(&json))?;
        let expected: Value = literal!({"message": "everything is snot", "code": 1});

        assert_eq!(expected, json);
        assert_eq!(Some(pb), back_again);

        // None
        let json = status_to_json(None);
        let back_again = status_to_pb(Some(&json))?;
        let expected: Value = literal!({"message": "status code unset", "code": 0});
        let pb = Status {
            message: "status code unset".into(),
            code: 0,
        };
        assert_eq!(expected, json);
        assert_eq!(Some(pb), back_again);

        // Invalid
        let invalid = status_to_pb(Some(&literal!("snot")));
        assert!(invalid.is_err());

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
        let json = span_events_to_json(pb.clone());
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

        // Empty span events
        let pb: Vec<Event> = vec![];
        let json = span_events_to_json(vec![]);
        let back_again = span_events_to_pb(Some(&json))?;
        let expected: Value = literal!([]);
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
        let json = span_links_to_json(pb.clone());
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

        // Empty span events
        let json = span_links_to_json(vec![]);
        let back_again = span_links_to_pb(Some(&json))?;
        let expected: Value = literal!([]);
        assert_eq!(expected, json);
        assert_eq!(back_again, vec![]);

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

        let pb = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                schema_url: "schema_url".into(),
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 8,
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "snot".to_string(),
                        version: "v1.2.3.4".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    spans: vec![Span {
                        start_time_unix_nano: 0,
                        end_time_unix_nano: 0,
                        name: "test".into(),
                        attributes: vec![],
                        dropped_attributes_count: 100,
                        trace_state: "snot:badger".into(),
                        parent_span_id: parent_span_id_pb,
                        span_id: span_id_pb.clone(),
                        trace_id: trace_id_pb,
                        kind: 0,
                        status: Some(Status {
                            code: 0,
                            message: "woot".into(),
                        }),
                        events: vec![],
                        dropped_events_count: 11,
                        links: vec![],
                        dropped_links_count: 13,
                    }],
                    schema_url: "schema_url".to_string(),
                }],
            }],
        };
        let json = resource_spans_to_json(pb.clone())?;
        let back_again = resource_spans_to_pb(&json)?;
        let expected: Value = literal!({
            "trace": [
                {
                    "resource": { "attributes": {}, "dropped_attributes_count": 8 },
                    "schema_url": "schema_url",
                    "scope_spans": [{
                        "schema_url": "schema_url",
                        "scope":{"attributes":{},"dropped_attributes_count":0,"name":"snot","version":"v1.2.3.4"},
                        "spans": [{
                            "start_time_unix_nano": 0u64,
                            "end_time_unix_nano": 0u64,
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

        assert_eq!(sorted_serialize(&expected)?, sorted_serialize(&json)?);
        assert_eq!(pb.resource_spans, back_again);

        let invalid = resource_spans_to_pb(&literal!("snot"));
        assert!(invalid.is_err());

        Ok(())
    }

    #[test]
    fn minimal_resource_spans() {
        let resource_spans = literal!({
            "trace": [
                {
                    "scope_spans": [],
                    "schema_url": "schema_url"
                }
            ]
        });
        assert_eq!(
            Ok(vec![ResourceSpans {
                resource: None,
                // instrumentation_library_spans: vec![],
                scope_spans: vec![],
                schema_url: "schema_url".to_string()
            }]),
            resource_spans_to_pb(&resource_spans)
        );
    }

    #[test]
    fn minimal_span() {
        let span = literal!({
            // hex encoded strings
            "trace_id": "61616161616161616161616161616161",
            "span_id": "6161616161616161",
            "trace_state": "",
            "parent_span_id": "", // empty means we are a root span
            "name": "span_name",
            "start_time_unix_nano": Duration::from_secs(1).as_nanos() as u64,
            "end_time_unix_nano": Duration::from_secs(2).as_nanos() as u64,
            "attributes": {},
            "dropped_attributes_count": 0
        });
        assert_eq!(
            Ok(Span {
                trace_id: b"aaaaaaaaaaaaaaaa".to_vec(),
                span_id: b"aaaaaaaa".to_vec(),
                trace_state: String::new(),
                parent_span_id: vec![],
                name: "span_name".to_string(),
                kind: 0,
                start_time_unix_nano: 1_000_000_000,
                end_time_unix_nano: 2_000_000_000,
                attributes: vec![],
                dropped_attributes_count: 0,
                events: vec![],
                dropped_events_count: 0,
                links: vec![],
                dropped_links_count: 0,
                status: None
            }),
            span_to_pb(&span)
        );
    }

    #[test]
    fn minimal_span_events() {
        let span_events = literal!([{
            "time_unix_nano": 1,
            "name": "urknall"
        }]);
        assert_eq!(
            Ok(vec![Event {
                time_unix_nano: 1,
                name: "urknall".to_string(),
                attributes: vec![],
                dropped_attributes_count: 0
            }]),
            span_events_to_pb(Some(&span_events))
        );
    }
}
