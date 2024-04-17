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

use crate::{
    common::{self, Error, EMPTY},
    id,
    pb::maybe_string,
    resource::{self, resource_to_pb},
};
use tremor_otelapis::opentelemetry::proto::{
    collector::trace::v1::ExportTraceServiceRequest,
    trace::v1::{
        span::{Event, Link},
        InstrumentationLibrarySpans, ResourceSpans, Span, Status,
    },
};
use tremor_value::prelude::*;

#[allow(deprecated)]
pub(crate) fn status_to_json<'event>(data: Option<Status>) -> Value<'event> {
    data.map_or_else(
        || literal!({ "code": 0, "deprecated_code": 0, "message": "status code unset" }),
        |data| {
            literal!({
                "code": data.code,
                "deprecated_code": data.deprecated_code,
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

pub(crate) fn span_events_to_pb(json: Option<&Value<'_>>) -> Result<Vec<Event>, Error> {
    json.as_array()
        .unwrap_or(&EMPTY)
        .iter()
        .map(|json| {
            Ok(Event {
                name: maybe_string(json.get("name"))?,
                time_unix_nano: {
                    let data = json.get("time_unix_nano");
                    data.try_as_u64()
                }?,
                attributes: common::maybe_key_value_list_to_pb(json.get("attributes"))
                    .unwrap_or_default(),
                dropped_attributes_count: {
                    let data = json.get("dropped_attributes_count");
                    data.try_as_u32()
                }
                .unwrap_or_default(),
            })
        })
        .collect()
}

pub(crate) fn span_links_to_json(pb: Vec<Link>) -> Value<'static> {
    pb.into_iter()
        .map(|data| {
            literal!({
                "trace_id": id::hex_id_to_json(&data.trace_id),
                "span_id": id::hex_id_to_json(&data.span_id),
                "trace_state": data.trace_state,
                "attributes": common::key_value_list_to_json(data.attributes),
                "dropped_attributes_count" : data.dropped_attributes_count
            })
        })
        .collect()
}

pub(crate) fn span_links_to_pb(json: Option<&Value<'_>>) -> Result<Vec<Link>, Error> {
    json.as_array()
        .unwrap_or(&EMPTY)
        .iter()
        .map(|json| {
            Ok(Link {
                span_id: id::hex_span_id_to_pb(json.get("span_id"))?,
                trace_id: id::hex_trace_id_to_pb(json.get("trace_id"))?,
                trace_state: maybe_string(json.get("trace_state"))?,
                attributes: common::maybe_key_value_list_to_pb(json.get("attributes"))?,
                dropped_attributes_count: {
                    let data = json.get("dropped_attributes_count");
                    data.try_as_u32()
                }?,
            })
        })
        .collect()
}

pub(crate) fn status_to_pb(json: Option<&Value<'_>>) -> Result<Option<Status>, Error> {
    if json.is_none() {
        return Ok(None);
    }
    let json = json.try_as_object()?;

    // This is generated code in the pb stub code deriving from otel proto files
    #[allow(deprecated)]
    Ok(Some(Status {
        code: {
            let data = json.get("code");
            data.try_as_i32()
        }?,
        deprecated_code: {
            let data = json.get("deprecated_code");
            data.try_as_i32()
        }?,
        message: maybe_string(json.get("message"))?,
    }))
}

fn span_to_json(span: Span) -> Value<'static> {
    literal!({
        "attributes": common::key_value_list_to_json(span.attributes),
        "events": span_events_to_json(span.events),
        "links": span_links_to_json(span.links),
        "span_id": id::hex_id_to_json(&span.span_id),
        "parent_span_id": id::hex_id_to_json(&span.parent_span_id),
        "trace_id": id::hex_id_to_json(&span.trace_id),
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

pub(crate) fn span_to_pb(span: &Value<'_>) -> Result<Span, Error> {
    Ok(Span {
        name: maybe_string(span.get("name"))?,
        start_time_unix_nano: {
            let data = span.get("start_time_unix_nano");
            data.try_as_u64()
        }?,
        end_time_unix_nano: {
            let data = span.get("end_time_unix_nano");
            data.try_as_u64()
        }?,
        status: status_to_pb(span.get("status"))?,
        kind: {
            let data = span.get("kind");
            data.try_as_i32()
        }
        .unwrap_or_default(),
        parent_span_id: id::hex_parent_span_id_to_pb(span.get("parent_span_id"))
            .unwrap_or_default(),
        span_id: id::hex_span_id_to_pb(span.get("span_id"))?,
        trace_id: id::hex_trace_id_to_pb(span.get("trace_id"))?,
        trace_state: maybe_string(span.get("trace_state"))?,
        attributes: common::maybe_key_value_list_to_pb(span.get("attributes"))?,
        dropped_attributes_count: {
            let data = span.get("dropped_attributes_count");
            data.try_as_u32()
        }
        .unwrap_or_default(),
        dropped_events_count: {
            let data = span.get("dropped_events_count");
            data.try_as_u32()
        }
        .unwrap_or_default(),
        dropped_links_count: {
            let data = span.get("dropped_links_count");
            data.try_as_u32()
        }
        .unwrap_or_default(),
        events: span_events_to_pb(span.get("events"))?,
        links: span_links_to_pb(span.get("links"))?,
    })
}

pub(crate) fn instrumentation_library_spans_to_json(
    data: Vec<InstrumentationLibrarySpans>,
) -> Value<'static> {
    let mut json: Vec<Value> = Vec::with_capacity(data.len());
    for data in data {
        let spans: Value = data.spans.into_iter().map(span_to_json).collect();

        let mut e = literal!({ "spans": spans, "schema_url": data.schema_url });
        if let Some(il) = data.instrumentation_library {
            let il = common::maybe_instrumentation_library_to_json(il);
            e.try_insert("instrumentation_library", il);
        }
        json.push(e);
    }

    Value::from(json)
}

pub(crate) fn instrumentation_library_spans_to_pb(
    data: Option<&Value<'_>>,
) -> Result<Vec<InstrumentationLibrarySpans>, Error> {
    data.as_array()
        .ok_or(Error::InvalidMapping("InstrumentationLibrarySpans"))?
        .iter()
        .filter_map(Value::as_object)
        .map(|data| {
            let spans = data
                .get("spans")
                .and_then(Value::as_array)
                .unwrap_or(&EMPTY)
                .iter()
                .map(span_to_pb)
                .collect::<Result<_, _>>()?;

            Ok(InstrumentationLibrarySpans {
                instrumentation_library: data
                    .get("instrumentation_library")
                    .map(common::instrumentation_library_to_pb)
                    .transpose()?,
                schema_url: data
                    .get("schema_url")
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                spans,
            })
        })
        .collect()
}

pub(crate) fn resource_spans_to_json(request: ExportTraceServiceRequest) -> Value<'static> {
    let json: Value = request
        .resource_spans
        .into_iter()
        .map(|span| {
            let ill = instrumentation_library_spans_to_json(span.instrumentation_library_spans);
            let mut base =
                literal!({ "instrumentation_library_spans": ill, "schema_url": span.schema_url });
            if let Some(r) = span.resource {
                base.try_insert("resource", resource::resource_to_json(r));
            };
            base
        })
        .collect();

    literal!({ "trace": json })
}

pub(crate) fn resource_spans_to_pb(json: Option<&Value<'_>>) -> Result<Vec<ResourceSpans>, Error> {
    json.get_array("trace")
        .ok_or(Error::InvalidMapping("ResourceSpans"))?
        .iter()
        .filter_map(Value::as_object)
        .map(|json| {
            Ok(ResourceSpans {
                instrumentation_library_spans: instrumentation_library_spans_to_pb(
                    json.get("instrumentation_library_spans"),
                )?,
                schema_url: json
                    .get("schema_url")
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                resource: json.get("resource").map(resource_to_pb).transpose()?,
            })
        })
        .collect::<Result<_, Error>>()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tremor_otelapis::opentelemetry::proto::{
        common::v1::InstrumentationLibrary, resource::v1::Resource,
    };
    use tremor_value::utils::sorted_serialize;

    use super::*;

    #[test]
    #[allow(deprecated)]
    fn status() -> anyhow::Result<()> {
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

        // None
        let json = status_to_json(None);
        let back_again = status_to_pb(Some(&json))?;
        let expected: Value =
            literal!({"deprecated_code": 0, "message": "status code unset", "code": 0});
        let pb = Status {
            deprecated_code: 0,
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
    fn span_event() -> anyhow::Result<()> {
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
    fn span_link() -> anyhow::Result<()> {
        let nanotime = tremor_common::time::nanotime();
        let span_id_pb = id::random_span_id_bytes(nanotime);
        let span_id_json = id::hex_id_to_json(&span_id_pb);
        let trace_id_json = id::random_trace_id_value(nanotime);
        let trace_id_pb = id::hex_trace_id_to_pb(Some(&trace_id_json))?;

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
    #[allow(deprecated)]
    fn instrument_library_spans() -> anyhow::Result<()> {
        let nanotime = tremor_common::time::nanotime();
        let parent_span_id_json = id::random_span_id_value(nanotime);
        let parent_span_id_pb = id::hex_span_id_to_pb(Some(&parent_span_id_json))?;
        let span_id_pb = id::random_span_id_bytes(nanotime);
        let span_id_json = id::hex_id_to_json(&span_id_pb);
        let trace_id_json = id::random_trace_id_value(nanotime);
        let trace_id_pb = id::hex_trace_id_to_pb(Some(&trace_id_json))?;

        let pb = vec![InstrumentationLibrarySpans {
            schema_url: "schema_url".into(),
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
                parent_span_id: parent_span_id_pb,
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
        let json = instrumentation_library_spans_to_json(pb.clone());
        let back_again = instrumentation_library_spans_to_pb(Some(&json))?;
        let expected: Value = literal!([{
            "instrumentation_library": { "name": "name", "version": "v0.1.2" },
            "schema_url": "schema_url",
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

        let invalid = instrumentation_library_spans_to_pb(Some(&literal!("snot")));
        assert!(invalid.is_err());

        Ok(())
    }

    #[test]
    fn resource_spans() -> anyhow::Result<()> {
        let nanotime = tremor_common::time::nanotime();
        let parent_span_id_json = id::random_span_id_value(nanotime);
        let parent_span_id_pb = id::hex_span_id_to_pb(Some(&parent_span_id_json))?;
        let span_id_pb = id::random_span_id_bytes(nanotime);
        let span_id_json = id::hex_id_to_json(&span_id_pb);
        let trace_id_json = id::random_trace_id_value(nanotime);
        let trace_id_pb = id::hex_trace_id_to_pb(Some(&trace_id_json))?;

        #[allow(deprecated)]
        let pb = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                schema_url: "schema_url".into(),
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 8,
                }),
                instrumentation_library_spans: vec![InstrumentationLibrarySpans {
                    schema_url: "schema_url".into(),
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
                        parent_span_id: parent_span_id_pb,
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
        let json = resource_spans_to_json(pb.clone());
        let back_again = resource_spans_to_pb(Some(&json))?;
        let expected: Value = literal!({
            "trace": [
                {
                    "resource": { "attributes": {}, "dropped_attributes_count": 8 },
                    "schema_url": "schema_url",
                    "instrumentation_library_spans": [{
                        "instrumentation_library": { "name": "name", "version": "v0.1.2" },
                        "schema_url": "schema_url",
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

        assert_eq!(sorted_serialize(&expected)?, sorted_serialize(&json)?);
        assert_eq!(pb.resource_spans, back_again);

        let invalid = resource_spans_to_pb(Some(&literal!("snot")));
        assert!(invalid.is_err());

        Ok(())
    }

    #[test]
    fn minimal_resource_spans() {
        let resource_spans = literal!({
            "trace": [
                {
                    "instrumentation_library_spans": [],
                    "schema_url": "schema_url"
                }
            ]
        });
        assert_eq!(
            resource_spans_to_pb(Some(&resource_spans)).ok(),
            Some(vec![ResourceSpans {
                resource: None,
                instrumentation_library_spans: vec![],
                schema_url: "schema_url".to_string()
            }]),
        );
    }

    #[test]
    fn minimal_instrumentation_library_spans() {
        let ils = literal!([{
            "spans": [],
            "schema_url": "schema_url"
        }]);
        assert_eq!(
            instrumentation_library_spans_to_pb(Some(&ils)).ok(),
            Some(vec![InstrumentationLibrarySpans {
                instrumentation_library: None,
                spans: vec![],
                schema_url: "schema_url".to_string()
            }]),
        );
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)]
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
            span_to_pb(&span).ok(),
            Some(Span {
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
        );
    }

    #[test]
    fn minimal_span_events() {
        let span_events = literal!([{
            "time_unix_nano": 1,
            "name": "urknall"
        }]);
        assert_eq!(
            span_events_to_pb(Some(&span_events)).ok(),
            Some(vec![Event {
                time_unix_nano: 1,
                name: "urknall".to_string(),
                attributes: vec![],
                dropped_attributes_count: 0
            }]),
        );
    }
}
