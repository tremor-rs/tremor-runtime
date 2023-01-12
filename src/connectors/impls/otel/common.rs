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

use crate::connectors::utils::{pb, url};
use crate::errors::Result;
use beef::Cow;
use halfbrown::HashMap;
use simd_json::Builder;
use tremor_otelapis::opentelemetry::proto::common::v1::{
    any_value, AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList,
};
use tremor_value::{literal, StaticNode, Value};
use value_trait::ValueAccess;

pub(crate) struct OtelDefaults;
impl url::Defaults for OtelDefaults {
    // We do add the port here since it's different from http's default

    const SCHEME: &'static str = "https";
    const HOST: &'static str = "localhost";
    const PORT: u16 = 4317;
}

/// Enum to support configurable compression on otel grpc client/servers.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Compression {
    #[default]
    None,
    Gzip,
}

pub(crate) const EMPTY: Vec<Value> = Vec::new();

pub(crate) fn any_value_to_json(pb: AnyValue) -> Value<'static> {
    use any_value::Value as Inner;
    let v: Value = match pb.value {
        Some(Inner::StringValue(v)) => v.into(),
        Some(Inner::BoolValue(v)) => v.into(),
        Some(Inner::IntValue(v)) => v.into(),
        Some(Inner::DoubleValue(v)) => v.into(),
        Some(Inner::ArrayValue(v)) => v.values.into_iter().map(any_value_to_json).collect(),
        Some(Inner::KvlistValue(v)) => {
            v.values
                .into_iter()
                .map(|e| {
                    (
                        e.key,
                        e.value.map(any_value_to_json).unwrap_or_default(), // TODO check conformance - not sure this is correct at all
                    )
                })
                .collect()
        }
        Some(Inner::BytesValue(b)) => Value::Bytes(b.into()),
        None => Value::null(),
    };
    v
}

pub(crate) fn any_value_to_pb(data: &Value<'_>) -> AnyValue {
    use any_value::Value as Inner;
    match data {
        Value::Static(StaticNode::Null) => AnyValue { value: None },
        Value::Static(StaticNode::Bool(v)) => AnyValue {
            value: Some(Inner::BoolValue(*v)),
        },
        Value::Static(StaticNode::I64(v)) => AnyValue {
            value: Some(Inner::IntValue(*v)),
        },
        Value::Static(StaticNode::U64(v)) => {
            #[allow(clippy::cast_sign_loss, clippy::cast_possible_wrap)]
            let v = *v as i64;

            AnyValue {
                value: Some(Inner::IntValue(v)),
            }
        }
        Value::Static(StaticNode::F64(v)) => AnyValue {
            value: Some(Inner::DoubleValue(*v)),
        },
        Value::String(v) => AnyValue {
            value: Some(Inner::StringValue(v.to_string())),
        },
        Value::Array(va) => {
            let a: Vec<AnyValue> = va.iter().map(any_value_to_pb).collect();
            let x = Inner::ArrayValue(ArrayValue { values: a });
            AnyValue { value: Some(x) }
        }
        Value::Object(vo) => {
            let mut kvl = Vec::with_capacity(vo.len());
            for (k, v) in vo.iter() {
                kvl.push(KeyValue {
                    key: k.to_string(),
                    value: Some(any_value_to_pb(v)),
                });
            }
            let pb = KeyValueList { values: kvl };
            AnyValue {
                value: Some(Inner::KvlistValue(pb)),
            }
        }
        Value::Bytes(b) => AnyValue {
            value: Some(Inner::BytesValue(b.to_vec())),
        },
    }
}

pub(crate) fn maybe_instrumentation_scope_to_json(
    pb: Option<InstrumentationScope>,
) -> Option<Value<'static>> {
    if let Some(pb) = pb {
        return Some(literal!({
            "name": pb.name.clone(),
            "version": pb.version.clone(),
            "attributes": key_value_list_to_json(pb.attributes),
            "dropped_attributes_count": 0
        }));
    }

    None
}

pub(crate) fn maybe_instrumentation_scope_to_pb(
    data: Option<&Value<'_>>,
) -> Result<Option<InstrumentationScope>> {
    if let Some(data) = data {
        let name = data
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let version = data
            .get("version")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let attributes = maybe_key_value_list_to_pb(data.get("attributes"))?;
        let dropped_attributes_count =
            pb::maybe_int_to_pbu32(data.get("dropped_attributes_count")).unwrap_or_default();
        Ok(Some(InstrumentationScope {
            name,
            version,
            attributes,
            dropped_attributes_count,
        }))
    } else {
        Ok(None)
    }
}

pub(crate) fn maybe_any_value_to_json(pb: Option<AnyValue>) -> Option<Value<'static>> {
    pb.map(any_value_to_json)
}

pub(crate) fn string_key_value_to_json(pb: Vec<KeyValue>) -> Value<'static> {
    // REFACTOR whack as string_key_value was deprecated-removed
    pb.into_iter()
        .map(|KeyValue { key, value }| (key, maybe_any_value_to_json(value)))
        .collect()
}

pub(crate) fn string_key_value_to_pb(data: Option<&Value<'_>>) -> Result<Vec<KeyValue>> {
    // REFACTOR whack as string_key_value was deprecated-removed
    data.as_object()
        .ok_or("Unable to map json to Vec<StringKeyValue> pb")?
        .iter()
        .map(|(key, value)| {
            let key = key.to_string();
            let value = any_value_to_pb(value);
            Ok(KeyValue {
                key,
                value: Some(value),
            })
        })
        .collect()
}

pub(crate) fn key_value_list_to_json(pb: Vec<KeyValue>) -> Value<'static> {
    let kv: HashMap<Cow<str>, Value<'_>> = pb
        .into_iter()
        .filter_map(|e| match e {
            KeyValue {
                key,
                value: Some(value),
            } => {
                let k = Cow::owned(key);
                Some((k, any_value_to_json(value)))
            }
            KeyValue { .. } => None,
        })
        .collect();
    Value::Object(Box::new(kv))
}

pub(crate) fn maybe_key_value_list_to_pb(data: Option<&Value<'_>>) -> Result<Vec<KeyValue>> {
    let obj = data
        .as_object()
        .ok_or("Expected a json object, found otherwise - cannot map to pb")?;
    Ok(obj_key_value_list_to_pb(obj))
}

pub(crate) fn get_attributes(data: &Value) -> Result<Vec<KeyValue>> {
    match (data.get_object("attributes"), data.get_object("labels")) {
        (None, None) => Err("missing field `attributes`".into()),
        (Some(a), None) | (None, Some(a)) => Ok(obj_key_value_list_to_pb(a)),
        (Some(a), Some(l)) => {
            let mut a = obj_key_value_list_to_pb(a);
            a.append(&mut obj_key_value_list_to_pb(l));
            Ok(a)
        }
    }
}

pub(crate) fn obj_key_value_list_to_pb(data: &tremor_value::Object<'_>) -> Vec<KeyValue> {
    data.iter()
        .map(|(key, value)| {
            let key = key.to_string();
            let value = Some(any_value_to_pb(value));
            KeyValue { key, value }
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::unnecessary_wraps)]
mod tests {
    #![allow(clippy::float_cmp)]
    use simd_json::prelude::*;
    use tremor_otelapis::opentelemetry::proto::common::v1::{ArrayValue, KeyValueList};
    use tremor_value::literal;

    use super::*;

    #[test]
    fn any_value_none() {
        let pb = AnyValue { value: None };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        assert!(json.is_null());
        assert_eq!(pb, back_again);
    }

    #[test]
    fn any_value_string() {
        let pb = AnyValue {
            value: Some(any_value::Value::StringValue("snot".into())),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        assert_eq!(json, "snot");
        assert_eq!(pb, back_again);

        let pb = AnyValue {
            value: Some(any_value::Value::StringValue(String::new())),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        assert_eq!(json, "");
        assert_eq!(pb, back_again);
    }

    #[test]
    fn any_value_bool() {
        let pb = AnyValue {
            value: Some(any_value::Value::BoolValue(true)),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        assert_eq!(json, true);
        assert_eq!(pb, back_again);

        let pb = AnyValue {
            value: Some(any_value::Value::BoolValue(false)),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        assert_eq!(json, false);
        assert_eq!(pb, back_again);
    }

    #[test]
    fn any_value_int() {
        let pb = AnyValue {
            value: Some(any_value::Value::IntValue(0i64)),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        assert_eq!(json, 0);
        assert_eq!(pb, back_again);

        let pb = AnyValue {
            value: Some(any_value::Value::IntValue(std::i64::MAX)),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        assert_eq!(json, std::i64::MAX);
        assert_eq!(pb, back_again);

        let pb = AnyValue {
            value: Some(any_value::Value::IntValue(std::i64::MIN)),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        assert_eq!(json, std::i64::MIN);
        assert_eq!(pb, back_again);
    }

    #[test]
    fn any_value_double() {
        let pb = AnyValue {
            value: Some(any_value::Value::DoubleValue(0f64)),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        assert_eq!(json, 0f64);
        assert_eq!(pb, back_again);

        let pb = AnyValue {
            value: Some(any_value::Value::DoubleValue(std::f64::MAX)),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        assert_eq!(json, std::f64::MAX);
        assert_eq!(pb, back_again);

        let pb = AnyValue {
            value: Some(any_value::Value::DoubleValue(std::f64::MIN)),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        assert_eq!(json, std::f64::MIN);
        assert_eq!(pb, back_again);
    }

    #[test]
    fn any_value_list() {
        let snot = AnyValue {
            value: Some(any_value::Value::StringValue("snot".into())),
        };
        let pb = AnyValue {
            value: Some(any_value::Value::ArrayValue(ArrayValue {
                values: vec![snot],
            })),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        let expected: Value = literal!(["snot"]);
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        let pb = AnyValue {
            value: Some(any_value::Value::ArrayValue(ArrayValue { values: vec![] })),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        let expected: Value = literal!([]);
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);
    }

    #[test]
    fn any_value_record() {
        let snot = AnyValue {
            value: Some(any_value::Value::StringValue("snot".into())),
        };
        let pb = AnyValue {
            value: Some(any_value::Value::KvlistValue(KeyValueList {
                values: vec![KeyValue {
                    key: "badger".into(),
                    value: Some(snot),
                }],
            })),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        let expected: Value = literal!({"badger": "snot"});
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        let pb = AnyValue {
            value: Some(any_value::Value::KvlistValue(KeyValueList {
                values: vec![],
            })),
        };
        let json = any_value_to_json(pb.clone());
        let back_again = any_value_to_pb(&json);
        let expected: Value = literal!({});
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);
    }

    #[test]
    fn key_value_list() -> Result<()> {
        let snot = AnyValue {
            value: Some(any_value::Value::StringValue("snot".into())),
        };
        let pb = vec![KeyValue {
            key: "snot".into(),
            value: Some(snot),
        }];
        let json = key_value_list_to_json(pb.clone());
        let back_again = maybe_key_value_list_to_pb(Some(&json))?;
        let expected: Value = literal!({"snot": "snot"});
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);
        Ok(())
    }

    #[test]
    fn string_key_value_list() -> Result<()> {
        let pb = vec![KeyValue {
            key: "snot".into(),
            value: Some(any_value_to_pb(&literal!("badger"))),
        }];
        let json = string_key_value_to_json(pb.clone());
        let back_again = string_key_value_to_pb(Some(&json))?;
        let expected: Value = literal!({"snot": "badger"});
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);
        Ok(())
    }

    #[test]
    fn instrumentation_scope() -> Result<()> {
        use any_value::Value as Inner;
        let pb = InstrumentationScope {
            name: "name".into(),
            version: "v0.1.2".into(),
            attributes: vec![KeyValue {
                key: "snot".to_string(),
                value: Some(AnyValue {
                    value: Some(Inner::StringValue("badger".to_string())),
                }),
            }],
            dropped_attributes_count: 0,
        };
        let json = maybe_instrumentation_scope_to_json(Some(pb.clone()));
        let back_again = maybe_instrumentation_scope_to_pb(json.as_ref())?;
        let expected: Value = literal!({"name": "name", "version": "v0.1.2", "attributes": { "snot": "badger" }, "dropped_attributes_count": 0});
        assert_eq!(Some(expected), json);
        assert_eq!(Some(pb), back_again);
        Ok(())
    }
}
