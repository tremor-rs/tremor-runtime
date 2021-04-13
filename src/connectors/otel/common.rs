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

use crate::connectors::pb;
use crate::errors::Result;
use halfbrown::HashMap;
use tremor_otelapis::opentelemetry::proto::common::v1::{
    any_value, AnyValue, ArrayValue, InstrumentationLibrary, KeyValue, KeyValueList, StringKeyValue,
};
use tremor_value::StaticNode;

use tremor_value::{literal, Value};
use value_trait::ValueAccess;

pub(crate) fn any_value_to_json<'event>(
    pb: tremor_otelapis::opentelemetry::proto::common::v1::AnyValue,
) -> Result<Value<'event>> {
    use any_value::Value as Inner;
    let v: Value = match pb.value {
        Some(Inner::StringValue(v)) => v.into(),
        Some(Inner::BoolValue(v)) => v.into(),
        Some(Inner::IntValue(v)) => v.into(),
        Some(Inner::DoubleValue(v)) => v.into(),
        Some(Inner::ArrayValue(v)) => v
            .values
            .into_iter()
            .map(|v| any_value_to_json(v).ok())
            .collect(),
        Some(Inner::KvlistValue(v)) => {
            let mut record = HashMap::with_capacity(v.values.len());
            for e in v.values {
                record.insert(
                    e.key.into(),
                    match e.value {
                        Some(v) => any_value_to_json(v)?,
                        None => Value::Static(StaticNode::Null), // TODO check conformance - not sure this is correct at all
                    },
                );
            }
            Value::Object(Box::new(record))
        }
        None => Value::Static(StaticNode::Null),
    };
    Ok(v)
}

pub(crate) fn any_value_to_pb(data: &Value<'_>) -> AnyValue {
    use any_value::Value as PbAnyValue;
    match data {
        Value::Static(StaticNode::Null) => AnyValue { value: None },
        Value::Static(StaticNode::Bool(v)) => AnyValue {
            value: Some(PbAnyValue::BoolValue(*v)),
        },
        Value::Static(StaticNode::I64(v)) => AnyValue {
            value: Some(PbAnyValue::IntValue(*v)),
        },
        Value::Static(StaticNode::U64(v)) => {
            #[allow(clippy::cast_sign_loss, clippy::cast_possible_wrap)]
            let v = *v as i64;

            AnyValue {
                value: Some(PbAnyValue::IntValue(v)),
            }
        }
        Value::Static(StaticNode::F64(v)) => AnyValue {
            value: Some(PbAnyValue::DoubleValue(*v)),
        },
        Value::String(v) => AnyValue {
            value: Some(PbAnyValue::StringValue(v.to_string())),
        },
        Value::Array(va) => {
            let a: Vec<AnyValue> = va.iter().map(|v| any_value_to_pb(v)).collect();
            let x = PbAnyValue::ArrayValue(ArrayValue { values: a });
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
                value: Some(PbAnyValue::KvlistValue(pb)),
            }
        }
        Value::Bytes(b) => {
            // TODO find a better binary mapping - the below is clearly not right!
            let b: Vec<AnyValue> = b
                .iter()
                .map(|b| AnyValue {
                    value: Some(PbAnyValue::IntValue(i64::from(*b))),
                })
                .collect();
            let x = PbAnyValue::ArrayValue(ArrayValue { values: b });
            AnyValue { value: Some(x) }
        }
    }
}

pub(crate) fn maybe_any_value_to_pb(data: Option<&Value<'_>>) -> Result<AnyValue> {
    data.map_or_else(
        || Err("Unable to map to pb otel any_value".into()),
        |x| Ok(any_value_to_pb(x)),
    )
}

pub(crate) fn maybe_any_value_to_json<'event>(
    pb: Option<AnyValue>,
) -> Result<Option<Value<'event>>> {
    match pb {
        Some(any) => Ok(Some(any_value_to_json(any)?)),
        None => Ok(None),
    }
}

pub(crate) fn string_key_value_to_json<'event>(pb: Vec<StringKeyValue>) -> Value<'event> {
    let mut json = HashMap::with_capacity(pb.len());
    for kv in pb {
        json.insert(kv.key.into(), kv.value.into());
    }

    Value::Object(Box::new(json))
}

pub(crate) fn string_key_value_to_pb(data: Option<&Value<'_>>) -> Result<Vec<StringKeyValue>> {
    if let Some(Value::Object(data)) = data {
        let mut pb = Vec::with_capacity(data.len());
        for (key, value) in data.iter() {
            let key: String = key.to_string();
            let value: String = pb::maybe_string_to_pb(Some(value))?;
            pb.push(StringKeyValue { key, value });
        }
        return Ok(pb);
    }

    Err("Unable to map json to Vec<StringKeyValue> pb".into())
}

pub(crate) fn key_value_list_to_json<'event>(pb: Vec<KeyValue>) -> Result<Value<'event>> {
    let mut json: HashMap<beef::Cow<str>, Value> = HashMap::with_capacity(pb.len());
    for kv in pb {
        if let Some(v) = maybe_any_value_to_json(kv.value)? {
            json.insert(kv.key.into(), v);
        } else {
            json.insert(kv.key.into(), Value::Static(StaticNode::Null));
        }
    }

    Ok(Value::Object(Box::new(json)))
}

pub(crate) fn maybe_key_value_list_to_pb(data: Option<&Value<'_>>) -> Result<Vec<KeyValue>> {
    if let Some(Value::Object(data)) = data {
        let mut pb: Vec<KeyValue> = Vec::with_capacity(data.len());
        for (key, value) in data.iter() {
            let key = key.to_string();
            let value = Some(maybe_any_value_to_pb(Some(&value))?);
            pb.push(KeyValue { key, value })
        }
        return Ok(pb);
    }
    Err("Expected a json object, found otherwise - cannot map to pb".into())
}

pub(crate) fn maybe_instrumentation_library_to_json<'event>(
    pb: Option<InstrumentationLibrary>,
) -> Value<'event> {
    match pb {
        None => Value::Static(StaticNode::Null),
        // TODO This is going to be pretty slow going from Owned -> Value - consider json! for borrowed
        Some(il) => literal!({
            "name": il.name,
            "version": il.version,
        }),
    }
}

pub(crate) fn maybe_instrumentation_library_to_pb(
    data: Option<&Value<'_>>,
) -> Result<Option<InstrumentationLibrary>> {
    match data {
        Some(data) => Ok(Some(InstrumentationLibrary {
            name: pb::maybe_string_to_pb((*data).get("name"))?,
            version: pb::maybe_string_to_pb((*data).get("version"))?,
        })),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use tremor_otelapis::opentelemetry::proto::common::v1::{ArrayValue, KeyValueList};

    use super::*;

    #[test]
    fn any_value_none() -> Result<()> {
        let pb = AnyValue { value: None };
        let json = any_value_to_json(pb.clone())?;
        let back_again = any_value_to_pb(&json);
        assert_eq!(Value::Static(StaticNode::Null), json);
        assert_eq!(pb, back_again);

        let pb = AnyValue { value: None };
        let json = any_value_to_json(pb.clone())?;
        let back_again = maybe_any_value_to_pb(Some(&json))?;
        assert_eq!(Value::Static(StaticNode::Null), json);
        assert_eq!(pb, back_again);

        Ok(())
    }

    #[test]
    fn any_value_string() -> Result<()> {
        let pb = AnyValue {
            value: Some(any_value::Value::StringValue("snot".into())),
        };
        let json = any_value_to_json(pb.clone())?;
        let back_again = any_value_to_pb(&json);
        assert_eq!(Value::String("snot".into()), json);
        assert_eq!(pb, back_again);

        let pb = AnyValue {
            value: Some(any_value::Value::StringValue("".into())),
        };
        let json = any_value_to_json(pb.clone())?;
        let back_again = any_value_to_pb(&json);
        assert_eq!(Value::String("".into()), json);
        assert_eq!(pb, back_again);

        Ok(())
    }

    #[test]
    fn any_value_bool() -> Result<()> {
        let pb = AnyValue {
            value: Some(any_value::Value::BoolValue(true)),
        };
        let json = any_value_to_json(pb.clone())?;
        let back_again = any_value_to_pb(&json);
        assert_eq!(Value::Static(StaticNode::Bool(true)), json);
        assert_eq!(pb, back_again);

        let pb = AnyValue {
            value: Some(any_value::Value::BoolValue(false)),
        };
        let json = any_value_to_json(pb.clone())?;
        let back_again = any_value_to_pb(&json);
        assert_eq!(Value::Static(StaticNode::Bool(false)), json);
        assert_eq!(pb, back_again);

        Ok(())
    }

    #[test]
    fn any_value_int() -> Result<()> {
        let pb = AnyValue {
            value: Some(any_value::Value::IntValue(0i64)),
        };
        let json = any_value_to_json(pb.clone())?;
        let back_again = any_value_to_pb(&json);
        assert_eq!(Value::Static(StaticNode::I64(0)), json);
        assert_eq!(pb, back_again);

        let pb = AnyValue {
            value: Some(any_value::Value::IntValue(std::i64::MAX)),
        };
        let json = any_value_to_json(pb.clone())?;
        let back_again = any_value_to_pb(&json);
        assert_eq!(Value::Static(StaticNode::I64(std::i64::MAX)), json);
        assert_eq!(pb, back_again);

        let pb = AnyValue {
            value: Some(any_value::Value::IntValue(std::i64::MIN)),
        };
        let json = any_value_to_json(pb.clone())?;
        let back_again = any_value_to_pb(&json);
        assert_eq!(Value::Static(StaticNode::I64(std::i64::MIN)), json);
        assert_eq!(pb, back_again);

        Ok(())
    }

    #[test]
    fn any_value_double() -> Result<()> {
        let pb = AnyValue {
            value: Some(any_value::Value::DoubleValue(0f64)),
        };
        let json = any_value_to_json(pb.clone())?;
        let back_again = any_value_to_pb(&json);
        assert_eq!(Value::Static(StaticNode::F64(0f64)), json);
        assert_eq!(pb, back_again);

        let pb = AnyValue {
            value: Some(any_value::Value::DoubleValue(std::f64::MAX)),
        };
        let json = any_value_to_json(pb.clone())?;
        let back_again = any_value_to_pb(&json);
        assert_eq!(Value::Static(StaticNode::F64(std::f64::MAX)), json);
        assert_eq!(pb, back_again);

        let pb = AnyValue {
            value: Some(any_value::Value::DoubleValue(std::f64::MIN)),
        };
        let json = any_value_to_json(pb.clone())?;
        let back_again = any_value_to_pb(&json);
        assert_eq!(Value::Static(StaticNode::F64(std::f64::MIN)), json);
        assert_eq!(pb, back_again);

        Ok(())
    }

    #[test]
    fn any_value_list() -> Result<()> {
        let snot = AnyValue {
            value: Some(any_value::Value::StringValue("snot".into())),
        };
        let pb = AnyValue {
            value: Some(any_value::Value::ArrayValue(ArrayValue {
                values: vec![snot],
            })),
        };
        let json = any_value_to_json(pb.clone())?;
        let back_again = any_value_to_pb(&json);
        let expected: Value = literal!(["snot"]);
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        let pb = AnyValue {
            value: Some(any_value::Value::ArrayValue(ArrayValue { values: vec![] })),
        };
        let json = any_value_to_json(pb.clone())?;
        let back_again = any_value_to_pb(&json);
        let expected: Value = literal!([]);
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);
        Ok(())
    }

    #[test]
    fn any_value_record() -> Result<()> {
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
        let json = any_value_to_json(pb.clone())?;
        let back_again = any_value_to_pb(&json);
        let expected: Value = literal!({"badger": "snot"});
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        let pb = AnyValue {
            value: Some(any_value::Value::KvlistValue(KeyValueList {
                values: vec![],
            })),
        };
        let json = any_value_to_json(pb.clone())?;
        let back_again = any_value_to_pb(&json);
        let expected: Value = literal!({});
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        Ok(())
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
        let json = key_value_list_to_json(pb.clone())?;
        let back_again = maybe_key_value_list_to_pb(Some(&json))?;
        let expected: Value = literal!({"snot": "snot"});
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);
        Ok(())
    }

    #[test]
    fn string_key_value_list() -> Result<()> {
        let pb = vec![StringKeyValue {
            key: "snot".into(),
            value: "badger".into(),
        }];
        let json = string_key_value_to_json(pb.clone());
        let back_again = string_key_value_to_pb(Some(&json))?;
        let expected: Value = literal!({"snot": "badger"});
        assert_eq!(expected, json);
        assert_eq!(pb, back_again);
        Ok(())
    }

    #[test]
    fn instrumentation_library() -> Result<()> {
        let pb = InstrumentationLibrary {
            name: "name".into(),
            version: "v0.1.2".into(),
        };
        let json = maybe_instrumentation_library_to_json(Some(pb.clone()));
        let back_again = maybe_instrumentation_library_to_pb(Some(&json))?;
        let expected: Value = literal!({"name": "name", "version": "v0.1.2"});
        assert_eq!(expected, json);
        assert_eq!(Some(pb), back_again);
        Ok(())
    }
}
