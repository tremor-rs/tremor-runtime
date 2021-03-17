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

use super::common;
use crate::connectors::pb;
use crate::errors::Result;
use simd_json::json;
use tremor_otelapis::opentelemetry::proto::resource::v1::Resource;
use tremor_value::Value;

pub(crate) fn resource_to_json<'event>(pb: Option<Resource>) -> Result<Value<'event>> {
    if let Some(data) = pb {
        Ok(json!({
            "attributes": common::key_value_list_to_json(data.attributes)?,
            "dropped_attributes_count": data.dropped_attributes_count,
        })
        .into())
    } else {
        Ok(json!({ "attributes": [], "dropped_attributes_count": 0 }).into())
    }
}

pub(crate) fn maybe_resource_to_pb(json: Option<&Value<'_>>) -> Result<Resource> {
    if let Some(Value::Object(json)) = json {
        let dropped_attributes_count: u32 =
            pb::maybe_int_to_pbu32(json.get("dropped_attributes_count"))?;
        let attributes = common::maybe_key_value_list_to_pb(json.get("attributes"))?;
        let pb = Resource {
            attributes,
            dropped_attributes_count,
        };
        return Ok(pb);
    }
    Err("Invalid json mapping for Resource".into())
}

#[cfg(test)]
mod tests {
    use tremor_otelapis::opentelemetry::proto::common::v1::{any_value, AnyValue, KeyValue};

    use super::*;

    #[test]
    fn resource() -> Result<()> {
        let pb = Resource {
            attributes: vec![KeyValue {
                key: "snot".into(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("badger".into())),
                }),
            }],
            dropped_attributes_count: 9,
        };
        let json = resource_to_json(Some(pb.clone()))?;
        let back_again = maybe_resource_to_pb(Some(&json))?;
        let expected: Value = json!({
            "attributes": [ { "key": "snot", "value": "badger" }],
            "dropped_attributes_count": 9
        })
        .into();

        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        Ok(())
    }
}
