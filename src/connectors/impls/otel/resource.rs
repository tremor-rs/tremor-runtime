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

use super::common;
use crate::connectors::prelude::*;
use crate::connectors::utils::pb;
use tremor_otelapis::opentelemetry::proto::resource::v1::Resource;

#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum Error {
    #[error("Unable to map json value to Resource pb")]
    InvalidResource,
}
pub(crate) fn resource_to_json(pb: Resource) -> Value<'static> {
    literal!({
        "attributes": common::key_value_list_to_json(pb.attributes),
        "dropped_attributes_count": pb.dropped_attributes_count,
    })
}

pub(crate) fn resource_to_pb(json: &Value<'_>) -> Result<Resource> {
    let json = json.as_object().ok_or(Error::InvalidResource)?;
    Ok(Resource {
        dropped_attributes_count: pb::maybe_int_to_pbu32(json.get("dropped_attributes_count"))
            .unwrap_or_default(),
        attributes: common::maybe_key_value_list_to_pb(json.get("attributes"))?,
    })
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
        let json = resource_to_json(pb.clone());
        let back_again = resource_to_pb(&json)?;
        let expected: Value = literal!({
            "attributes": { "snot": "badger" },
            "dropped_attributes_count": 9
        });

        assert_eq!(expected, json);
        assert_eq!(pb, back_again);

        Ok(())
    }
}
