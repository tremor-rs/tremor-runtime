// Copyright 2020-2021, The T key: (), value: () key: (), value: ()remor Team
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

use crate::errors::Result;
use simd_json::StaticNode;
use tremor_value::Value;

pub(crate) fn maybe_string_to_pb<'event>(data: Option<&Value<'event>>) -> Result<String> {
    if let Some(Value::String(s)) = data {
        Ok(s.to_string())
    } else {
        Err("Expected a string to convert from json to pb".into())
    }
}

pub(crate) fn maybe_int_to_pbu64<'event>(data: Option<&Value<'event>>) -> Result<u64> {
    if let Some(&Value::Static(StaticNode::U64(i))) = data {
        Ok(i)
    } else if let Some(&Value::Static(StaticNode::I64(i))) = data {
        Ok(i as u64)
    } else {
        Err("Expected an json u64 to convert to pb u64".into())
    }
}

pub(crate) fn maybe_int_to_pbi32<'event>(data: Option<&Value<'event>>) -> Result<i32> {
    if let Some(&Value::Static(StaticNode::I64(i))) = data {
        Ok(i as i32)
    } else {
        Err("Expected an json i64 to convert to pb i32".into())
    }
}

pub(crate) fn maybe_int_to_pbi64<'event>(data: Option<&Value<'event>>) -> Result<i64> {
    if let Some(&Value::Static(StaticNode::I64(i))) = data {
        Ok(i)
    } else {
        Err("Expected an json i64 to convert to pb i64".into())
    }
}

pub(crate) fn maybe_int_to_pbu32<'event>(data: Option<&Value<'event>>) -> Result<u32> {
    if let Some(&Value::Static(StaticNode::I64(i))) = data {
        Ok(i as u32) // TODO check for v > u32 max
    } else {
        Err("Expected an i64 to convert to pb u32".into())
    }
}

pub(crate) fn maybe_double_to_pb<'event>(data: Option<&Value<'event>>) -> Result<f64> {
    if let Some(&Value::Static(StaticNode::F64(i))) = data {
        Ok(i)
    } else {
        Err("Expected a json f64 to convert to pb f64".into())
    }
}

pub(crate) fn maybe_bool_to_pb<'event>(data: Option<&Value<'event>>) -> Result<bool> {
    if let Some(&Value::Static(StaticNode::Bool(i))) = data {
        Ok(i)
    } else {
        Err("Expected a json bool to convert to pb bool".into())
    }
}

pub(crate) fn f64_repeated_to_pb<'event>(json: Option<&Value<'event>>) -> Result<Vec<f64>> {
    if let Some(Value::Array(json)) = json {
        let mut arr = Vec::new();
        for data in json {
            let value = maybe_double_to_pb(Some(data))?;
            arr.push(value);
        }
        return Ok(arr);
    }

    Err("Unable to map json value to repeated f64 pb".into())
}

pub(crate) fn u64_repeated_to_pb<'event>(json: Option<&Value<'event>>) -> Result<Vec<u64>> {
    if let Some(Value::Array(json)) = json {
        let mut arr = Vec::new();
        for data in json {
            let value = maybe_int_to_pbu64(Some(data))?;
            arr.push(value);
        }
        return Ok(arr);
    }

    Err("Unable to map json value to repeated u64 pb".into())
}
