// Copyright 2021, The Tremor Team
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

use std::{
    net::{Ipv4Addr, Ipv6Addr},
    str::FromStr,
    sync::Arc,
};

pub(crate) use clickhouse_rs::types::Value as CValue;
use either::Either;
use simd_json::{StaticNode, Value, ValueAccess};
use tremor_value::Value as TValue;
use uuid::Uuid;

use super::DummySqlType;
use crate::errors::{Error, ErrorKind, Result};

pub(super) fn convert_value(
    column_name: &str,
    value: &TValue,
    expected_type: &DummySqlType,
) -> Result<CValue> {
    match (value, expected_type) {
        (TValue::Static(value), _) => {
            if let (StaticNode::Null, DummySqlType::Nullable(inner_type)) = (value, expected_type) {
                // Null can be of any type, as long as it is allowed by the
                // schema.

                return Ok(CValue::Nullable(Either::Left(inner_type.as_ref().into())));
            }

            let value_as_non_null = match (value, expected_type.as_non_nullable()) {
                // These are the *obvious* translations. No cast is required,
                // Not much to say here.
                (StaticNode::U64(v), DummySqlType::UInt64) => CValue::UInt64(*v),
                (StaticNode::I64(v), DummySqlType::Int64) => CValue::Int64(*v),

                // Booleans can be converted to integers (true = 1, false = 0).
                (StaticNode::Bool(b), DummySqlType::UInt64) => CValue::UInt64(u64::from(*b)),
                (StaticNode::Bool(b), DummySqlType::Int64) => CValue::Int64(i64::from(*b)),

                (other, _) => {
                    return Err(Error::from(ErrorKind::UnexpectedEventFormat(
                        column_name.to_string(),
                        expected_type.to_string(),
                        other.value_type(),
                    )))
                }
            };

            Ok(expected_type.wrap_if_nullable(value_as_non_null))
        }

        // String -> String
        (TValue::String(string), DummySqlType::String) => {
            let content = string.as_bytes().to_vec();
            Ok(CValue::String(Arc::new(content)))
        }

        // String -> Ipv4 (parsed using std's Ipv4Addr::from_str implementation)
        (TValue::String(string), DummySqlType::IPv4) => Ipv4Addr::from_str(string.as_ref())
            .map(|addr| addr.octets())
            .map(CValue::Ipv4)
            .map_err(|_| Error::from(ErrorKind::MalformedIpAddr)),

        // String -> Ipv6 (parsed using std's Ipv6Addr::from_str implementation)
        (TValue::String(string), DummySqlType::IPv6) => Ipv6Addr::from_str(string.as_ref())
            .map(|addr| addr.octets())
            .map(CValue::Ipv6)
            .map_err(|_| Error::from(ErrorKind::MalformedIpAddr)),

        // String -> UUID (parsed using uuid's from_str implementation)
        (TValue::String(string), DummySqlType::UUID) => Uuid::from_str(string.as_ref())
            .map(Uuid::into_bytes)
            .map(CValue::Uuid)
            .map_err(|_| Error::from(ErrorKind::MalformedUuid)),

        // Array -> Array
        (TValue::Array(values), DummySqlType::Array(expected_inner_type)) => values
            .iter()
            .map(|value| convert_value(column_name, value, expected_inner_type))
            .collect::<Result<Vec<_>>>()
            .map(|converted_array| {
                CValue::Array(
                    expected_inner_type.as_ref().into(),
                    Arc::new(converted_array),
                )
            }),

        // Array -> IPv4
        (TValue::Array(values), DummySqlType::IPv4) => coerce_octet_sequence(values.as_slice())
            .map(CValue::Ipv4)
            .map_err(|()| Error::from(ErrorKind::MalformedIpAddr)),

        // Array -> IPv6
        (TValue::Array(values), DummySqlType::IPv6) => coerce_octet_sequence(values.as_slice())
            .map(CValue::Ipv6)
            .map_err(|()| Error::from(ErrorKind::MalformedIpAddr)),

        // Array -> UUID
        (TValue::Array(values), DummySqlType::UUID) => coerce_octet_sequence(values.as_slice())
            .map(CValue::Uuid)
            .map_err(|()| Error::from(ErrorKind::MalformedUuid)),

        // We don't support the Map datatype on the clickhouse side. There's a
        // PR for that, but it's not yet merged. As a result, we can't handle
        // Tremor Objects.
        // https://github.com/suharev7/clickhouse-rs/pull/170

        // TODO: I don't remember. Is there a way to store binary data in
        // clickhouse?
        (other, _) => Err(Error::from(ErrorKind::UnexpectedEventFormat(
            column_name.to_string(),
            expected_type.to_string(),
            other.value_type(),
        ))),
    }
}

fn coerce_octet_sequence<const N: usize>(values: &[TValue]) -> std::result::Result<[u8; N], ()> {
    values
        .iter()
        .map(|value| value.as_u8().ok_or(()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .and_then(|octets| <[u8; N]>::try_from(octets).map_err(drop))
}
