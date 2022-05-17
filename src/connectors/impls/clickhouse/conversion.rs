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

pub(super) use clickhouse_rs::types::Value as CValue;
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
    if let (TValue::Static(StaticNode::Null), DummySqlType::Nullable(inner_type)) =
        (value, expected_type)
    {
        // Null can be of any type, as long as it is allowed by the
        // schema.
        return Ok(CValue::Nullable(Either::Left(inner_type.as_ref().into())));
    }

    match (value, expected_type.as_non_nullable()) {
        (TValue::Static(value), inner_type) => {
            match (value, inner_type) {
                // These are the *obvious* translations. No cast is required,
                // Not much to say here.
                (StaticNode::U64(v), DummySqlType::UInt64) => Ok(CValue::UInt64(*v)),
                (StaticNode::I64(v), DummySqlType::Int64) => Ok(CValue::Int64(*v)),

                // Booleans can be converted to integers (true = 1, false = 0).
                (StaticNode::Bool(b), DummySqlType::UInt64) => Ok(CValue::UInt64(u64::from(*b))),
                (StaticNode::Bool(b), DummySqlType::Int64) => Ok(CValue::Int64(i64::from(*b))),

                (other, _) => Err(Error::from(ErrorKind::UnexpectedEventFormat(
                    column_name.to_string(),
                    expected_type.to_string(),
                    other.value_type(),
                ))),
            }
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
        (other, _) => Err(Error::from(ErrorKind::UnexpectedEventFormat(
            column_name.to_string(),
            expected_type.to_string(),
            other.value_type(),
        ))),
    }
    .map(|value| expected_type.wrap_if_nullable(value))
}

fn coerce_octet_sequence<const N: usize>(values: &[TValue]) -> std::result::Result<[u8; N], ()> {
    values
        .iter()
        .map(|value| value.as_u8().ok_or(()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .and_then(|octets| <[u8; N]>::try_from(octets).map_err(drop))
}

#[cfg(test)]
mod tests {
    use super::*;

    use clickhouse_rs::types::SqlType;
    use tremor_value::{json, StaticNode};

    // A simple macro which will help us to test the conversion stuff in this
    // module.
    macro_rules! test_value_conversion {
        ($test_name:ident {
            $input:expr, $ty:expr => $output:expr $(,)?
        }) => {
            #[test]
            fn $test_name() {
                let input_value = TValue::from($input);
                let output_type: DummySqlType = $ty;
                let column_name = "test_column_name";

                let left = convert_value(column_name, &input_value, &output_type).unwrap();
                let right = $output;

                assert_eq!(left, right);
            }
        };
    }

    test_value_conversion! {
        u64_conversion {
            json! { 42u64 }, DummySqlType::UInt64 => CValue::UInt64(42),
        }
    }

    test_value_conversion! {
        i64_conversion {
            json! { 101i64 }, DummySqlType::Int64 => CValue::Int64(101),
        }
    }

    test_value_conversion! {
        string_conversion {
            json! { "foo" }, DummySqlType::String => clickhouse_string_value("foo"),
        }
    }

    test_value_conversion! {
        array_conversion_1 {
            json! { [ "foo" ]}, DummySqlType::Array(Box::new(DummySqlType::String))
            =>
            clickhouse_array_value(
                SqlType::String,
                [clickhouse_string_value("foo")]
            )
        }
    }

    test_value_conversion! {
        array_conversion_2 {
            json! { [ "foo", "bar", "baz" ]},
            DummySqlType::Array(Box::new(DummySqlType::String))
            =>
            clickhouse_array_value(
                SqlType::String,
                [clickhouse_string_value("foo"), clickhouse_string_value("bar"), clickhouse_string_value("baz")]
            )
        }
    }

    test_value_conversion! {
        nullable_null_1 {
            StaticNode::Null, DummySqlType::Nullable(Box::new(DummySqlType::String))
            =>
            CValue::Nullable(Either::Left(SqlType::String.into()))
        }
    }

    test_value_conversion! {
        nullable_null_2 {
            StaticNode::Null, DummySqlType::Nullable(Box::new(DummySqlType::UInt64))
            =>
            CValue::Nullable(Either::Left(SqlType::UInt64.into()))
        }
    }

    test_value_conversion! {
        nullable_non_null_1 {
            json! { 101u64 }, DummySqlType::Nullable(Box::new(DummySqlType::UInt64))
            =>
            CValue::Nullable(Either::Right(Box::new(CValue::UInt64(101))))
        }
    }

    test_value_conversion! {
        nullable_non_null_2 {
            json! { "tira-misu" }, DummySqlType::Nullable(Box::new(DummySqlType::String))
            =>
            CValue::Nullable(Either::Right(Box::new(clickhouse_string_value("tira-misu"))))
        }
    }

    test_value_conversion! {
        ipv4_from_string {
            json! { "127.0.0.1" }, DummySqlType::IPv4 => CValue::Ipv4([127, 0, 0, 1]),
        }
    }

    test_value_conversion! {
        ipv4_from_array {
            json! { [1, 2, 3, 4] }, DummySqlType::IPv4 => CValue::Ipv4([1, 2, 3, 4]),
        }
    }

    test_value_conversion! {
        ipv6_from_string {
            json! { "0000:0000:0000:0000:0000:0000:0000:0001" },
            DummySqlType::IPv6
            =>
            CValue::Ipv6([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
        }
    }

    test_value_conversion! {
        ipv6_from_array {
            json! {
                [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
            },
            DummySqlType::IPv6
            =>
            CValue::Ipv6([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
        }
    }

    test_value_conversion! {
        uuid_from_string {
            json! {
                "123e4567-e89b-12d3-a456-426614174000"
            },
            DummySqlType::UUID
            =>
            CValue::Uuid([0x12,
                0x3e,
                0x45,
                0x67,
                0xe8,
                0x9b,
                0x12,
                0xd3,
                0xa4,
                0x56,
                0x42,
                0x66,
                0x14,
                0x17,
                0x40,
                0x00
            ])
        }
    }

    fn clickhouse_string_value(input: &str) -> CValue {
        CValue::String(Arc::new(input.to_string().into_bytes()))
    }

    fn clickhouse_array_value<const N: usize>(ty: SqlType, values: [CValue; N]) -> CValue {
        CValue::Array(ty.into(), Arc::new(values.to_vec()))
    }
}
