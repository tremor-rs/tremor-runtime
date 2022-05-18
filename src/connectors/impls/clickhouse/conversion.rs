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

use chrono_tz::Tz;
pub(super) use clickhouse_rs::types::Value as CValue;
use either::Either;
use simd_json::{StaticNode, Value, ValueAccess};
use tremor_value::Value as TValue;
use uuid::Uuid;

use super::DummySqlType;
use crate::errors::{Error, ErrorKind, Result};

const UTC: Tz = Tz::UTC;

pub(super) fn convert_value(
    column_name: &str,
    value: &TValue,
    expected_type: &DummySqlType,
) -> Result<CValue> {
    match expected_type {
        DummySqlType::Array(expected_inner_type) => {
            // We don't check that all elements of the array have the same type.
            // Instead, we check that every element can be converted to the expected
            // array type.
            wrap_getter_error(column_name, value, ValueAccess::as_array, expected_type)?
                .iter()
                .map(|value| convert_value(column_name, value, expected_inner_type))
                .collect::<Result<Vec<_>>>()
                .map(|converted_array| {
                    CValue::Array(
                        expected_inner_type.as_ref().into(),
                        Arc::new(converted_array),
                    )
                })
        }

        DummySqlType::Nullable(expected_inner_type) if value.is_null() => {
            // Null value (when we expect it).
            Ok(CValue::Nullable(Either::Left(
                expected_inner_type.as_ref().into(),
            )))
        }

        DummySqlType::Nullable(expected_inner_type) => {
            // TODO: we don't want to `?` here because the expected type it
            // provides is incorrect.
            let inner_value = convert_value(column_name, value, expected_inner_type)?;
            Ok(CValue::Nullable(Either::Right(Box::new(inner_value))))
        }

        DummySqlType::Int64 => get_and_wrap(
            column_name,
            value,
            ValueAccess::as_i64,
            CValue::Int64,
            expected_type,
        ),

        DummySqlType::UInt64 => get_and_wrap(
            column_name,
            value,
            |value| value.as_u64(),
            CValue::UInt64,
            expected_type,
        ),

        DummySqlType::String => get_and_wrap(
            column_name,
            value,
            ValueAccess::as_str,
            |s| CValue::String(Arc::new(s.as_bytes().to_vec())),
            expected_type,
        ),

        // TODO: there's quite much duplication between Ipv4, Ipv6 and Uuid:
        // they can all be created either from a string or from a sequence of
        // u8. It could be a good idea to merge them.
        DummySqlType::Ipv4 => {
            if let Some(octets) = value.as_array() {
                // Array of values -> Ipv4
                coerce_octet_sequence(octets.as_slice())
                    .map(CValue::Ipv4)
                    .map_err(|()| Error::from(ErrorKind::MalformedIpAddr))
            } else if let Some(string) = value.as_str() {
                // Conversion from String
                Ipv4Addr::from_str(string.as_ref())
                    .map(|addr| addr.octets())
                    .map(CValue::Ipv4)
                    .map_err(|_| Error::from(ErrorKind::MalformedIpAddr))
            } else {
                Err(Error::from(ErrorKind::UnexpectedEventFormat(
                    column_name.to_string(),
                    expected_type.to_string(),
                    value.value_type(),
                )))
            }
        }

        DummySqlType::Ipv6 => {
            if let Some(octets) = value.as_array() {
                // Array of values -> Ipv6
                coerce_octet_sequence(octets.as_slice())
                    .map(CValue::Ipv6)
                    .map_err(|()| Error::from(ErrorKind::MalformedIpAddr))
            } else if let Some(string) = value.as_str() {
                // Conversion from String
                Ipv6Addr::from_str(string.as_ref())
                    .map(|addr| addr.octets())
                    .map(CValue::Ipv6)
                    .map_err(|_| Error::from(ErrorKind::MalformedIpAddr))
            } else {
                Err(Error::from(ErrorKind::UnexpectedEventFormat(
                    column_name.to_string(),
                    expected_type.to_string(),
                    value.value_type(),
                )))
            }
        }
        DummySqlType::Uuid => {
            if let Some(octets) = value.as_array() {
                // Array of values -> Uuid
                coerce_octet_sequence(octets.as_slice())
                    .map(CValue::Uuid)
                    .map_err(|()| Error::from(ErrorKind::MalformedUuid))
            } else if let Some(string) = value.as_str() {
                // Conversion from String
                Uuid::from_str(string.as_ref())
                    .map(|addr| addr.into_bytes())
                    .map(CValue::Uuid)
                    .map_err(|_| Error::from(ErrorKind::MalformedUuid))
            } else {
                Err(Error::from(ErrorKind::UnexpectedEventFormat(
                    column_name.to_string(),
                    expected_type.to_string(),
                    value.value_type(),
                )))
            }
        }

        DummySqlType::DateTime => get_and_wrap(
            column_name,
            value,
            ValueAccess::as_u32,
            |timestamp| CValue::DateTime(timestamp, UTC),
            expected_type,
        ),

        DummySqlType::DateTime64Secs => get_and_wrap(
            column_name,
            value,
            ValueAccess::as_i64,
            |timestamp| CValue::DateTime64(timestamp, (0, UTC)),
            expected_type,
        ),

        DummySqlType::DateTime64Millis => get_and_wrap(
            column_name,
            value,
            ValueAccess::as_i64,
            |timestamp| CValue::DateTime64(timestamp, (3, UTC)),
            expected_type,
        ),

        DummySqlType::DateTime64Micros => get_and_wrap(
            column_name,
            value,
            ValueAccess::as_i64,
            |timestamp| CValue::DateTime64(timestamp, (6, UTC)),
            expected_type,
        ),

        DummySqlType::DateTime64Nanos => get_and_wrap(
            column_name,
            value,
            ValueAccess::as_i64,
            |timestamp| CValue::DateTime64(timestamp, (9, UTC)),
            expected_type,
        ),
    }
}

fn wrap_getter_error<'a, 'b, F, T>(
    column_name: &str,
    value: &'a TValue<'b>,
    f: F,
    expected_type: &DummySqlType,
) -> Result<T>
where
    F: FnOnce(&'a TValue<'b>) -> Option<T>,
    T: 'a,
{
    f(value).ok_or_else(|| {
        Error::from(ErrorKind::UnexpectedEventFormat(
            column_name.to_string(),
            expected_type.to_string(),
            value.value_type(),
        ))
    })
}

fn get_and_wrap<'a, 'b, F, G, T>(
    column_name: &str,
    value: &'a TValue<'b>,
    getter: F,
    wrapper: G,
    expected_type: &DummySqlType,
) -> Result<CValue>
where
    F: FnOnce(&'a TValue<'b>) -> Option<T>,
    G: FnOnce(T) -> CValue,
    T: 'a,
{
    wrap_getter_error(column_name, value, getter, expected_type).map(wrapper)
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

                let left = convert_value_2(column_name, &input_value, &output_type).unwrap();
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
            json! { "127.0.0.1" }, DummySqlType::Ipv4 => CValue::Ipv4([127, 0, 0, 1]),
        }
    }

    test_value_conversion! {
        ipv4_from_array {
            json! { [1, 2, 3, 4] }, DummySqlType::Ipv4 => CValue::Ipv4([1, 2, 3, 4]),
        }
    }

    test_value_conversion! {
        ipv6_from_string {
            json! { "0000:0000:0000:0000:0000:0000:0000:0001" },
            DummySqlType::Ipv6
            =>
            CValue::Ipv6([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
        }
    }

    test_value_conversion! {
        ipv6_from_array {
            json! {
                [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
            },
            DummySqlType::Ipv6
            =>
            CValue::Ipv6([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
        }
    }

    test_value_conversion! {
        uuid_from_string {
            json! {
                "123e4567-e89b-12d3-a456-426614174000"
            },
            DummySqlType::Uuid
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

    const TIMESTAMP_32: u32 = 1652790383;
    const TIMESTAMP_U64: u64 = 1652790383;
    const TIMESTAMP_I64: i64 = 1652790383;

    test_value_conversion! {
        datetime32 {
            json! { TIMESTAMP_32 }, DummySqlType::DateTime => CValue::DateTime(TIMESTAMP_32, UTC),
        }
    }

    test_value_conversion! {
        datetime64_secs {
            json! { TIMESTAMP_U64 }, DummySqlType::DateTime64Secs => CValue::DateTime64(TIMESTAMP_I64, (0, UTC)),
        }
    }

    test_value_conversion! {
        datetime64_millis {
            json! { TIMESTAMP_U64 }, DummySqlType::DateTime64Millis => CValue::DateTime64(TIMESTAMP_I64, (3, UTC)),
        }
    }

    test_value_conversion! {
        datetime64_micros {
            json! { TIMESTAMP_U64 }, DummySqlType::DateTime64Micros => CValue::DateTime64(TIMESTAMP_I64, (6, UTC)),
        }
    }

    test_value_conversion! {
        datetime64_nanos {
            json! { TIMESTAMP_U64 }, DummySqlType::DateTime64Nanos => CValue::DateTime64(TIMESTAMP_I64, (9, UTC)),
        }
    }

    fn clickhouse_string_value(input: &str) -> CValue {
        CValue::String(Arc::new(input.to_string().into_bytes()))
    }

    fn clickhouse_array_value<const N: usize>(ty: SqlType, values: [CValue; N]) -> CValue {
        CValue::Array(ty.into(), Arc::new(values.to_vec()))
    }
}
