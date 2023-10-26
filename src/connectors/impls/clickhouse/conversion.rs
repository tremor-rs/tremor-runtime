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
use tremor_value::Value as TValue;
use uuid::Uuid;
use value_trait::prelude::*;

use super::DummySqlType;
use crate::errors::{Error, ErrorKind, Result};

const UTC: Tz = Tz::UTC;

pub(super) fn convert_value(
    column_name: &str,
    value: &TValue,
    expected_type: &DummySqlType,
) -> Result<CValue> {
    let context = ConversionContext {
        column_name,
        value,
        expected_type,
    };

    match expected_type {
        DummySqlType::Array(expected_inner_type) => {
            // We don't check that all elements of the array have the same type.
            // Instead, we check that every element can be converted to the expected
            // array type.
            wrap_getter_error(context, ValueAsContainer::as_array)?
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

        DummySqlType::UInt8 => get_and_wrap(context, ValueAsScalar::as_u8, CValue::UInt8),

        DummySqlType::UInt16 => get_and_wrap(context, ValueAsScalar::as_u16, CValue::UInt16),

        DummySqlType::UInt32 => get_and_wrap(context, ValueAsScalar::as_u32, CValue::UInt32),

        DummySqlType::UInt64 => get_and_wrap(context, ValueAsScalar::as_u64, CValue::UInt64),

        DummySqlType::Int8 => get_and_wrap(context, ValueAsScalar::as_i8, CValue::Int8),

        DummySqlType::Int16 => get_and_wrap(context, ValueAsScalar::as_i16, CValue::Int16),

        DummySqlType::Int32 => get_and_wrap(context, ValueAsScalar::as_i32, CValue::Int32),

        DummySqlType::Int64 => get_and_wrap(context, ValueAsScalar::as_i64, CValue::Int64),

        DummySqlType::String => get_and_wrap(context, ValueAsScalar::as_str, |s| {
            CValue::String(Arc::new(s.as_bytes().to_vec()))
        }),

        DummySqlType::Ipv4 => convert_string_or_array(
            context,
            |ip: Ipv4Addr| ip.octets(),
            // This is slightly embarrassing...
            //
            // ... But it does work!
            |[a, b, c, d]| CValue::Ipv4([d, c, b, a]),
            ErrorKind::MalformedIpAddr,
        ),

        DummySqlType::Ipv6 => convert_string_or_array(
            context,
            |ip: Ipv6Addr| ip.octets(),
            CValue::Ipv6,
            ErrorKind::MalformedIpAddr,
        ),

        DummySqlType::Uuid => convert_string_or_array(
            context,
            Uuid::into_bytes,
            CValue::Uuid,
            ErrorKind::MalformedUuid,
        ),

        DummySqlType::DateTime => get_and_wrap(context, ValueAsScalar::as_u32, |timestamp| {
            CValue::DateTime(timestamp, UTC)
        }),

        DummySqlType::DateTime64Secs => get_and_wrap(context, ValueAsScalar::as_i64, |timestamp| {
            CValue::DateTime64(timestamp, (0, UTC))
        }),

        DummySqlType::DateTime64Millis => {
            get_and_wrap(context, ValueAsScalar::as_i64, |timestamp| {
                CValue::DateTime64(timestamp, (3, UTC))
            })
        }

        DummySqlType::DateTime64Micros => {
            get_and_wrap(context, ValueAsScalar::as_i64, |timestamp| {
                CValue::DateTime64(timestamp, (6, UTC))
            })
        }

        DummySqlType::DateTime64Nanos => {
            get_and_wrap(context, ValueAsScalar::as_i64, |timestamp| {
                CValue::DateTime64(timestamp, (9, UTC))
            })
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct ConversionContext<'config, 'event> {
    column_name: &'config str,
    value: &'event TValue<'event>,
    expected_type: &'config DummySqlType,
}

fn wrap_getter_error<'config, 'event, Getter, Output>(
    context: ConversionContext<'config, 'event>,
    f: Getter,
) -> Result<Output>
where
    Getter: FnOnce(&'event TValue<'event>) -> Option<Output>,
    Output: 'event,
{
    f(context.value).ok_or_else(|| {
        Error::from(ErrorKind::UnexpectedEventFormat(
            context.column_name.to_string(),
            context.expected_type.to_string(),
            context.value.value_type(),
        ))
    })
}

fn get_and_wrap<'config, 'event, Getter, Mapper, Output>(
    context: ConversionContext<'config, 'event>,
    getter: Getter,
    wrapper: Mapper,
) -> Result<CValue>
where
    Getter: FnOnce(&'event TValue<'event>) -> Option<Output>,
    Mapper: FnOnce(Output) -> CValue,
    Output: 'event,
{
    wrap_getter_error(context, getter).map(wrapper)
}

fn convert_string_or_array<Output, Getter, Variant, const N: usize>(
    context: ConversionContext,
    extractor: Getter,
    variant: Variant,
    error: ErrorKind,
) -> Result<CValue>
where
    Output: FromStr,
    Getter: FnOnce(Output) -> [u8; N],
    Variant: FnOnce([u8; N]) -> CValue,
{
    // Before everyone gets lost, let's briefly describe the generic types of
    // this function.
    //
    // When a String is passed, we must have a parsing function and an
    // extracting function. The types involved are the following:
    //
    // String
    //  |
    //  | Output::from_str
    //  V
    // Output
    //  |
    //  | extractor
    //  V
    // [u8; N]
    //  |
    //  | variant
    //  V
    // CValue

    if let Some(octets) = context.value.as_array() {
        coerce_octet_sequence(octets.as_slice())
            .map(variant)
            .map_err(|()| Error::from(error))
    } else if let Some(string) = context.value.as_str() {
        Output::from_str(string)
            .map(extractor)
            .map(variant)
            .map_err(|_| Error::from(error))
    } else {
        Err(Error::from(ErrorKind::UnexpectedEventFormat(
            context.column_name.to_string(),
            context.expected_type.to_string(),
            context.value.value_type(),
        )))
    }
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
        u8_conversion {
            json! { 42 }, DummySqlType::UInt8 => CValue::UInt8(42),
        }
    }

    test_value_conversion! {
        u16_conversion {
            json! { 42 }, DummySqlType::UInt16 => CValue::UInt16(42),
        }
    }

    test_value_conversion! {
        u32_conversion {
            json! { 42 }, DummySqlType::UInt32 => CValue::UInt32(42),
        }
    }

    test_value_conversion! {
        u64_conversion {
            json! { 42 }, DummySqlType::UInt64 => CValue::UInt64(42),
        }
    }

    test_value_conversion! {
        i8_conversion {
            json! { 101 }, DummySqlType::Int8 => CValue::Int8(101),
        }
    }

    test_value_conversion! {
        i16_conversion {
            json! { 101 }, DummySqlType::Int16 => CValue::Int16(101),
        }
    }

    test_value_conversion! {
        i32_conversion {
            json! { 101 }, DummySqlType::Int32 => CValue::Int32(101),
        }
    }

    test_value_conversion! {
        i64_conversion {
            json! { 101 }, DummySqlType::Int64 => CValue::Int64(101),
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
                &[clickhouse_string_value("foo")]
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
                &[clickhouse_string_value("foo"), clickhouse_string_value("bar"), clickhouse_string_value("baz")]
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
            json! { "127.0.0.1" }, DummySqlType::Ipv4 => CValue::Ipv4([1, 0, 0, 127]),
        }
    }

    test_value_conversion! {
        ipv4_from_array {
            json! { [1, 2, 3, 4] }, DummySqlType::Ipv4 => CValue::Ipv4([4, 3, 2, 1]),
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

    const TIMESTAMP_32: u32 = 1_652_790_383;
    const TIMESTAMP_U64: u64 = 1_652_790_383;
    const TIMESTAMP_I64: i64 = 1_652_790_383;

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

    fn clickhouse_array_value(ty: SqlType, values: &[CValue]) -> CValue {
        CValue::Array(ty.into(), Arc::new(values.to_vec()))
    }
}
