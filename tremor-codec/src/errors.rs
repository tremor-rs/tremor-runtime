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

//NOTE: error_chain
#![allow(deprecated, missing_docs, clippy::large_enum_variant)]

use error_chain::error_chain;
use value_trait::prelude::*;

pub type Kind = ErrorKind;

impl From<TryTypeError> for Error {
    fn from(e: TryTypeError) -> Self {
        ErrorKind::TypeError(e.expected, e.got).into()
    }
}

#[cfg(test)]
impl PartialEq for Error {
    fn eq(&self, _other: &Self) -> bool {
        // This might be Ok since we try to compare Result in tests
        false
    }
}

error_chain! {
    foreign_links {
        CsvError(csv::Error);
        DateTimeParseError(chrono::ParseError);
        FromUtf8Error(std::string::FromUtf8Error);
        InfluxEncoderError(tremor_influx::EncoderError);
        Io(std::io::Error);
        JsonAccessError(value_trait::AccessError);
        JsonError(simd_json::Error);
        MsgPackDecoderError(rmp_serde::decode::Error);
        MsgPackEncoderError(rmp_serde::encode::Error);
        ReqwestError(reqwest::Error);
        InvalidHeaderName(reqwest::header::InvalidHeaderName);
        TryFromIntError(std::num::TryFromIntError);
        ValueError(tremor_value::Error);
        Utf8Error(std::str::Utf8Error);
        YamlError(serde_yaml::Error) #[doc = "Error during yaml parsing"];
        Uuid(uuid::Error);
        Lexical(lexical::Error);
        SimdUtf8(simdutf8::basic::Utf8Error);
        TremorCodec(crate::codec::tremor::Error);
        AvroError(apache_avro::Error);
        UrlParseError(tremor_common::url::ParseError);
        SRCError(schema_registry_converter::error::SRCError);
    }

    errors {
        TypeError(expected: ValueType, found: ValueType) {
            description("Type error")
                display("Type error: Expected {}, found {}", expected, found)
        }

        CodecNotFound(name: String) {
            description("Codec not found")
                display("Codec \"{}\" not found.", name)
        }

        NotCSVSerializableValue(value: String) {
            description("The value cannot be serialized to CSV. Expected an array.")
            display("The value {} cannot be serialized to CSV. Expected an array.", value)
        }

        InvalidStatsD {
            description("Invalid statsd metric")
                display("Invalid statsd metric")
        }
        InvalidDogStatsD {
            description("Invalid dogstatsd metric")
                display("Invalid dogstatsd metric")
        }
        InvalidInfluxData(s: String, e: tremor_influx::DecoderError) {
            description("Invalid Influx Line Protocol data")
                display("Invalid Influx Line Protocol data: {}\n{}", e, s)
        }
        InvalidBInfluxData(s: String) {
            description("Invalid BInflux Line Protocol data")
                display("Invalid BInflux Line Protocol data: {}", s)
        }
        InvalidSyslogData(s: &'static str) {
            description("Invalid Syslog Protocol data")
                display("Invalid Syslog Protocol data: {}", s)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use matches::assert_matches;

    #[test]
    fn test_type_error() {
        let r = Error::from(TryTypeError {
            expected: ValueType::Object,
            got: ValueType::String,
        })
        .0;
        assert_matches!(
            r,
            ErrorKind::TypeError(ValueType::Object, ValueType::String)
        );
    }
}
