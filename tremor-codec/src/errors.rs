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

use value_trait::prelude::*;

#[cfg(test)]
impl PartialEq for Error {
    fn eq(&self, _other: &Self) -> bool {
        // This might be Ok since we try to compare Result in tests
        false
    }
}

/// A [`std::result::Result`] with an [`Error`] as err variant.
pub type Result<T> = std::result::Result<T, Error>;

/// Tremor Codec Error
#[derive(Debug, thiserror::Error)]
pub enum Error {
    // wrappers for foreign errors
    /// Error during CSV decoding/encoding
    #[error(transparent)]
    Csv(#[from] csv::Error),
    /// Error parsing a datetime string
    #[error(transparent)]
    DateTimeParse(#[from] chrono::ParseError),
    /// Error encoding as influx
    #[error(transparent)]
    InfluxEncoder(#[from] tremor_influx::EncoderError),
    /// IO Error. See [`std::io::Error`]
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Error accessing a tremor value
    #[error(transparent)]
    JsonAccess(#[from] value_trait::AccessError),
    /// JSON encoding/decoding error
    #[error(transparent)]
    Json(#[from] simd_json::Error),
    /// Error decoding data as msgpack
    #[error(transparent)]
    MsgPackDecode(#[from] rmp_serde::decode::Error),
    /// Error encoding value as msgpack
    #[error(transparent)]
    MsgPackEncode(#[from] rmp_serde::encode::Error),
    /// Error converting from an int type
    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),
    /// JSON error
    #[error(transparent)]
    Value(#[from] tremor_value::Error),
    /// Invalid UTF8
    #[error(transparent)]
    Utf8(#[from] std::str::Utf8Error),
    /// Invalid YAML
    #[error("Error during YAML parsing: {0}")]
    YamlError(#[from] serde_yaml::Error),
    /// Error working with UUIDs
    #[error(transparent)]
    Uuid(#[from] uuid::Error),
    /// Error parsing ints or floats
    #[error(transparent)]
    Lexical(#[from] lexical::Error),
    /// Error decoding UTF8
    #[error(transparent)]
    SimdUtf8(#[from] simdutf8::basic::Utf8Error),
    #[error(transparent)]
    /// Invalid Tremor codec
    TremorCodec(#[from] crate::codec::tremor::Error),
    #[error(transparent)]
    /// Error handling avro
    AvroError(#[from] apache_avro::Error),
    #[error(transparent)]
    /// Error parsing a URL
    UrlParseError(#[from] tremor_common::url::ParseError),
    /// Schema Registry Converter Error
    #[error(transparent)]
    SRCError(#[from] schema_registry_converter::error::SRCError),
    /// generic str error for using:
    ///
    /// ```rust
    /// use tremor_codec::errors::Result;
    /// fn foo() -> Result<()> {
    ///     return Err("foo".into());
    /// }
    /// ```
    #[error("{0}")]
    Str(String),

    // our own errors
    /// Unexpected Type
    #[error("Type error: Expected {expected}, found {found}")]
    TypeError {
        /// Expected value type
        expected: ValueType,
        /// found value type
        found: ValueType,
    },

    /// Codec not found
    #[error("Codec \"{0}\" not found.")]
    CodecNotFound(String),
    /// Value is not CSV serializable
    #[error("The value {0} cannot be serialized to CSV. Expected an array.")]
    NotCSVSerializableValue(String),
    /// Invalid statsd metric
    #[error("Invalid statsd metric")]
    InvalidStatsD,
    /// Invalid graphite plaintext
    #[error("Invalid graphite plaintext protocol metric")]
    InvalidGraphitePlaintext,
    /// Invalid dogstatsd
    #[error("Invalid dogstatsd metric")]
    InvalidDogStatsD,
    /// Invalid influx data
    #[error("Invalid Influx Line Protocol data: {source}\n{line}")]
    InvalidInfluxData {
        /// invalid influx data
        line: String,
        /// underlying error
        source: tremor_influx::DecoderError,
    },
    /// Invalid binflux data
    #[error("Invalid BInflux Line Protocol data: {0}")]
    InvalidBInfluxData(String),
    /// Invalid syslog data
    #[error("Invalid Syslog Protocol data: {0}")]
    InvalidSyslogData(&'static str),
}

impl From<TryTypeError> for Error {
    fn from(e: TryTypeError) -> Self {
        Self::TypeError {
            expected: e.expected,
            found: e.got,
        }
    }
}

impl From<&str> for Error {
    fn from(value: &str) -> Self {
        Self::Str(value.to_string())
    }
}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Self::Str(value)
    }
}
