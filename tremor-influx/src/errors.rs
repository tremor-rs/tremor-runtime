// Copyright 2020-2021, The Tremor Team
//
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

use std::fmt;
use std::io;
use value_trait::ValueType;

/// Influx parser error
#[derive(Debug)]
pub enum EncoderError {
    /// an invalid filed was encountered
    InvalidField(&'static str),
    /// an invalid timestamp was encountered
    InvalidTimestamp(ValueType),
    /// an invalid value was encountered
    InvalidValue(String, ValueType),
    /// a io error was encountered
    Io(io::Error),
    /// a required field is missing
    MissingField(&'static str),
    /// InvalidType
    TypeError(value_trait::TryTypeError),
}

impl From<value_trait::TryTypeError> for EncoderError {
    fn from(e: value_trait::TryTypeError) -> Self {
        Self::TypeError(e)
    }
}

impl PartialEq for EncoderError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (EncoderError::InvalidField(v1), EncoderError::InvalidField(v2))
            | (EncoderError::MissingField(v1), EncoderError::MissingField(v2)) => v1 == v2,
            (EncoderError::InvalidTimestamp(v1), EncoderError::InvalidTimestamp(v2)) => v1 == v2,
            (EncoderError::InvalidValue(k1, v1), EncoderError::InvalidValue(k2, v2)) => {
                k1 == k2 && v1 == v2
            }
            (EncoderError::Io(_), EncoderError::Io(_)) => true,
            (EncoderError::TypeError(e1), EncoderError::TypeError(e2)) => e1 == e2,

            (_, _) => false,
        }
    }
}

/// Influx parser error
#[derive(Debug, PartialEq, Eq)]
pub enum DecoderError {
    /// an `=` in a value was found
    EqInTagValue(usize),
    /// unexpected character
    Unexpected(usize),
    /// a generic error
    Generic(usize, String),
    /// an invalid field
    InvalidFields(usize),
    /// missing value for a tag
    MissingTagValue(usize),
    /// failed to parse float value
    ParseFloatError(usize, lexical::Error),
    /// failed to parse integer value
    ParseIntError(usize, lexical::Error),
    /// trailing characters after the timestamp
    TrailingCharacter(usize),
    /// invalid escape sequence
    InvalidEscape(usize),
    /// unexpected end of message
    UnexpectedEnd(usize),
}

impl fmt::Display for EncoderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl fmt::Display for DecoderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for EncoderError {}
impl std::error::Error for DecoderError {}

/// Parser result type
pub type EncoderResult<T> = std::result::Result<T, EncoderError>;

/// Parser result type
pub type DecoderResult<T> = std::result::Result<T, DecoderError>;

impl From<io::Error> for EncoderError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
