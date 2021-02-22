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

//! Tremor Network Layer - Assigned names and numbers.
//!
//! This module contains network layer associated status code related structs
//! and errors. The main type in this module is `StatusCode` which is not
//! intended to be used through this module but rather the `network::StatusCode`
//! type.
//!

use std::fmt;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StatusCode {
    base: u16,
    code: u16,
}

/// A possible error value when converting a `StatusCode` from a `u16` or `&str`
///
/// This error indicates that the supplied input was not a valid code
#[derive(Debug)]
pub struct InvalidStatusCode {
    _priv: (),
}

impl StatusCode {
    pub(crate) fn base(&self) -> u16 {
        self.base
    }

    pub(crate) fn code(&self) -> u16 {
        self.code
    }

    /// Converts a u16 to a status code.
    ///
    /// The function validates the correctness of the supplied u16. It must be
    /// greater or equal to 100 but less than 600.
    ///
    #[inline]
    pub fn from_u16(src: u16) -> Result<StatusCode, InvalidStatusCode> {
        let base = src % 1000;
        let ext = src / 1000;
        if src < 100 || src >= 500 {
            return Err(InvalidStatusCode::new());
        }

        Ok(StatusCode {
            base: ext,
            code: base,
        })
    }

    /// Returns the `u16` corresponding to this `StatusCode`.
    ///
    #[inline]
    pub fn as_u16(&self) -> u16 {
        (*self).into()
    }

    #[inline]
    pub fn is_core(&self) -> bool {
        0 == self.base
    }

    #[inline]
    pub fn is_protocol(&self) -> bool {
        !self.is_core()
    }
}

impl fmt::Debug for StatusCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&((self.base * 1000) + self.code), f)
    }
}

/// Formats the status code, *including* the canonical reason.
///
impl fmt::Display for StatusCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}", u16::from(*self), self.canonical_reason())
    }
}

impl From<StatusCode> for u16 {
    #[inline]
    fn from(status: StatusCode) -> u16 {
        status.code
    }
}

impl InvalidStatusCode {
    fn new() -> InvalidStatusCode {
        InvalidStatusCode { _priv: () }
    }
}

macro_rules! status_codes {
    (
        $(
            $(#[$docs:meta])*
            ($base:expr, $code:expr, $konst:ident, $phrase:expr);
        )+
    ) => {
        impl StatusCode {
        $(
            $(#[$docs])*
            pub const $konst: StatusCode = StatusCode { base: $base, code: $code };
        )+

            /// Get the standardised `reason-phrase` for this status code.
            pub fn canonical_reason(&self) -> &'static str {
                match (self.base, self.code) {
                    $(
                    ($base, $code) => $phrase,
                    )+
                    _ => "internal network error"
                }
            }
        }
    }
}

status_codes! {
    /// Snot badger
    (0,   0, CLOSE_OK, "Ok");

    // Malformed UTF-8 data
    (0, 100, WIRE_MALFORMED_UTF8_VALUE, "Malformed value - not UTF-8 valid");
    /// Malformed structural value
    (0, 101, WIRE_MALFORMED_VALUE, "Malformed value - not a well-formed tremor value");

    /// Protocol was not a registered protocol
    (0, 200, PROTOCOL_NOT_FOUND, "Protocol not found error");
    /// Protocol was not specified
    (0, 201, PROTOCOL_NOT_SPECIFIED, "Protocol not specified error");
    /// Protocol does not support the operation specified
    (0, 202, PROTOCOL_OPERATION_INVALID, "Protocol operation not supported");
    /// Protocol operation was expected to be a record value
    (0, 203, PROTOCOL_RECORD_EXPECTED, "Protocol operation expected a record");

    /// Protocol operation has a missing record field
    (0, 204, PROTOCOL_RECORD_FIELD_EXPECTED, "Protocol operation - record has missing field");

    /// Protocol operation was expected to have exactly one field
    (0, 205, PROTOCOL_RECORD_ONE_FIELD_EXPECTED, "Protocol operation - record requires one field only");

    /// Protocol can only be registered once ( per alias )
    (0, 206, PROTOCOL_ALREADY_REGISTERED, "Protocol already registered error");

    /// Protocol session for specified alias not found
    (0, 207, PROTOCOL_SESSION_NOT_FOUND, "Protocol session not connected error");

    // API does not support the operation specified
    (1, 200, API_OPERATION_INVALID, "Network API Protocol operation not supported");

    // API body record value expected
    (1, 201, API_BODY_RECORD_EXPECTED, "Network API Protocol -  record expected for body");

    // API operation record value expected
    (1, 202, API_RECORD_EXPECTED, "Network API Protocol -  record expected");

    /// API body field was expected in operation record
    (1, 203, API_BODY_FIELD_EXPECTED, "Network API Protocol -  expected `body` record field");

    /// API operation failed due to unexpected internal server error
    (1, 204, API_OPERATION_FAILED, "Network API Protocol - operation failed due to internal server error");

    /// A connector of this name already exists
    (1, 205, API_CONNECTOR_ALREADY_EXISTS, "Network API Protocol - connector already registered error");

    /// A PE ( pipeline ) of this name already exists
    (1, 206, API_PROCESSING_ELEMENT_ALREADY_EXISTS, "Network API Protocol - processing element already registered error");

    /// A string value was expected
    (1, 207, API_STRING_EXPECTED, "Network API Protocol -  string expected");

    /// A string value was expected for the body field
    (1, 208, API_BODY_STRING_FIELD_EXPECTED, "Network API Protocol -  expected `body` string field");

    /// A binding by the name specified already exists
    (1, 209, API_BINDING_ALREADY_EXISTS, "Network API Protocol - binding already registered error");

    /// Operation expected the `name` field which not supplied
    (1, 210, API_NAME_FIELD_EXPECTED, "Network API Protocol - `name` field expected for `create` operation");

    /// The artefact kind specified is not a known type of artefact
    (1, 211, API_ARTEFACT_KIND_UNKNOWN, "Network API Protocol - invalid artefact kind");
}

impl fmt::Display for InvalidStatusCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("invalid status code")
    }
}

impl std::error::Error for InvalidStatusCode {
    fn description(&self) -> &str {
        "invalid status code"
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_status_code() -> Result<(), InvalidStatusCode> {
        for i in 100..499 {
            let x = StatusCode::from_u16(i)?;
            // NOTE asserts range well-formedness not whether or not code is valid
            // Currently, for each range ( < 1000 ) up to and not including 500 is ok
            assert_eq!(0, x.base());
            assert_eq!(i, x.code());
        }
        Ok(())
    }
}
