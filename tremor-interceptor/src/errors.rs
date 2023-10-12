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
// use value_trait::prelude::*;

#[cfg(test)]
impl PartialEq for Error {
    fn eq(&self, _other: &Self) -> bool {
        // This might be Ok since we try to compare Result in tests
        false
    }
}

error_chain! {
    links {
    }
    foreign_links {
        Base64Error(tremor_common::base64::DecodeError);
        Config(tremor_config::Error);
        Io(std::io::Error);
        ParseIntError(std::num::ParseIntError);
        SnappyError(snap::Error);
        TryFromIntError(std::num::TryFromIntError);
        Utf8Error(std::str::Utf8Error);
        ValueError(tremor_value::Error);

    }

    errors {

        InvalidGelfHeader(len: usize, initial: Option<[u8; 2]>) {
            description("Invalid GELF header")
                display("Invalid GELF header len: {}, prefix: {:?}", len, initial)
        }

        MissingConfiguration(s: String) {
            description("Missing Configuration")
                display("Missing Configuration for {}", s)
        }

        InvalidConfiguration(configured_thing: String, msg: String) {
            description("Invalid Configuration")
                display("Invalid Configuration for {}: {}", configured_thing, msg)
        }

        InvalidInputData(msg: &'static str) {
            description("Invalid Input data")
                display("Invalid Input data: {}", msg)
        }
    }
}
