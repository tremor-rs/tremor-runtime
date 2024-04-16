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

//! Tremor common utility functions
#![deny(missing_docs)]
#![recursion_limit = "1024"]
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic,
    clippy::mod_module_files
)]

/// functions for async related code
pub mod asy;
mod errors;
/// File related functions
pub mod file;
/// Time related functions
pub mod time;

/// common id handling
pub mod ids;

/// Random numbers
pub mod rand;

/// String/str functions
pub mod string;

/// A dashmap with an expiration date for items
pub mod blue_green_hashmap;
/// Common ports
pub mod ports;

/// Base64 engine
pub mod base64;

/// URL with defaults
pub mod url;

/// Aliases for naming tremor elements
pub mod alias;

/// Priority Merge
pub mod primerge;

pub use errors::Error;

/// function that always returns true
#[must_use]
pub fn default_true() -> bool {
    true
}
/// function that always returns false
#[must_use]
pub fn default_false() -> bool {
    false
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
