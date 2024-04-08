// Copyright 2021-2024, The Tremor Team
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

//! Tremor AWS connectors

#![deny(warnings)]
#![deny(missing_docs)]
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic,
    clippy::mod_module_files
)]

use tremor_connectors::ConnectorBuilder;

/// AWS S3 connector
pub mod s3;

/// builtin connector types
#[must_use]
pub fn builtin_connector_types() -> Vec<Box<dyn ConnectorBuilder + 'static>> {
    vec![
        Box::<s3::streamer::Builder>::default(),
        Box::<s3::reader::Builder>::default(),
    ]
}

#[cfg(test)]
mod tests {}
