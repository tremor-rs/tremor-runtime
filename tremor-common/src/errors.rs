// Copyright 2020, The Tremor Team
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
use std::fmt::Display;

/// A shared error for common functions
#[derive(Debug)]
pub enum Error {
    /// Failed to open a File
    FileOpen(std::io::Error, String),
}

impl Display for Error {
    fn fmt(&self, w: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::FileOpen(e, f) => write!(w, "Failed to open file `{}`: {}", f, e),
        }
    }
}

impl std::error::Error for Error {}
