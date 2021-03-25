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
use std::fmt::Display;

/// A shared error for common functions
#[derive(Debug)]
pub enum Error {
    /// Failed to open a File
    FileOpen(std::io::Error, String),
    /// Failed to create a File
    FileCreate(std::io::Error, String),
    /// Failed to canonicalize a path
    FileCanonicalize(std::io::Error, String),
    /// Failed to change working directory
    Cwd(std::io::Error, String),
}

impl Display for Error {
    fn fmt(&self, w: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::FileOpen(e, f) => write!(w, "Failed to open file `{}`: {}", f, e),
            Error::FileCreate(e, f) => write!(w, "Failed to create file `{}`: {}", f, e),
            Error::FileCanonicalize(e, f) => {
                write!(w, "Failed to canonicalize path `{}`: {}", f, e)
            }
            Error::Cwd(e, f) => write!(w, "Failed to change working directory `{}`: {}", f, e),
        }
    }
}

impl std::error::Error for Error {}

#[doc(hidden)]
/// Optimized try
#[macro_export]
macro_rules! stry {
    ($e:expr) => {
        match $e {
            ::std::result::Result::Ok(val) => val,
            ::std::result::Result::Err(err) => return ::std::result::Result::Err(err),
        }
    };
}
