// Copyright 2018, Wayfair GmbH
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

use elastic;
use pipeline::error::TypeError;
use reqwest;
use serde_json;
use std::error::Error;
use std::string::ToString;
use std::{fmt, num};

/// Generic error
#[derive(Debug, Clone)]
pub struct TSError {
    message: String,
}
impl Error for TSError {}

impl TSError {
    pub fn new<T: ToString>(msg: &T) -> Self {
        TSError {
            message: msg.to_string(),
        }
    }
}

impl fmt::Display for TSError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl From<elastic::Error> for TSError {
    fn from(from: elastic::Error) -> TSError {
        TSError::new(&format!("ES Error: {}", from))
    }
}

impl From<num::ParseFloatError> for TSError {
    fn from(e: num::ParseFloatError) -> TSError {
        TSError::new(&format!("{}", e))
    }
}
impl From<num::ParseIntError> for TSError {
    fn from(e: num::ParseIntError) -> TSError {
        TSError::new(&format!("{}", e))
    }
}

impl From<serde_json::Error> for TSError {
    fn from(e: serde_json::Error) -> TSError {
        TSError::new(&format!("Serade error: {}", e))
    }
}

impl From<reqwest::Error> for TSError {
    fn from(from: reqwest::Error) -> TSError {
        TSError::new(&format!("HTTP Error: {}", from))
    }
}

impl From<TypeError> for TSError {
    fn from(from: TypeError) -> TSError {
        TSError::new(&format!("Type Error: {}", from))
    }
}
