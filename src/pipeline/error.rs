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

use super::types::ValueType;
use std::error::Error;
use std::fmt::{self, Display};

#[derive(Debug)]
pub struct TypeError {
    location: String,
    is: ValueType,
    expects: ValueType,
}
impl TypeError {
    pub fn new(is: ValueType, expects: ValueType) -> Self {
        TypeError::with_location(&"", is, expects)
    }
    pub fn with_location<L: ToString>(location: &L, is: ValueType, expects: ValueType) -> Self {
        TypeError {
            location: location.to_string(),
            is,
            expects,
        }
    }
}

impl Error for TypeError {}

impl Display for TypeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Type Error: expected {:?} got {:?} at {}",
            self.expects, self.is, self.location
        )
    }
}

/// Graph error, thrown if something goes bad with the graph
#[derive(Debug)]
pub enum GraphError {
    ConfigError(String),
    TypeError(TypeError),
}

impl From<TypeError> for GraphError {
    fn from(t: TypeError) -> GraphError {
        GraphError::TypeError(t)
    }
}

impl From<String> for GraphError {
    fn from(s: String) -> GraphError {
        GraphError::ConfigError(s)
    }
}

impl<'a> From<&'a str> for GraphError {
    fn from(s: &'a str) -> GraphError {
        GraphError::ConfigError(s.to_string())
    }
}

impl fmt::Display for GraphError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Tremor Graph Error: {:?}", self)
    }
}
impl Error for GraphError {}
