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

use std::{convert::TryFrom, error::Error, fmt::Debug, fmt::Display};
/// Specifies a kind of test framework, or a composite `all` to capture all framework variants
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Hash, Eq)]
pub(crate) enum Kind {
    Bench,
    Integration,
    Command,
    Unit,
    All,
    Unknown(String),
}
impl ToString for Kind {
    fn to_string(&self) -> String {
        match self {
            Kind::Bench => "bench".to_string(),
            Kind::Integration => "integration".to_string(),
            Kind::Command => "command".to_string(),
            Kind::Unit => "unit".to_string(),
            Kind::All => "all".to_string(),
            Kind::Unknown(u) => format!("unknown({})", u),
        }
    }
}
impl Default for Kind {
    fn default() -> Self {
        Self::All
    }
}

/// An unknown test kind
#[derive(Debug)]
pub struct Unknown(String);

impl Display for Unknown {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Unknown test kind `{}`, please choose one of `all`, `api`, `bench`, `command`, `integration`, `rest`, or `unit`", self.0)
    }
}

impl Error for Unknown {}

impl TryFrom<&str> for Kind {
    fn try_from(from: &str) -> Result<Self, Unknown> {
        match from.to_lowercase().as_str() {
            "all" => Ok(Kind::All),
            "api" | "command" | "rest" => Ok(Kind::Command),
            "bench" | "benchmark" => Ok(Kind::Bench),
            "it" | "integration" => Ok(Kind::Integration),
            "unit" => Ok(Kind::Unit),
            default => Err(Unknown(default.into())),
        }
    }

    type Error = Unknown;
}
