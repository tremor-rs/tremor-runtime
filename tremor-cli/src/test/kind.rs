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

use std::{convert::TryFrom, error::Error, fmt::Debug, fmt::Display};
/// Specifies a kind of test framework, or a composite `all` to capture all framework variants
#[derive(Deserialize, Debug, PartialEq)]
#[allow(clippy::module_name_repetitions)]
pub(crate) enum TestKind {
    Bench,
    Integration,
    Command,
    Unit,
    All,
    Unknown(String),
}

/// An unknown test kind
#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct UnknownKind(String);

impl Display for UnknownKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Unknown test kind `{}`, please choose one of `all`, `api`, `bench`, `command`, `integration`, `rest`, or `unit`", self.0)
    }
}

impl Error for UnknownKind {}

impl TryFrom<&str> for TestKind {
    fn try_from(from: &str) -> Result<Self, UnknownKind> {
        match from.to_lowercase().as_str() {
            "all" => Ok(TestKind::All),
            "api" | "command" | "rest" => Ok(TestKind::Command),
            "bench" | "benchmark" => Ok(TestKind::Bench),
            "it" | "integration" => Ok(TestKind::Integration),
            "unit" => Ok(TestKind::Unit),
            default => Err(UnknownKind(default.into())),
        }
    }

    type Error = UnknownKind;
}
