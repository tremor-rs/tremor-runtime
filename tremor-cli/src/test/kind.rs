// Copyright 2018-2020, Wayfair GmbH
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

#[derive(Deserialize, Debug, PartialEq)]
pub(crate) enum TestKind {
    Bench,
    Integration,
    Command,
    Unit,
    All,
}

impl From<String> for TestKind {
    fn from(from: String) -> Self {
        From::<&str>::from(from.as_str())
    }
}

impl From<&str> for TestKind {
    fn from(from: &str) -> Self {
        match from.to_lowercase().as_str() {
            "bench" => TestKind::Bench,
            "benchmark" => TestKind::Bench,
            "integration" => TestKind::Integration,
            "it" => TestKind::Integration,
            "api" => TestKind::Command,
            "rest" => TestKind::Command,
            "command" => TestKind::Command,
            "unit" => TestKind::Unit,
            "all" => TestKind::All,
            _default => TestKind::All,
        }
    }
}
