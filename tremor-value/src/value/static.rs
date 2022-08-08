// Copyright 2021, The Tremor Team
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

use crate::value::Value;

/// avoiding lifetime issues with generics
/// See: <https://github.com/rust-lang/rust/issues/64552>
#[derive(Debug, Clone)]
pub struct StaticValue(Value<'static>);

impl StaticValue {
    /// extract the inner `Value`
    #[must_use]
    pub fn into_value(self) -> Value<'static> {
        self.0
    }

    /// get a reference to the inner value
    #[must_use]
    pub fn value(&self) -> &Value<'static> {
        &self.0
    }
}
impl<'de> serde::de::Deserialize<'de> for StaticValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Value::deserialize(deserializer).map(|value| StaticValue(value.into_static()))
    }
}

impl std::fmt::Display for StaticValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
