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

use std::borrow::Borrow;

use beef::Cow;
use serde::{Deserialize, Serialize};

/// A port
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Port<'port> {
    /// The `in` port
    In,
    /// The `out` port
    Out,
    /// The `err` port
    Err,
    /// The `metrics` port
    Metrics,
    /// The `overflow` port
    Overflow,
    /// The `control` port
    Control,
    /// All other ports
    #[serde(bound(deserialize = "'de: 'port"))]
    Custom(Cow<'port, str>),
}

impl<'port> Port<'port> {
    /// Creates a new custom port
    #[must_use]
    pub const fn const_str(s: &'port str) -> Self {
        Port::Custom(Cow::const_str(s))
    }
}

impl<'port> From<Cow<'port, str>> for Port<'port> {
    fn from(value: Cow<'port, str>) -> Self {
        match value.as_ref() {
            "in" => Port::In,
            "out" => Port::Out,
            "err" => Port::Err,
            "metrics" => Port::Metrics,
            "overflow" => Port::Overflow,
            "control" => Port::Control,
            _ if value == value.to_lowercase() => Port::Custom(value),
            _ => Port::from(Cow::from(value.to_lowercase())),
        }
    }
}

impl<'port, 'str> From<&'str str> for Port<'port>
where
    'str: 'port,
{
    fn from(value: &'str str) -> Self {
        match value {
            "in" => Port::In,
            "out" => Port::Out,
            "err" => Port::Err,
            "metrics" => Port::Metrics,
            "overflow" => Port::Overflow,
            "control" => Port::Control,
            _ => Port::from(Cow::from(value)),
        }
    }
}
impl From<String> for Port<'static> {
    fn from(value: String) -> Self {
        match value.as_str() {
            "in" => Port::In,
            "out" => Port::Out,
            "err" => Port::Err,
            "metrics" => Port::Metrics,
            "overflow" => Port::Overflow,
            "control" => Port::Control,
            _ => Port::from(Cow::from(value)),
        }
    }
}

impl<'port> Borrow<str> for Port<'port> {
    fn borrow(&self) -> &str {
        match self {
            Port::In => "in",
            Port::Out => "out",
            Port::Err => "err",
            Port::Metrics => "metrics",
            Port::Overflow => "overflow",
            Port::Control => "control",
            Port::Custom(c) => c,
        }
    }
}
impl<'port> PartialEq<str> for Port<'port> {
    fn eq(&self, other: &str) -> bool {
        let this: &str = self.borrow();
        this.eq(other)
    }
}

impl<'port> PartialEq<&str> for Port<'port> {
    fn eq(&self, other: &&str) -> bool {
        self.eq(*other)
    }
}

impl<'port> PartialEq<Port<'port>> for &str {
    fn eq(&self, other: &Port) -> bool {
        other.eq(self)
    }
}

impl<'port> std::fmt::Display for Port<'port> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.borrow())
    }
}

/// standard input port
pub const IN: Port<'static> = Port::In;
/// standard output port
pub const OUT: Port<'static> = Port::Out;
/// standard err port
pub const ERR: Port<'static> = Port::Err;
/// standard metrics port
pub const METRICS: Port<'static> = Port::Metrics;
/// Overflow
pub const OVERFLOW: Port<'static> = Port::Overflow;
/// standard control port
pub const CONTROL: Port<'static> = Port::Control;
