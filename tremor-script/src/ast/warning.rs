// Copyright 2020-2022, The Tremor Team
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

use crate::pos::Span;

/// ordered collection of warnings
pub type Warnings = std::collections::BTreeSet<Warning>;

/// Class of warning that gives additional insight into the warning's intent
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Copy)]
pub enum Class {
    /// A general warning that isn't specific to a particular class
    General,
    /// A warning that is related to performance
    Performance,
    /// A warning that is related to consistency
    Consistency,
    /// A warning that is related to possibly unexpected behaviour
    Behaviour,
    // /// A warning that is related to deprecated functionality
    // Deprication,
}

impl Display for Class {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::General => write!(f, "general"),
            Self::Performance => write!(f, "performance"),
            Self::Consistency => write!(f, "consistency"),
            Self::Behaviour => write!(f, "behaviour"),
        }
    }
}

#[derive(Serialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
/// A warning generated while lexing or parsing
pub struct Warning {
    /// type of the warning
    pub class: Class,
    /// Outer span of the warning
    pub outer: Span,
    /// Inner span of thw warning
    pub inner: Span,
    /// Warning message
    pub msg: String,
}

impl Warning {
    pub(crate) fn new<T: ToString>(outer: Span, inner: Span, msg: &T, class: Class) -> Self {
        Self {
            class,
            outer,
            inner,
            msg: msg.to_string(),
        }
    }
    pub(crate) fn new_with_scope<T: ToString>(warning_scope: Span, msg: &T, class: Class) -> Self {
        Self::new(warning_scope, warning_scope, msg, class)
    }
}
