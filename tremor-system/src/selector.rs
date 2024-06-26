// Copyright 2020-2024, The Tremor Team
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

use serde::{Deserialize, Serialize};

/// Plugin type
#[derive(Clone, Debug, PartialEq, Copy, Eq, Serialize, Deserialize, Hash, Default)]
pub enum PluginType {
    /// Normal plugin
    #[default]
    Normal,
    /// Debug plugin not meant to be used during production
    Debug,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash, Eq)]

/// Single selector rule
#[derive(Default)]
pub enum Selector {
    /// Select all plugins
    #[default]
    All,
    /// Select plugins by name
    Name(String),
    /// Select plugins by type
    Type(PluginType),
}

impl Selector {
    /// Test if a plugin should be selected
    pub fn test(&self, name: &(impl AsRef<str> + ?Sized), t: PluginType) -> bool {
        match self {
            Selector::All => true,
            Selector::Name(n) => n == name.as_ref(),
            Selector::Type(ty) => *ty == t,
        }
    }
}
impl From<&str> for Selector {
    fn from(s: &str) -> Self {
        Selector::Name(s.to_string())
    }
}
impl From<String> for Selector {
    fn from(s: String) -> Self {
        Selector::Name(s)
    }
}
impl From<PluginType> for Selector {
    fn from(s: PluginType) -> Self {
        Selector::Type(s)
    }
}
#[derive(Clone, Debug, PartialEq, Copy, Eq, Serialize, Deserialize, Hash)]

/// Selector disposition
pub enum Deposition {
    /// Include the plugin
    Include,
    /// Exclude the plugin
    Exclude,
}

impl From<bool> for Deposition {
    fn from(val: bool) -> Self {
        if val {
            Deposition::Include
        } else {
            Deposition::Exclude
        }
    }
}
impl From<Deposition> for bool {
    fn from(val: Deposition) -> Self {
        match val {
            Deposition::Include => true,
            Deposition::Exclude => false,
        }
    }
}
#[derive(Debug, Clone)]

/// Selector combiner for includes and excludes. Excludes take precedence over incluedes.
pub struct Rules {
    rules: Vec<(Selector, Deposition)>,
    default: Deposition,
}
/// Rule selector builder
pub struct RulesBuilder {
    rules: Vec<(Selector, Deposition)>,
}

impl RulesBuilder {
    /// adds an include rule
    #[must_use]
    pub fn include(mut self, s: impl Into<Selector>) -> Self {
        self.rules.push((s.into(), Deposition::Include));
        self
    }
    /// adds an exclude rule
    #[must_use]
    pub fn exclude(mut self, s: impl Into<Selector>) -> Self {
        self.rules.push((s.into(), Deposition::Exclude));
        self
    }

    /// default disposition include
    #[must_use]
    pub fn default_include(self) -> Rules {
        Rules {
            rules: self.rules,
            default: Deposition::Include,
        }
    }
    /// default disposition exclude
    #[must_use]
    pub fn default_exclude(self) -> Rules {
        Rules {
            rules: self.rules,
            default: Deposition::Exclude,
        }
    }
}

impl Rules {
    /// Create a new rule selector builder
    #[must_use]
    pub fn builder() -> RulesBuilder {
        RulesBuilder { rules: vec![] }
    }
    /// Test if a plugin should be selected
    pub fn test(&self, name: &(impl AsRef<str> + ?Sized), t: PluginType) -> bool {
        for (rule, disposition) in &self.rules {
            if rule.test(name, t) {
                return (*disposition).into();
            }
        }
        self.default.into()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_selector() {
        let s: Selector = "foo".into();
        assert_eq!(Selector::Name("foo".to_string()), s);
        let s: Selector = PluginType::Debug.into();
        assert_eq!(Selector::Type(PluginType::Debug), s);
    }
    #[test]
    fn test_rule_selector() {
        let rs = Rules::builder()
            .include("foo")
            .exclude(PluginType::Debug)
            .default_include();
        assert!(rs.test("foo", PluginType::Normal));
        assert!(rs.test("foo", PluginType::Debug));
        assert!(rs.test("bar", PluginType::Normal));
        assert!(!rs.test("bar", PluginType::Debug));
    }

    #[test]
    fn test_rule_selector_builder() {
        let rs = Rules::builder()
            .include("foo")
            .exclude(PluginType::Debug)
            .default_exclude();
        assert!(rs.test("foo", PluginType::Normal));
        assert!(rs.test("foo", PluginType::Debug));
        assert!(!rs.test("bar", PluginType::Normal));
        assert!(!rs.test("bar", PluginType::Debug));
    }
}
