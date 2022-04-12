use std::fmt::Display;

use super::raw::IdentRaw;

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

/// Identifies a node in the AST.
#[derive(Clone, Debug, PartialEq, Serialize, Hash, Eq)]
pub struct NodeId {
    /// The ID of the Node
    pub(crate) id: String,
    /// The module of the Node
    pub(crate) module: Vec<String>,
}

impl<T: ToString> From<&T> for NodeId {
    fn from(id: &T) -> Self {
        Self {
            id: id.to_string(),
            module: vec![],
        }
    }
}

impl<'script> From<IdentRaw<'script>> for NodeId {
    fn from(id: IdentRaw<'script>) -> Self {
        Self {
            id: id.id.to_string(),
            module: vec![],
        }
    }
}

impl NodeId {
    /// Creates a new NodeId with a given module prefix
    #[must_use]
    pub fn with_prefix(mut self, prefix: &[String]) -> Self {
        let mut module = prefix.to_vec();
        module.append(&mut self.module);
        self.module = module;
        self
    }
    /// Create a new `NodeId` from an ID and Module list.
    #[must_use]
    pub fn new<T: ToString>(id: T, module: &[String]) -> Self {
        Self {
            id: id.to_string(),
            module: module.to_vec(),
        }
    }

    /// The node's id.
    #[must_use]
    pub fn id(&self) -> &str {
        self.id.as_str()
    }

    /// The node's module.
    #[must_use]
    pub fn module(&self) -> &[String] {
        &self.module
    }

    /// Mutate the node's module.
    pub fn module_mut(&mut self) -> &mut Vec<String> {
        &mut self.module
    }

    /// Calculate the fully qualified name from
    /// the given module path.
    #[must_use]
    pub fn fqn(&self) -> String {
        self.to_string()
    }

    /// Calculate the fully qualified name of some
    /// target identifier given this node's module
    /// path.
    #[must_use]
    pub fn target_fqn(&self, target: &str) -> String {
        if self.module.is_empty() {
            target.to_string()
        } else {
            format!("{}::{}", self.module.join("::"), target)
        }
    }
}

impl Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.module.is_empty() {
            write!(f, "{}", self.id)
        } else {
            write!(f, "{}::{}", self.module.join("::"), self.id)
        }
    }
}

/// fully qualified reference to nodes
pub trait BaseRef {
    /// Generates the fully qualified name
    fn fqn(&self) -> String;
}

#[doc(hidden)]
#[macro_export]
macro_rules! impl_fqn {
    ($struct:ident) => {
        impl crate::ast::node_id::BaseRef for $struct<'_> {
            fn fqn(&self) -> String {
                self.node_id.fqn()
            }
        }
    };
}

#[cfg(test)]
mod test {
    use super::NodeId;

    #[test]
    fn fqn() {
        let no_module = NodeId::new("foo".to_string(), &[]);
        assert_eq!(no_module.fqn(), "foo");
        assert!(no_module.module().is_empty());

        let with_module = NodeId::new("foo".to_string(), &["bar".to_string(), "baz".to_string()]);
        assert_eq!(with_module.fqn(), "bar::baz::foo");
        assert_eq!(with_module.module(), &["bar", "baz"]);

        let target = "quux";
        assert_eq!(no_module.target_fqn(target), target);
        assert_eq!(with_module.target_fqn(target), "bar::baz::quux");
    }
}
