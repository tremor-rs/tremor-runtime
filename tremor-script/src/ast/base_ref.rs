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

// Don't cover this file it's only getters
#![cfg(not(tarpaulin_include))]

/// A Basic expression that can be turned into a location
pub trait BaseRef: Clone {
    /// Calculates the fully qualified name or reference to this AST node given a `module_path`
    fn fqsn(&self, module_path: &[String]) -> String;
}

/// Implements the BaseRef trait providing a default / generic implementation
#[macro_export]
macro_rules! impl_fqsn {
    ($name:ident) => {
        impl<'script> BaseRef for $name<'script> {
            /// Calculates the fully qualified name or reference to this AST node given a `module_path`
            fn fqsn(&self, module_path: &[String]) -> String {
                if module_path.is_empty() {
                    self.id.clone()
                } else {
                    format!("{}::{}", module_path.join("::"), self.id)
                }
            }
        }
    };
}
