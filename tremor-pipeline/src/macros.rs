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

// this are macros
// #![cfg_attr(coverage, no_coverage)]

/// A macro that makes it simple to create a operator and the
/// required factory usage is:
/// `op!(NodeFactory(Node) {<constructor>})`
#[macro_export]
macro_rules! op {
    ($factory:ident ($uid:ident, $node:ident) $constructor:block) => {
        #[derive(Default)]
        pub struct $factory {}
        impl $crate::op::InitializableOperator for $factory {
            fn node_to_operator(
                &self,
                $uid: tremor_common::uids::OperatorUId,
                $node: &$crate::NodeConfig,
            ) -> $crate::errors::Result<Box<dyn $crate::op::Operator>> {
                $constructor
            }
        }
        impl $factory {
            fn new() -> Self {
                Self {}
            }
            pub fn new_boxed() -> Box<dyn $crate::op::InitializableOperator> {
                Box::new(Self::new())
            }
        }
    };
}
