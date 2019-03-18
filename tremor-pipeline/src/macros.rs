// Copyright 2018-2019, Wayfair GmbH
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

macro_rules! type_error {
    ($location:expr, $got:expr, $want:expr) => {
        Err(ErrorKind::TypeError($location.into(), $got, $want).into())
    };
}

#[macro_export]
macro_rules! op {
    ($factory:ident ($node:ident) $constructor:block) => {
        #[derive(Default)]
        pub struct $factory {}
        impl crate::op::InitializableOperator for $factory {
            fn from_node(
                &self,
                $node: &crate::NodeConfig,
            ) -> crate::errors::Result<Box<crate::op::Operator>> {
                $constructor
            }
        }
        impl $factory {
            fn new() -> Self {
                $factory {}
            }
            pub fn new_boxed() -> Box<crate::op::InitializableOperator> {
                Box::new(Self::new())
            }
        }
    };
}
