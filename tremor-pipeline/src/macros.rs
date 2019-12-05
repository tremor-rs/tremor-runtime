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

#[macro_export]
macro_rules! op {
    ($factory:ident ($node:ident) $constructor:block) => {
        #[derive(Default)]
        pub struct $factory {}
        impl crate::op::InitializableOperator for $factory {
            fn from_node(
                &self,
                $node: &crate::NodeConfig,
            ) -> crate::errors::Result<Box<dyn crate::op::Operator>> {
                $constructor
            }
        }
        impl $factory {
            fn new() -> Self {
                Self {}
            }
            pub fn new_boxed() -> Box<dyn crate::op::InitializableOperator> {
                Box::new(Self::new())
            }
        }
    };
}

#[macro_export]
macro_rules! metamap {
    { $($key:expr => $value:expr),+ } => {
        {
            // TODO figure out why `crate::` does not work here
            let mut m = simd_json::value::borrowed::Object::new();
            $(
                m.insert($key.into(), simd_json::OwnedValue::from($value));
            )+
            m
        }
     };
}
