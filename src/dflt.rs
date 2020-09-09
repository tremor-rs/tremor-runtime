// Copyright 2020, The Tremor Team
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

// This are just default values

use std::convert;

pub fn d_true() -> bool {
    true
}

pub fn d_false() -> bool {
    false
}

pub fn d_4<T>() -> T
where
    T: convert::From<u8>,
{
    T::from(4)
}

pub fn d_ttl() -> u32 {
    64
}

pub fn d<T>() -> T
where
    T: Default,
{
    T::default()
}
