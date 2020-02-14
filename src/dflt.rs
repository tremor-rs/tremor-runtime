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

use std::convert;

// just a default
#[cfg_attr(tarpaulin, skip)]
pub fn d_true() -> bool {
    true
}

// just a default
#[cfg_attr(tarpaulin, skip)]
pub fn d_false() -> bool {
    false
}

// just a default
#[cfg_attr(tarpaulin, skip)]
pub fn d_0<T>() -> T
where
    T: convert::From<u8>,
{
    T::from(0)
}

// just a default
#[cfg_attr(tarpaulin, skip)]
pub fn d_4<T>() -> T
where
    T: convert::From<u8>,
{
    T::from(4)
}

// just a default
#[cfg_attr(tarpaulin, skip)]
pub fn d_ttl() -> u32 {
    64
}

// just a default
#[cfg_attr(tarpaulin, skip)]
pub fn d<T>() -> T
where
    T: Default,
{
    T::default()
}

// just a default
#[cfg_attr(tarpaulin, skip)]
pub fn dflt<T: Default>() -> T {
    T::default()
}
