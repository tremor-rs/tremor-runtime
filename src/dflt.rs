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

use std::{convert, f64};

pub fn d_true() -> bool {
    true
}

pub fn d_false() -> bool {
    false
}

pub fn d_0<T>() -> T
where
    T: convert::From<u8>,
{
    T::from(0)
}

pub fn d_1<T>() -> T
where
    T: convert::From<u8>,
{
    T::from(1)
}

pub fn d_inf() -> f64 {
    f64::INFINITY
}

pub fn d_4<T>() -> T
where
    T: convert::From<u8>,
{
    T::from(4)
}

pub fn d_100<T>() -> T
where
    T: convert::From<u8>,
{
    T::from(100)
}

pub fn d_1000<T>() -> T
where
    T: convert::From<u16>,
{
    T::from(1000)
}

pub fn d_none<T>() -> Option<T> {
    None
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

pub fn dflt<T: Default>() -> T {
    T::default()
}
