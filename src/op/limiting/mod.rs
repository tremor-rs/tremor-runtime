// Copyright 2018, Wayfair GmbH
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

//! # Rate limiting operators
//!
//! Rate limiting operators can be used to limit the number of
//! events that are allowed to pass through them.

pub mod backpressure;
pub mod percentile;
pub mod windowed;
use errors::*;
use pipeline::prelude::*;

pub enum Limiter {
    Backpressure(backpressure::Limiter),
    Percentile(percentile::Limiter),
    Windowed(windowed::Limiter),
}

impl Limiter {
    pub fn new(name: &str, opts: ConfValue) -> Result<Limiter> {
        match name {
            "windowed" => Ok(Limiter::Windowed(windowed::Limiter::new(opts)?)),
            "percentile" => Ok(Limiter::Percentile(percentile::Limiter::new(opts)?)),
            "backpressure" => Ok(Limiter::Backpressure(backpressure::Limiter::new(opts)?)),
        _ => panic!(
            "Unknown limiting plugin: {} valid options are 'percentile', 'backpressure', 'windowed'",
            name
        ),
    }
    }
}

opable!(Limiter, Backpressure, Percentile, Windowed);
