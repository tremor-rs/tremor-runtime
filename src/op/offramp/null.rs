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

//! # Null Offramp
//!
//! The `null` offramp simply discards every event it receives
//!
//! ## Configuration
//!
//! This operator takes no configuration

use crate::errors::*;
use crate::pipeline::prelude::*;
/// An offramp that write to stdout
#[derive(Debug, Clone)]
pub struct Offramp {}

impl Offramp {
    pub fn create(_opts: &ConfValue) -> Result<Self> {
        Ok(Offramp {})
    }
}

impl Opable for Offramp {
    fn exec(&mut self, input: EventData) -> EventResult {
        EventResult::Return(input.make_return(Ok(None)))
    }
    opable_types!(ValueType::Any, ValueType::None);
}
