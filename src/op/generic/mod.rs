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

//! Generic operations for logic or metadata modifications.
pub mod copy;
pub mod count;
pub mod into_var;
pub mod into_vars;
pub mod route;
pub mod set;

use errors::*;
use pipeline::prelude::*;

/// Enum of all offramp connectors we have implemented.
/// New connectors need to be added here.
#[derive(Debug)]
pub enum Generic {
    Copy(copy::Op),
    Count(count::Op),
    IntoVar(into_var::Op),
    IntoVars(into_vars::Op),
    Set(set::Op),
    Route(route::Op),
}

opable!(Generic, Count, Set, Copy, Route, IntoVar, IntoVars);

impl Generic {
    pub fn new(name: &str, opts: &ConfValue) -> Result<Generic> {
        match name {
            "copy" => Ok(Generic::Copy(copy::Op::new(opts)?)),
            "count" => Ok(Generic::Count(count::Op::new(opts)?)),
            "into_var" => Ok(Generic::IntoVar(into_var::Op::new(opts)?)),
            "into_vars" => Ok(Generic::IntoVars(into_vars::Op::new(opts)?)),
            "set" => Ok(Generic::Set(set::Op::new(opts)?)),
            "route" => Ok(Generic::Route(route::Op::new(opts)?)),
            _ => Err(ErrorKind::UnknownOp("op".into(), name.into()).into()),
        }
    }
}
