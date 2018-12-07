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

//! # Grouping and static limiting
//!
//! This model handles grouping messages given on their classification and
//! the first level of traffic shaping.

pub mod bucket;

use crate::errors::*;
use crate::pipeline::prelude::*;

#[derive(Debug)]
pub enum Grouper {
    Bucket(bucket::Grouper),
}

impl Grouper {
    pub fn create(name: &str, opts: &ConfValue) -> Result<Grouper> {
        match name {
            "bucket" => Ok(Grouper::Bucket(bucket::Grouper::create(opts)?)),
            _ => Err(ErrorKind::UnknownOp("grouper".into(), name.into()).into()),
        }
    }
}

opable!(Grouper, Bucket);
