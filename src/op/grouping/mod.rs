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

//! This model handles grouping messages given on ther classification and
//! the first level of traffic shaping.

mod bucket;

use errors::*;
use pipeline::prelude::*;

#[derive(Debug)]
pub enum Grouper {
    Bucket(bucket::Grouper),
}

impl Grouper {
    pub fn new(name: &str, opts: &ConfValue) -> Result<Grouper> {
        match name {
            "bucket" => Ok(Grouper::Bucket(bucket::Grouper::new(opts)?)),
            _ => Err(ErrorKind::UnknownOp("grouper".into(), name.into()).into()),
        }
    }
}

opable!(Grouper, Bucket);
