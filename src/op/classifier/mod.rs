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

//! # Classifiers used to classify events
//!
//! Classifiers are used to classify events so that decisions can be
//! derived from this classification.

pub mod json;
use errors::*;
use pipeline::prelude::*;

#[derive(Debug)]
pub enum Classifier {
    JSON(json::Classifier),
}
impl Classifier {
    pub fn new(name: &str, opts: &ConfValue) -> Result<Classifier> {
        match name {
            "json" => Ok(Classifier::JSON(json::Classifier::new(opts)?)),
            _ => Err(ErrorKind::UnknownOp("classifier".into(), name.into()).into()),
        }
    }
}

opable!(Classifier, JSON);
