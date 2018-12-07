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

//! # Format specific parsers

pub mod influx;
pub mod json;

use crate::errors::*;
use crate::pipeline::prelude::*;

#[derive(Debug)]
pub enum Parser {
    JSON(json::Parser),
    Influx(influx::Parser),
}
impl Parser {
    pub fn create(name: &str, opts: &ConfValue) -> Result<Parser> {
        match name {
            "json" => Ok(Parser::JSON(json::Parser::create(opts)?)),
            "influx" => Ok(Parser::Influx(influx::Parser::create(opts)?)),
            _ => Err(ErrorKind::UnknownOp("parse".into(), name.into()).into()),
        }
    }
}
opable!(Parser, JSON, Influx);

#[derive(Debug)]
pub enum Renderer {
    JSON(json::Renderer),
    //Influx(influx::Renderer),
}
impl Renderer {
    pub fn create(name: &str, opts: &ConfValue) -> Result<Renderer> {
        match name {
            "json" => Ok(Renderer::JSON(json::Renderer::create(opts)?)),
            _ => Err(ErrorKind::UnknownOp("render".into(), name.into()).into()),
        }
    }
}
opable!(Renderer, JSON);
