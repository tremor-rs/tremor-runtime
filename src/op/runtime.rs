//! This module contains operators that provide a runtime to interact with data

pub mod php;
use crate::errors::*;
use crate::pipeline::prelude::*;

#[derive(Debug)]
pub enum Runtime {
    Php(php::Runtime),
}
impl Runtime {
    pub fn create(name: &str, opts: &ConfValue) -> Result<Self> {
        match name {
            "php" => Ok(Runtime::Php(php::Runtime::create(opts)?)),
            _ => Err(ErrorKind::UnknownOp("runtime".into(), name.into()).into()),
        }
    }
}

opable!(Runtime, Php);
