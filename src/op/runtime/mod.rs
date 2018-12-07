//! This module contains operators that provide a runtime to interact with data

pub mod php;
use errors::*;
use pipeline::prelude::*;

#[derive(Debug)]
pub enum Runtime {
    Php(php::Runtime),
}
impl Runtime {
    pub fn new(name: &str, opts: &ConfValue) -> Result<Runtime> {
        match name {
            "php" => Ok(Runtime::Php(php::Runtime::new(opts)?)),
            _ => Err(ErrorKind::UnknownOp("runtime".into(), name.into()).into()),
        }
    }
}

opable!(Runtime, Php);
