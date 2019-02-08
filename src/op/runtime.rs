//! This module contains operators that provide a runtime to interact with data

pub mod mimir;
#[cfg(feature = "php-runtime")]
pub mod php;
use crate::errors::*;
use crate::pipeline::prelude::*;

#[derive(Debug)]
pub enum Runtime {
    #[cfg(feature = "php-runtime")]
    Php(php::Runtime),
    Mimir(mimir::Runtime),
}

impl Runtime {
    pub fn create(name: &str, opts: &ConfValue) -> Result<Self> {
        match name {
            #[cfg(feature = "php-runtime")]
            "php" => Ok(Runtime::Php(php::Runtime::create(opts)?)),
            "mimir" => Ok(Runtime::Mimir(mimir::Runtime::create(opts)?)),
            _ => Err(ErrorKind::UnknownOp("runtime".into(), name.into()).into()),
        }
    }
}

#[cfg(feature = "php-runtime")]
opable!(Runtime, Php, Mimir);
#[cfg(not(feature = "php-runtime"))]
opable!(Runtime, Mimir);
