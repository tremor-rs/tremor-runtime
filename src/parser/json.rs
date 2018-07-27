use error::TSError;
use parser::utils::{Parsed, Parser as ParserT};
use serde_json::{self, Value};
use std::convert::From;
/// The Raw Parser is a simple parser that performs no action on the
/// data and just hands on `raw`
pub struct Parser {}
impl Parser {
    pub fn new(_opts: &str) -> Self {
        Parser {}
    }
}

impl ParserT for Parser {
    fn parse<'a>(&self, msg: &'a str) -> Result<Parsed<'a>, TSError> {
        let parsed = serde_json::from_str::<Value>(msg)?;
        Ok(Parsed::new(msg, parsed))
    }
}

impl From<serde_json::Error> for TSError {
    fn from(e: serde_json::Error) -> TSError {
        TSError::new(format!("Serade error: {}", e).as_str())
    }
}
