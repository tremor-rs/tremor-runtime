use error::TSError;
use serde_json::Value;

/// The parser trait defines the functiuonality each parser needs
/// to provde.
pub trait Parser {
    fn parse<'a>(&self, msg: &'a str) -> Result<Parsed<'a>, TSError>;
}

/// The Parsed struct contains the raw data we received along with
/// the parsed output based on the parser
#[derive(Debug)]
pub struct Parsed<'a> {
    pub raw: &'a str,
    pub parsed: Value,
}

impl<'a> Parsed<'a> {
    pub fn new(raw: &'a str, parsed: Value) -> Parsed<'a> {
        Self { raw, parsed }
    }
    pub fn raw(&self) -> &str {
        self.raw
    }
}
