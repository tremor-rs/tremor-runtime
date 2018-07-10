//! This module contains the definition of different parsers

use error::TSError;

pub fn new(name: &str, opts: &str) -> Parsers {
    match name {
        "raw" => Parsers::Raw(RawParser::new(opts)),
        _ => panic!("Unknown parser: '{}' valid options are 'raw'", name),
    }
}

pub enum Parsers {
    Raw(RawParser),
}

impl Parser for Parsers {
    fn parse(&self, msg: String) -> Result<Parsed, TSError> {
        match self {
            Parsers::Raw(p) => p.parse(msg),
        }
    }
}

/// The Parsed struct contains the raw data we received along with
/// the parsed output based on the parser
pub struct Parsed {
    raw: String,
}

/// The parser trait defines the functiuonality each parser needs
/// to provde.
pub trait Parser {
    fn parse(&self, msg: String) -> Result<Parsed, TSError>;
}

/// The Raw Parser is a simple parser that performs no action on the
/// data and just hands on `raw`
pub struct RawParser {}
impl RawParser {
    fn new(_opts: &str) -> Self {
        RawParser {}
    }
}
impl Parser for RawParser {
    fn parse(&self, msg: String) -> Result<Parsed, TSError> {
        Ok(Parsed { raw: msg })
    }
}

#[cfg(test)]
mod tests {
    use parser;
    use parser::Parser;
    #[test]
    fn not_parser() {
        let s = String::from("Example");
        let p = parser::new("raw", "");
        let parsed = p.parse(s.clone()).expect("parsing failed!");
        assert_eq!(s, parsed.raw);
    }
}
