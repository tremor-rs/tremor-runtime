//! This module contains the definition of different parsers

mod json;
mod raw;
mod utils;

pub use self::utils::{Parsed, Parser};
use error::TSError;

pub fn new(name: &str, opts: &str) -> Parsers {
    match name {
        "raw" => Parsers::Raw(raw::Parser::new(opts)),
        "json" => Parsers::JSON(json::Parser::new(opts)),
        _ => panic!(
            "Unknown parser: '{}' valid options are 'raw', and 'json'",
            name
        ),
    }
}

pub enum Parsers {
    Raw(raw::Parser),
    JSON(json::Parser),
}

impl Parser for Parsers {
    fn parse(&self, msg: String) -> Result<Parsed, TSError> {
        match self {
            Parsers::Raw(p) => p.parse(msg),
            Parsers::JSON(p) => p.parse(msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use parser;
    use parser::Parser;
    #[test]
    fn raw_parser() {
        let s = String::from("Example");
        let p = parser::new("raw", "");
        let parsed = p.parse(s.clone()).expect("parsing failed!");
        assert_eq!(s, parsed.raw());
    }

    #[test]
    fn json_parser() {
        let s = String::from("[1]");
        let p = parser::new("json", "");
        let parsed = p.parse(s.clone()).expect("parsing failed!");
        assert_eq!(s, parsed.raw());
    }

    #[test]
    fn json_parser_error() {
        let s = String::from("[1");
        let p = parser::new("json", "");
        let parsed = p.parse(s.clone());
        assert!(parsed.is_err());
    }
}
