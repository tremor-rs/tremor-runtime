//! This module contains the definition of different parsers

mod influx;
mod json;
mod raw;

use error::TSError;
use pipeline::{Event, Step};

pub fn new(name: &str, opts: &str) -> Parser {
    match name {
        "raw" => Parser::Raw(raw::Parser::new(opts)),
        "json" => Parser::JSON(json::Parser::new(opts)),
        "influx" => Parser::Influx(influx::Parser::new(opts)),
        _ => panic!(
            "Unknown parser: '{}' valid options are 'raw', 'influx' and 'json'",
            name
        ),
    }
}

#[derive(Clone)]
pub enum Parser {
    Raw(raw::Parser),
    JSON(json::Parser),
    Influx(influx::Parser),
}

impl Step for Parser {
    fn apply(&mut self, event: Event) -> Result<Event, TSError> {
        match self {
            Parser::Raw(p) => p.apply(event),
            Parser::JSON(p) => p.apply(event),
            Parser::Influx(p) => p.apply(event),
        }
    }
}

#[cfg(test)]
mod tests {
    use parser;
    use pipeline::{Event, Step};
    use utils;

    #[test]
    fn raw_parser() {
        let s = Event::new("Example", false, utils::nanotime());
        let mut p = parser::new("raw", "");
        let event = p.apply(s).expect("parsing failed!");
        assert_eq!("Example", event.raw);
    }

    #[test]
    fn json_parser() {
        let s = Event::new("[1]", false, utils::nanotime());
        let mut p = parser::new("json", "");
        let parsed = p.apply(s).expect("parsing failed!");
        assert_eq!("[1]", parsed.raw);
        assert_eq!("[1]", parsed.json.unwrap());
    }

}
