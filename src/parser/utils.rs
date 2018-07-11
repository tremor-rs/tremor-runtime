use error::TSError;

/// The parser trait defines the functiuonality each parser needs
/// to provde.
pub trait Parser {
    fn parse<'a>(&self, msg: &'a str) -> Result<Parsed<'a>, TSError>;
}

/// The Parsed struct contains the raw data we received along with
/// the parsed output based on the parser
#[derive(Debug)]
pub struct Parsed<'a> {
    raw: &'a str,
}

impl<'a> Parsed<'a> {
    pub fn new(raw: &'a str) -> Parsed<'a> {
        Self { raw: raw }
    }
    pub fn raw(&self) -> &str {
        self.raw
    }
}
