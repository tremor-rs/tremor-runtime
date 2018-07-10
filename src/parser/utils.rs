use error::TSError;

/// The parser trait defines the functiuonality each parser needs
/// to provde.
pub trait Parser {
    fn parse(&self, msg: String) -> Result<Parsed, TSError>;
}

/// The Parsed struct contains the raw data we received along with
/// the parsed output based on the parser
pub struct Parsed {
    raw: String,
}

impl Parsed {
    pub fn new(raw: String) -> Self {
        Self { raw: raw }
    }
    pub fn raw(&self) -> String {
        self.raw.clone()
    }
}
