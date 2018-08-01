use error::TSError;
use pipeline::{Event, Step};

/// The Raw Parser is a simple parser that performs no action on the
/// data and just hands on `raw`
pub struct Parser {}
impl Parser {
    pub fn new(_opts: &str) -> Self {
        Self {}
    }
}
impl Step for Parser {
    fn apply(&mut self, event: Event) -> Result<Event, TSError> {
        Ok(event)
    }
}
