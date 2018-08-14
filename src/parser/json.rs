use error::TSError;
use pipeline::{Event, Step};
use serde_json::{self, Value};
use std::convert::From;
/// The Raw Parser is a simple parser that performs no action on the
/// data and just hands on `raw`
#[derive(Clone)]
pub struct Parser {}
impl Parser {
    pub fn new(_opts: &str) -> Self {
        Parser {}
    }
}

impl Step for Parser {
    fn apply(&mut self, event: Event) -> Result<Event, TSError> {
        let mut event = Event::from(event);
        event.parsed = serde_json::from_str::<Value>(event.raw.as_str())?;
        Ok(event)
    }
}

impl From<serde_json::Error> for TSError {
    fn from(e: serde_json::Error) -> TSError {
        TSError::new(format!("Serade error: {}", e).as_str())
    }
}
