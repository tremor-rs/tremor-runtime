use error::TSError;
use pipeline::{Event, Step};
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
        event.json = Some(event.raw.clone());
        Ok(event)
    }
}
