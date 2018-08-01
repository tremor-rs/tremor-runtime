//! A simple grouper that ignores the classification and either passes or drops a message.

use error::TSError;
use pipeline::{Event, Step};

/// A grouper either drops or keeps all messages.
pub struct Grouper {
    drop: bool,
}

impl Grouper {
    /// Creates a new boolean grouper with either drop set to either `"true"` or `"false"`
    pub fn new(opts: &str) -> Self {
        match opts {
            "true" => Grouper { drop: true },
            "false" => Grouper { drop: false },
            _ => panic!("Unknown option for Boolean grouper, valid options are true and false."),
        }
    }
}

impl Step for Grouper {
    fn apply(&mut self, event: Event) -> Result<Event, TSError> {
        let mut event = Event::from(event);
        event.drop = self.drop;
        Ok(event)
    }
}
