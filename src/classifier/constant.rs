use error::TSError;
use pipeline::{Event, Step};

/// A constant classifier, it will classify all mesages the same way.
#[derive(Debug)]
pub struct Classifier {
    classification: String,
}

impl Classifier {
    pub fn new(opts: &str) -> Self {
        Classifier {
            classification: String::from(opts),
        }
    }
}

impl Step for Classifier {
    fn apply(&mut self, event: Event) -> Result<Event, TSError> {
        let mut event = Event::from(event);
        event.classification = self.classification.clone();
        Ok(event)
    }
}
