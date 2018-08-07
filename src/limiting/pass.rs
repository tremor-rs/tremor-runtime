use error::TSError;
use limiting::Feedback;
use pipeline::{Event, Step};

pub struct Limiter {}

impl Limiter {
    pub fn new(_opts: &str) -> Self {
        Limiter {}
    }
}

impl Step for Limiter {
    fn apply(&mut self, event: Event) -> Result<Event, TSError> {
        Ok(event)
    }
}
impl Feedback for Limiter {
    fn feedback(&mut self, _feedback: f64) {}
}
