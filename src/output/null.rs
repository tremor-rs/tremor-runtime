use error::TSError;
use output::{OUTPUT_DELIVERED, OUTPUT_SKIPED};
use pipeline::{Event, Step};

/// An output that write to stdout
pub struct Output {}

impl Output {
    pub fn new(_opts: &str) -> Self {
        Output {}
    }
}
impl Step for Output {
    fn apply(&mut self, event: Event) -> Result<Event, TSError> {
        if !event.drop {
            OUTPUT_DELIVERED.inc();
        } else {
            OUTPUT_SKIPED.inc();
        };
        Ok(event)
    }
}
