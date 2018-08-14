use error::TSError;
use output::{OUTPUT_DELIVERED, OUTPUT_SKIPPED};
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
        if event.drop {
            OUTPUT_SKIPPED.with_label_values(&["null"]).inc();
        } else {
            OUTPUT_DELIVERED.with_label_values(&["null"]).inc();
        };
        Ok(event)
    }
}
