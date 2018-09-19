use error::TSError;
use output::{self, OUTPUT_DELIVERED, OUTPUT_SKIPPED};
use pipeline::{Event, Step};
use std::fs::File;
use std::io;
use std::io::prelude::*;
/// An output that write to stdout
pub struct Output {
    file: File,
}

impl Output {
    pub fn new(opts: &str) -> Self {
        let file = File::create(opts).unwrap();
        Output { file }
    }
}
impl Step for Output {
    fn apply(&mut self, event: Event) -> Result<Event, TSError> {
        if event.drop {
            OUTPUT_SKIPPED
                .with_label_values(&[output::step(&event), "stdout"])
                .inc();
            return Ok(event);
        }
        OUTPUT_DELIVERED
            .with_label_values(&[output::step(&event), "stdout"])
            .inc();
        self.file.write_all(&event.raw.as_bytes())?;
        self.file.write_all(b"\n")?;
        Ok(event)
    }
}

impl From<io::Error> for TSError {
    fn from(from: io::Error) -> TSError {
        TSError::new(format!("IO Error: {}", from).as_str())
    }
}
