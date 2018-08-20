use error::TSError;
use output::{OUTPUT_DELIVERED, OUTPUT_SKIPPED};
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
            OUTPUT_SKIPPED.with_label_values(&["stdout"]).inc();
        } else {
            OUTPUT_DELIVERED.with_label_values(&["stdout"]).inc();
        }
        let out_event = event.clone();
        self.file.write(event.raw.as_bytes())?;
        self.file.write(&['\n' as u8])?;
        Ok(out_event)
    }
}

impl From<io::Error> for TSError {
    fn from(from: io::Error) -> TSError {
        TSError::new(format!("IO Error: {}", from).as_str())
    }
}
