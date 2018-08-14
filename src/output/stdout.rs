use error::TSError;
use output::{OUTPUT_DELIVERED, OUTPUT_SKIPPED};
use pipeline::{Event, Step};

/// An output that write to stdout
pub struct Output {
    pfx: String,
}

impl Output {
    pub fn new(opts: &str) -> Self {
        Output {
            pfx: String::from(opts),
        }
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

        match event.key {
            None => println!("{}{}", self.pfx, event.raw),
            Some(key) => println!("{}{} {}", self.pfx, key, event.raw),
        }
        Ok(out_event)
    }
}
