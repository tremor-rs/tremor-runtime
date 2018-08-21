use error::TSError;
use output::{self, OUTPUT_DELIVERED, OUTPUT_SKIPPED};
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
        let step = output::step(&event);
        if event.drop {
            OUTPUT_SKIPPED.with_label_values(&[step, "stdout"]).inc();
            return Ok(event);
        };
        match event.key {
            None => println!("{}{}", self.pfx, &event.raw),
            Some(ref key) => println!("{}{} {}", self.pfx, key, &event.raw),
        }
        OUTPUT_DELIVERED.with_label_values(&[step, "stdout"]).inc();
        Ok(event)
    }
}
