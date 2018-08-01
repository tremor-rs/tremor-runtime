use error::TSError;
use output::{OUTPUT_DELIVERED, OUTPUT_DROPPED};
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
        if !event.drop {
            let out_event = event.clone();
            OUTPUT_DELIVERED.inc();
            match event.key {
                None => println!("{}{}", self.pfx, event.raw),
                Some(key) => println!("{}{} {}", self.pfx, key, event.raw),
            }
            Ok(out_event)
        } else {
            OUTPUT_DROPPED.inc();
            Ok(event)
        }
    }
}
