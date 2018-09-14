use error::TSError;
use output::{self, OUTPUT_DELIVERED, OUTPUT_SKIPPED};
use pipeline::{Event, Step};
use reqwest::Client;

/// Influx output connector
pub struct Output {
    url: String
}

impl Output {

    pub fn new(opts: &str) -> Self {
        Output{
            url: String::from(opts)
        }
    }
}
impl Step for Output {
    fn apply(&mut self, event: Event) -> Result<Event, TSError> {
        let out_event = event.clone();
        if event.drop {
            OUTPUT_SKIPPED
                .with_label_values(&[output::step(&event), "null"])
                .inc();
        } else {
            OUTPUT_DELIVERED
                .with_label_values(&[output::step(&event), "null"])
                .inc();
            let client = Client::new();
            client.post(self.url.as_str()).body(event.raw).send()?;
        };
        Ok(out_event)
    }
}
