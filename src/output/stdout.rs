use error::TSError;
use grouping::MaybeMessage;
use output::utils::{Output as OutputT, OUTPUT_DELIVERED, OUTPUT_DROPPED};

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
impl OutputT for Output {
    fn send<'a>(&mut self, msg: MaybeMessage<'a>) -> Result<Option<f64>, TSError> {
        if !msg.drop {
            OUTPUT_DELIVERED.inc();
            match msg.key {
                None => println!("{}{}", self.pfx, msg.msg.raw),
                Some(key) => println!("{}{} {}", self.pfx, key, msg.msg.raw),
            }
        } else {
            OUTPUT_DROPPED.inc()
        }

        Ok(None)
    }
}
