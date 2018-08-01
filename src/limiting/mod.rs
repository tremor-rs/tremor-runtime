//! This model handles grouping messages given on ther classification and
//! the first level of traffic shaping.

mod feedback;
mod percentile;
mod windowed;

pub use self::feedback::Feedback;
use error::TSError;
use pipeline::{Event, Step};
use prometheus::Counter;

lazy_static! {
    /*
     * Number of messages that pass the limiting stage.
     */
    static ref LIMITING_PASS: Counter =
        register_counter!(opts!("ts_limiting_pass", "Passes during the limiting stage.")).unwrap();
    /*
     * Number of messages marked to drop the limiting stage.
     */
    static ref LIMITING_DROP: Counter =
        register_counter!(opts!("ts_limiting_drop", "Drops during the limiting stage.")).unwrap();
    /*
     * Number of messages skipped due to previous drop request
     */
    static ref LIMITING_SKIP: Counter =
        register_counter!(opts!("ts_limiting_skip", "Skipped during the limiting stage due to earlier drop request.")).unwrap();
    /*
     * Errors during the limiting stage
     */
    static ref LIMITING_ERR: Counter =
        register_counter!(opts!("ts_limiting_error", "Errors during the limiting stage.")).unwrap();
}
pub fn new(name: &str, opts: &str) -> Limiter {
    match name {
        "windowed" => Limiter::Windowed(windowed::Limiter::new(opts)),
        "percentile" => Limiter::Percentile(percentile::Limiter::new(opts)),
        "drop" => Limiter::Percentile(percentile::Limiter::new("0")),
        "pass" => Limiter::Percentile(percentile::Limiter::new("1")),
        _ => panic!(
            "Unknown limiting plugin: {} valid options are 'percentile', 'pass' (all messages), 'drop' (no messages)",
            name
        ),
    }
}

pub enum Limiter {
    Percentile(percentile::Limiter),
    Windowed(windowed::Limiter),
}

impl Step for Limiter {
    fn apply(&mut self, msg: Event) -> Result<Event, TSError> {
        if msg.drop {
            LIMITING_SKIP.inc();
            Ok(msg)
        } else {
            let r = match self {
                Limiter::Percentile(b) => b.apply(msg),
                Limiter::Windowed(b) => b.apply(msg),
            };
            match r {
                Err(_) => LIMITING_ERR.inc(),
                Ok(Event { drop: true, .. }) => LIMITING_DROP.inc(),
                Ok(Event { drop: false, .. }) => LIMITING_PASS.inc(),
            };
            r
        }
    }
}
impl Feedback for Limiter {
    fn feedback(&mut self, feedback: f64) {
        match self {
            Limiter::Percentile(b) => b.feedback(feedback),
            Limiter::Windowed(b) => b.feedback(feedback),
        }
    }
}
