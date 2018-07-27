//! This model handles grouping messages given on ther classification and
//! the first level of traffic shaping.

pub use self::utils::Limiter;
use error::TSError;
use grouping::MaybeMessage;
use prometheus::Counter;
mod percentile;
mod utils;
mod windowed;

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
pub fn new(name: &str, opts: &str) -> Limiters {
    match name {
        "windowed" => Limiters::Windowed(windowed::Limiter::new(opts)),
        "percentile" => Limiters::Percentile(percentile::Limiter::new(opts)),
        "drop" => Limiters::Percentile(percentile::Limiter::new("0")),
        "pass" => Limiters::Percentile(percentile::Limiter::new("1")),
        _ => panic!(
            "Unknown limiting plugin: {} valid options are 'percentile', 'pass' (all messages), 'drop' (no messages)",
            name
        ),
    }
}

pub enum Limiters {
    Percentile(percentile::Limiter),
    Windowed(windowed::Limiter),
}

impl Limiter for Limiters {
    fn apply<'a>(&mut self, msg: MaybeMessage<'a>) -> Result<MaybeMessage<'a>, TSError> {
        if msg.drop {
            LIMITING_SKIP.inc();
            Ok(msg)
        } else {
            let r = match self {
                Limiters::Percentile(b) => b.apply(msg),
                Limiters::Windowed(b) => b.apply(msg),
            };
            match r {
                Err(_) => LIMITING_ERR.inc(),
                Ok(MaybeMessage { drop: true, .. }) => LIMITING_DROP.inc(),
                Ok(MaybeMessage { drop: false, .. }) => LIMITING_PASS.inc(),
            };
            r
        }
    }
    fn feedback(&mut self, feedback: f64) {
        match self {
            Limiters::Percentile(b) => b.feedback(feedback),
            Limiters::Windowed(b) => b.feedback(feedback),
        }
    }
}
