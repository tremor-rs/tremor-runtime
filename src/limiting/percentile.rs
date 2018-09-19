use error::TSError;
use limiting::Feedback;
use pipeline::{Event, Step};
use prometheus::Gauge;
use rand::prelude::*;
use std::f64;

lazy_static! {
    static ref PERCENTILE_GAUGE: Gauge = register_gauge!(opts!(
        "ts_limiting_percentile",
        "Current limiting percentile."
    )).unwrap();
}
/// A Limitier algorith that just lets trough a percentage of messages
pub struct Limiter {
    percentile: f64,
    upper_limit: f64,
    lower_limit: f64,
    adjustment: f64,
}

impl Limiter {
    pub fn new(opts: &str) -> Self {
        let opts: Vec<&str> = opts.split(':').collect();
        match *opts.as_slice() {
            [percentile] => {
                let percentile = percentile.parse().unwrap();
                PERCENTILE_GAUGE.set(percentile);
                Limiter {
                    percentile,
                    lower_limit: 0.0,
                    upper_limit: f64::INFINITY,
                    adjustment: 0.0,
                }
            }
            [percentile, lower, upper, adjust] => {
                let percentile = percentile.parse().unwrap();
                let lower_limit = lower.parse().unwrap();
                let upper_limit = upper.parse().unwrap();
                let adjustment = adjust.parse().unwrap();
                PERCENTILE_GAUGE.set(percentile);

                Limiter {
                    percentile,
                    lower_limit,
                    upper_limit,
                    adjustment,
                }
            }
            _ => panic!("Bad configuration for limiter use <percentile>[:<lower limit>:<upper limit>:<adjustment>]."),
        }
    }
}

fn max(f1: f64, f2: f64) -> f64 {
    if f1 >= f2 {
        f1
    } else {
        f2
    }
}

fn min(f1: f64, f2: f64) -> f64 {
    if f1 <= f2 {
        f1
    } else {
        f2
    }
}

impl Step for Limiter {
    fn apply(&mut self, event: Event) -> Result<Event, TSError> {
        let mut event = Event::from(event);
        let mut rng = thread_rng();
        event.drop = rng.gen::<f64>() > self.percentile;
        Ok(event)
    }
}
impl Feedback for Limiter {
    fn feedback(&mut self, feedback: f64) {
        match feedback {
            f if f > self.upper_limit => {
                // TODO: We should not set this to `adjustment` but perhaps
                // re-set to `adjustment` on ever X messages if it's 0 to test
                // if we have recovered.
                self.percentile = max(self.adjustment, self.percentile - self.adjustment);
                PERCENTILE_GAUGE.set(self.percentile);
                debug!("v {} ({})", self.percentile, f);
            }
            f if f < self.lower_limit => {
                self.percentile = min(1.0, self.percentile + self.adjustment);
                PERCENTILE_GAUGE.set(self.percentile);
                debug!("^ {} ({})", self.percentile, f);
            }
            f => debug!("= {} ({})", self.percentile, f),
        }
    }
}

#[cfg(test)]
mod tests {
    use classifier;
    use grouping;
    use limiting;
    use parser;
    use pipeline::{Event, Step};
    use utils;

    #[test]
    fn keep_all() {
        let s = Event::new("Example", None, utils::nanotime());

        let mut p = parser::new("raw", "");
        let mut c = classifier::new("constant", "Classification");
        let mut g = grouping::new("pass", "");
        let mut b = limiting::new("percentile", "1:0:1:0.1");

        let msg = p
            .apply(s)
            .and_then(|parsed| c.apply(parsed))
            .and_then(|classified| g.apply(classified))
            .and_then(|msg| b.apply(msg))
            .expect("handling failed!");
        assert_eq!(msg.drop, false);
    }

    #[test]
    fn keep_non() {
        let s = Event::new("Example", None, utils::nanotime());

        let mut p = parser::new("raw", "");
        let mut c = classifier::new("constant", "Classification");
        let mut g = grouping::new("pass", "");
        let mut b = limiting::new("percentile", "0:0:1:0.1");

        let msg = p
            .apply(s)
            .and_then(|parsed| c.apply(parsed))
            .and_then(|classified| g.apply(classified))
            .and_then(|msg| b.apply(msg))
            .expect("handling failed!");
        assert_eq!(msg.drop, true);
    }

}
