//! This model handles grouping messages given on ther classification and
//! the first level of traffic shaping.

use error::TSError;
use grouping::MaybeMessage;
use rand::prelude::*;

pub fn new(name: &str, opts: &str) -> Limiters {
    match name {
        "percentile" => Limiters::Percentile(PercentileLimit::new(opts)),
        "drop" => Limiters::Percentile(PercentileLimit::new("0")),
        "pass" => Limiters::Percentile(PercentileLimit::new("1")),
        _ => panic!(
            "Unknown limiting plugin: {} valid options are 'percentile', 'pass' (all messages), 'drop' (no messages)",
            name
        ),
    }
}

pub enum Limiters {
    Percentile(PercentileLimit),
}

impl Limitier for Limiters {
    fn apply<'a>(&self, msg: MaybeMessage<'a>) -> Result<MaybeMessage<'a>, TSError> {
        match self {
            Limiters::Percentile(b) => b.apply(msg),
        }
    }
}
/// The grouper trait, defining the required functions for a grouper.
pub trait Limitier {
    fn apply<'a>(&self, msg: MaybeMessage<'a>) -> Result<MaybeMessage<'a>, TSError>;
}

/// A Limitier algorith that just lets trough a percentage of messages
pub struct PercentileLimit {
    percentile: f64,
}

impl PercentileLimit {
    fn new(opts: &str) -> Self {
        PercentileLimit {
            percentile: opts.parse().unwrap(),
        }
    }
}

impl Limitier for PercentileLimit {
    fn apply<'a>(&self, msg: MaybeMessage<'a>) -> Result<MaybeMessage<'a>, TSError> {
        if msg.drop {
            Ok(msg)
        } else {
            let mut rng = thread_rng();
            let drop = rng.gen::<f64>() > self.percentile;
            Ok(MaybeMessage {
                drop: drop,
                msg: msg.msg,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use classifier;
    use classifier::Classifier;
    use grouping;
    use grouping::Grouper;
    use limiting;
    use limiting::Limitier;
    use parser;
    use parser::Parser;
    #[test]
    fn keep_all() {
        let s = "Example";

        let p = parser::new("raw", "");
        let c = classifier::new("static", "Classification");
        let mut g = grouping::new("pass", "");
        let b = limiting::new("percentile", "1.0");

        let msg = p.parse(s)
            .and_then(|parsed| c.classify(parsed))
            .and_then(|classified| g.group(classified))
            .and_then(|msg| b.apply(msg))
            .expect("handling failed!");
        assert_eq!(msg.drop, false);
    }

    #[test]
    fn keep_non() {
        let s = "Example";

        let p = parser::new("raw", "");
        let c = classifier::new("static", "Classification");
        let mut g = grouping::new("pass", "");
        let b = limiting::new("percentile", "0");

        let msg = p.parse(s)
            .and_then(|parsed| c.classify(parsed))
            .and_then(|classified| g.group(classified))
            .and_then(|msg| b.apply(msg))
            .expect("handling failed!");
        assert_eq!(msg.drop, true);
    }

}
