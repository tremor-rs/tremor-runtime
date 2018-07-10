//! This model handles grouping messages given on ther classification and
//! the first level of traffic shaping.

use classifier::Classified;
use error::TSError;
use parser::Parsed;

pub fn new(name: &str, _opts: &str) -> Groupers {
    match name {
        "drop" => Groupers::Boolean(BooleanGrouper::new("true")),
        "pass" => Groupers::Boolean(BooleanGrouper::new("false")),
        _ => panic!(
            "Unknown grouper: {} valid options are 'drop' and 'pass'",
            name
        ),
    }
}

pub enum Groupers {
    Boolean(BooleanGrouper),
}
impl Grouper for Groupers {
    fn group(&self, msg: Classified) -> Result<MaybeMessage, TSError> {
        match self {
            Groupers::Boolean(g) => g.group(msg),
        }
    }
}
pub struct MaybeMessage {
    pub drop: bool,
    pub msg: Parsed,
}

/// The grouper trait, defining the required functions for a grouper.
pub trait Grouper {
    fn group(&self, msg: Classified) -> Result<MaybeMessage, TSError>;
}

/// A grouper either drops or keeps all messages.
pub struct BooleanGrouper {
    drop: bool,
}
impl BooleanGrouper {
    fn new(opts: &str) -> Self {
        match opts {
            "true" => BooleanGrouper { drop: true },
            "false" => BooleanGrouper { drop: false },
            _ => panic!("Unknown option for Boolean grouper, valid options are true and false."),
        }
    }
}
impl Grouper for BooleanGrouper {
    fn group(&self, msg: Classified) -> Result<MaybeMessage, TSError> {
        Ok(MaybeMessage {
            drop: self.drop,
            msg: msg.msg,
        })
    }
}

#[cfg(test)]
mod tests {
    use classifier;
    use classifier::Classifier;
    use grouping;
    use grouping::Grouper;
    use parser;
    use parser::Parser;
    #[test]
    fn boolean_grouper() {
        let s = String::from("Example");
        let p = parser::new("raw", "");
        let c = classifier::new("static", "Classification");
        let g_d = grouping::new("drop", "");
        let g_k = grouping::new("pass", "");

        let r = p.parse(s.clone())
            .and_then(|parsed| c.classify(parsed))
            .and_then(|classified| g_d.group(classified))
            .expect("grouping failed");
        assert_eq!(r.drop, true);

        let r = p.parse(s.clone())
            .and_then(|parsed| c.classify(parsed))
            .and_then(|classified| g_k.group(classified))
            .expect("grouping failed");
        assert_eq!(r.drop, false);
    }
}
