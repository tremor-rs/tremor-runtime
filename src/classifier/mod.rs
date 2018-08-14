//! This module contains code used to classify messages based on
//! the rule based language.

mod constant;
mod mimir;

use error::TSError;
use pipeline::{Event, Step};

pub fn new(name: &str, opts: &str) -> Classifier {
    match name {
        "mimir" => Classifier::Mimir(mimir::Classifier::new(opts)),
        "constant" => Classifier::Constant(constant::Classifier::new(opts)),
        _ => panic!(
            "Unknown classifier: {} valid options are 'constant', and 'mimir'",
            name
        ),
    }
}

pub enum Classifier {
    Constant(constant::Classifier),
    Mimir(mimir::Classifier),
}
impl Step for Classifier {
    fn apply(&mut self, msg: Event) -> Result<Event, TSError> {
        match self {
            Classifier::Constant(c) => c.apply(msg),
            Classifier::Mimir(c) => c.apply(msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use classifier;
    use parser;
    use pipeline::{Event, Step};

    #[test]
    fn constant_classifier() {
        let s = Event::new("Example");
        let t = String::from("Classification");
        let mut p = parser::new("raw", "");
        let mut c = classifier::new("constant", t.as_str());
        let classified = p.apply(s)
            .and_then(|parsed| c.apply(parsed))
            .expect("classification failed!");
        assert_eq!(t, classified.classification);
    }

}
