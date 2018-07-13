//! This module contains code used to classify messages based on
//! the rule based language.

use error::TSError;
use parser::Parsed;

mod constant;
mod matcher;
mod mimir;
mod utils;

pub use self::utils::{Classified, Classifier};

pub fn new<'c>(name: &str, opts: &'c str) -> Classifiers<'c> {
    match name {
        "mimir" => Classifiers::Mimir(mimir::Classifier::new(opts)),
        "matcher" => Classifiers::Matcher(matcher::Classifier::new(opts)),
        "constant" => Classifiers::Constant(constant::Classifier::new(opts)),
        _ => panic!(
            "Unknown classifier: {} valid options are 'constant', and 'mimir'",
            name
        ),
    }
}

#[derive(Debug)]
pub enum Classifiers<'c> {
    Constant(constant::Classifier<'c>),
    Mimir(mimir::Classifier),
    Matcher(matcher::Classifier),
}
impl<'p, 'c: 'p> Classifier<'p, 'c> for Classifiers<'c> {
    fn classify(&'c self, msg: Parsed<'p>) -> Result<Classified<'c, 'p>, TSError> {
        match self {
            Classifiers::Constant(c) => c.classify(msg),
            Classifiers::Mimir(c) => c.classify(msg),
            Classifiers::Matcher(c) => c.classify(msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use classifier;
    use classifier::Classifier;
    use parser;
    use parser::Parser;
    #[test]
    fn constant_classifier() {
        let s = "Example";
        let t = String::from("Classification");
        let p = parser::new("raw", "");
        let c = classifier::new("constant", t.as_str());
        let classified = p.parse(s)
            .and_then(|parsed| c.classify(parsed))
            .expect("classification failed!");
        assert_eq!(t, classified.classification);
    }

}
