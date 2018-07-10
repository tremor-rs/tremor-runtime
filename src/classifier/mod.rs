//! This module contains code used to classify messages based on
//! the rule based language.

use error::TSError;
use parser::Parsed;

pub fn new<'c>(name: &str, opts: &'c str) -> Classifiers<'c> {
    match name {
        "static" => Classifiers::Static(StaticClassifier::new(opts)),
        _ => panic!("Unknown classifier: {} valid options are 'static'", name),
    }
}

pub enum Classifiers<'c> {
    Static(StaticClassifier<'c>),
}
impl<'c> Classifier<'c> for Classifiers<'c> {
    fn classify<'p: 'c>(&'c self, msg: Parsed<'p>) -> Result<Classified<'c, 'p>, TSError> {
        match self {
            Classifiers::Static(c) => c.classify(msg),
        }
    }
}

/// The result of the classification
pub struct Classified<'c, 'p> {
    pub msg: Parsed<'p>,
    pub classification: &'c str,
}

/// The trait defining how classifiers works
pub trait Classifier<'c> {
    /// This function is called with a parsed message
    /// and returns
    fn classify<'p: 'c>(&'c self, msg: Parsed<'p>) -> Result<Classified<'c, 'p>, TSError>;
}

/// A static classifier, it will classify all mesages the same way.
pub struct StaticClassifier<'c> {
    classification: &'c str,
}

impl<'c> StaticClassifier<'c> {
    fn new(opts: &'c str) -> Self {
        StaticClassifier {
            classification: opts,
        }
    }
}

impl<'c> Classifier<'c> for StaticClassifier<'c> {
    fn classify<'p: 'c>(&'c self, msg: Parsed<'p>) -> Result<Classified<'c, 'p>, TSError> {
        Ok(Classified {
            msg: msg,
            classification: self.classification,
        })
    }
}

#[cfg(test)]
mod tests {
    use classifier;
    use classifier::Classifier;
    use parser;
    use parser::Parser;
    #[test]
    fn static_classifier() {
        let s = "Example";
        let t = String::from("Classification");
        let p = parser::new("raw", "");
        let c = classifier::new("static", t.as_str());
        let classified = p.parse(s)
            .and_then(|parsed| c.classify(parsed))
            .expect("classification failed!");
        assert_eq!(t, classified.classification);
    }
}
