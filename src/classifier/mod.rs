//! This module contains code used to classify messages based on
//! the rule based language.

use error::TSError;
use parser::Parsed;

pub fn new(name: &str, opts: &str) -> Classifiers {
    match name {
        "static" => Classifiers::Static(StaticClassifier::new(opts)),
        _ => panic!("Unknown classifier: {} valid options are 'static'", name),
    }
}

pub enum Classifiers {
    Static(StaticClassifier),
}

impl Classifier for Classifiers {
    fn classify(&self, msg: Parsed) -> Result<Classified, TSError> {
        match self {
            Classifiers::Static(c) => c.classify(msg),
        }
    }
}

/// The result of the classification
pub struct Classified {
    pub msg: Parsed,
    pub classification: String,
}

/// The trait defining how classifiers works
pub trait Classifier {
    /// This function is called with a parsed message
    /// and returns
    fn classify(&self, msg: Parsed) -> Result<Classified, TSError>;
}

/// A static classifier, it will classify all mesages the same way.
pub struct StaticClassifier {
    classification: String,
}

impl StaticClassifier {
    fn new(opts: &str) -> Self {
        StaticClassifier {
            classification: String::from(opts),
        }
    }
}

impl Classifier for StaticClassifier {
    fn classify(&self, msg: Parsed) -> Result<Classified, TSError> {
        Ok(Classified {
            msg: msg,
            classification: self.classification.clone(),
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
        let s = String::from("Example");
        let t = String::from("Classification");
        let p = parser::new("raw", "");
        let c = classifier::new("static", t.clone().as_str());
        let classified = p.parse(s.clone())
            .and_then(|parsed| c.classify(parsed))
            .expect("classification failed!");
        assert_eq!(t, classified.classification);
    }
}
