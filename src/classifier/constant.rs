use error::TSError;
use parser::Parsed;

use classifier::utils::{Classified, Classifier as ClassifierT};
/// A constant classifier, it will classify all mesages the same way.
#[derive(Debug)]
pub struct Classifier<'c> {
    classification: &'c str,
}

impl<'c> Classifier<'c> {
    pub fn new(opts: &'c str) -> Self {
        Classifier {
            classification: opts,
        }
    }
}

impl<'p, 'c: 'p> ClassifierT<'p, 'c> for Classifier<'c> {
    fn classify(&'c self, msg: Parsed<'p>) -> Result<Classified<'c, 'p>, TSError> {
        Ok(Classified {
            msg: msg,
            classification: self.classification,
        })
    }
}
