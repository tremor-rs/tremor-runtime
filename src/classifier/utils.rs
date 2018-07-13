use error::TSError;
use parser::Parsed;

/// The result of the classification
#[derive(Debug)]
pub struct Classified<'p, 'c: 'p> {
    pub msg: Parsed<'p>,
    pub classification: &'c str,
}

/// The trait defining how classifiers works

pub trait Classifier<'p, 'c: 'p> {
    /// This function is called with a parsed message
    /// and returns
    fn classify(&'c self, msg: Parsed<'p>) -> Result<Classified<'c, 'p>, TSError>;
}
