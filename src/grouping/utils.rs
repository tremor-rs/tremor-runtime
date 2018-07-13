use classifier::Classified;
use error::TSError;
use parser::Parsed;

pub struct MaybeMessage<'p> {
    pub drop: bool,
    pub msg: Parsed<'p>,
}

/// The grouper trait, defining the required functions for a grouper.
pub trait Grouper {
    fn group<'p, 'c: 'p>(&mut self, msg: Classified<'p, 'c>) -> Result<MaybeMessage<'p>, TSError>;
}
