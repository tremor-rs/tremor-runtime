use classifier::Classified;
use error::TSError;
use parser::Parsed;

pub struct MaybeMessage<'p> {
    pub drop: bool,
    pub msg: Parsed<'p>,
}

/// The grouper trait, defining the required functions for a grouper.
pub trait Grouper {
    fn group<'c, 'p>(&mut self, msg: Classified<'c, 'p>) -> Result<MaybeMessage<'p>, TSError>;
}
