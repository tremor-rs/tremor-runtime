use error::TSError;
use grouping::MaybeMessage;

/// The grouper trait, defining the required functions for a grouper.
pub trait Limiter {
    fn apply<'a>(&mut self, msg: MaybeMessage<'a>) -> Result<MaybeMessage<'a>, TSError>;
    fn feedback(&mut self, feedback: f64);
}
