use error::TSError;
use pipeline::Event;

/// The grouper trait, defining the required functions for a grouper.
pub trait Step {
    fn apply(&mut self, msg: Event) -> Result<Event, TSError>;
}
