use error::TSError;
use pipeline::Event;

/// The grouper trait, defining the required functions for a grouper.
pub trait Step {
    fn apply(&mut self, msg: Event) -> Result<Event, TSError>;
    fn shutdown(&mut self) {
        // By default, assume nothing to do
        // Step implementations should override as required
    }
}
