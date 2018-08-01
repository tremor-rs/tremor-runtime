use error::TSError;
use pipeline::Event;
/// The output trait, it defines the required functions for any output.
pub trait Output {
    /// Sends a message, blocks until sending is done and returns an empty Ok() or
    /// an Error.
    fn send(&mut self, msg: Event) -> Result<Option<f64>, TSError>;
}
