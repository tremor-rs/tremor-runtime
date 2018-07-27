use error::TSError;
use grouping::MaybeMessage;
use prometheus::IntCounter;

lazy_static! {
    /*
     * Number of errors read received from the input
     */
    pub static ref OUTPUT_DROPPED: IntCounter =
        register_int_counter!(opts!("ts_output_droped", "Messages dropped.")).unwrap();
    /*
     * Number of successes read received from the input
     */
    pub static ref OUTPUT_DELIVERED: IntCounter =
        register_int_counter!(opts!("ts_output_delivered", "Messages delivered.")).unwrap();
}

/// The output trait, it defines the required functions for any output.
pub trait Output {
    /// Sends a message, blocks until sending is done and returns an empty Ok() or
    /// an Error.
    fn send<'a>(&mut self, msg: MaybeMessage<'a>) -> Result<Option<f64>, TSError>;
}
