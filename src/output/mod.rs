//! This module handles outputs

mod debug;
mod es_async;
mod kafka;
mod stdout;
mod utils;

use error::TSError;
use pipeline::{Event, Step};
use prometheus::IntCounter;

lazy_static! {
    /*
     * Number of errors read received from the input
     */
    pub static ref OUTPUT_DROPPED: IntCounter =
        register_int_counter!(opts!("ts_output_droped", "Messages dropped as part of the output.")).unwrap();
    /*
     * Number of successes read received from the input
     */
    pub static ref OUTPUT_DELIVERED: IntCounter =
        register_int_counter!(opts!("ts_output_delivered", "Messages delivered.")).unwrap();
    /*
     * Number of successes read received from the input
     */
    pub static ref OUTPUT_SKIPED: IntCounter =
        register_int_counter!(opts!("ts_output_skipped", "Messages skipped as an earlier step decided to drop them.")).unwrap();
}

// Constructor function that given the name of the output will return the correct
// output connector.
pub fn new(name: &str, opts: &str) -> Output {
    match name {
        "kafka" => Output::Kafka(kafka::Output::new(opts)),
        "stdout" => Output::Stdout(stdout::Output::new(opts)),
        "debug" => Output::Debug(debug::Output::new(opts)),
        "es" => Output::Elastic(es_async::Output::new(opts)),

        _ => panic!("Unknown output: {} use kafka, stdout, es or debug", name),
    }
}

/// Enum of all output connectors we have implemented.
/// New connectors need to be added here.
pub enum Output {
    Kafka(kafka::Output),
    Elastic(es_async::Output),
    Stdout(stdout::Output),
    Debug(debug::Output),
}

/// Implements the Output trait for the enum.
/// this needs to ba adopted for each implementation.
impl Step for Output {
    fn apply(&mut self, msg: Event) -> Result<Event, TSError> {
        match self {
            Output::Kafka(o) => o.apply(msg),
            Output::Elastic(o) => o.apply(msg),
            Output::Stdout(o) => o.apply(msg),
            Output::Debug(o) => o.apply(msg),
        }
    }
}
