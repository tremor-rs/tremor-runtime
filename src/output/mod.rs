//! This module handles outputs

mod debug;
mod elastic;
mod kafka;
mod null;
mod stdout;

use error::TSError;
use pipeline::{Event, Step};
use prometheus::IntCounterVec;
use std::boxed::Box;

lazy_static! {
    /*
     * Number of errors read received from the input
     */
    pub static ref OUTPUT_DROPPED: IntCounterVec =
        register_int_counter_vec!(opts!("ts_output_droped", "Messages dropped as part of the output."), &["dest"]).unwrap();
    /*
     * Number of successes read received from the input
     */
    pub static ref OUTPUT_DELIVERED: IntCounterVec =
        register_int_counter_vec!(opts!("ts_output_delivered", "Messages delivered."), &["dest"]).unwrap();
    /*
     * Number of successes read received from the input
     */
    pub static ref OUTPUT_SKIPED: IntCounterVec =
        register_int_counter_vec!(opts!("ts_output_skipped", "Messages skipped as an earlier step decided to drop them."), &["dest"]).unwrap();
}

// Constructor function that given the name of the output will return the correct
// output connector.
pub fn new(name: &str, opts: &str) -> Output {
    match name {
        "kafka" => Output::Kafka(kafka::Output::new(opts)),
        "stdout" => Output::Stdout(stdout::Output::new(opts)),
        "debug" => Output::Debug(debug::Output::new(opts)),
        "es" => Output::Elastic(Box::new(elastic::Output::new(opts))),
        "null" => Output::Null(null::Output::new(opts)),

        _ => panic!("Unknown output: {} use kafka, stdout, es or debug", name),
    }
}

/// Enum of all output connectors we have implemented.
/// New connectors need to be added here.
pub enum Output {
    Kafka(kafka::Output),
    Elastic(Box<elastic::Output>),
    Stdout(stdout::Output),
    Debug(debug::Output),
    Null(null::Output),
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
            Output::Null(o) => o.apply(msg),
        }
    }
}
