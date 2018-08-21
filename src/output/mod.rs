//! This module handles outputs

mod debug;
mod elastic;
mod file;
mod kafka;
mod null;
mod stdout;

use error::TSError;
use pipeline::{Event, OutputStep, Step};
use prometheus::IntCounterVec;
use std::boxed::Box;

lazy_static! {
    /*
     * Number of errors read received from the input
     */
    pub static ref OUTPUT_DROPPED: IntCounterVec =
        register_int_counter_vec!(opts!("ts_output_dropped", "Messages dropped as part of the output."), &["step", "dest"]).unwrap();
    /*
     * Number of successes read received from the input
     */
    pub static ref OUTPUT_DELIVERED: IntCounterVec =
        register_int_counter_vec!(opts!("ts_output_delivered", "Events delivered."), &["step", "dest"]).unwrap();
    /*
     * Number of successes read received from the input
     */
    pub static ref OUTPUT_SKIPPED: IntCounterVec =
        register_int_counter_vec!(opts!("ts_output_skipped", "Events skipped as an earlier step decided to drop them."), &["step", "dest"]).unwrap();
    pub static ref OUTPUT_ERROR: IntCounterVec =
        register_int_counter_vec!(opts!("ts_output_error", "Events discarded because of errors."), &["step", "dest"]).unwrap();
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
        "file" => Output::File(file::Output::new(opts)),

        _ => panic!(
            "Unknown output: {} use kafka, stdout, es, null, file or debug",
            name
        ),
    }
}

pub fn step(event: &Event) -> &'static str {
    match event.output_step {
        OutputStep::Drop => "drop",
        OutputStep::Deliver => "deliver",
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
    File(file::Output),
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
            Output::File(o) => o.apply(msg),
        }
    }
}
