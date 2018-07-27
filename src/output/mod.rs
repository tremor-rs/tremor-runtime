//! This module handles outputs

mod debug;
mod es_time;
mod kafka;
mod stdout;
mod utils;

pub use self::utils::Output;
use error::TSError;
use grouping::MaybeMessage;

// Constructor function that given the name of the output will return the correct
// output connector.
pub fn new(name: &str, opts: &str) -> Outputs {
    match name {
        "kafka" => Outputs::Kafka(kafka::Output::new(opts)),
        "stdout" => Outputs::Stdout(stdout::Output::new(opts)),
        "debug" => Outputs::Debug(debug::Output::new(opts)),
        "es" => Outputs::Elastic(es_time::Output::new(opts)),

        _ => panic!("Unknown output: {} use kafka, stdout, es or debug", name),
    }
}

/// Enum of all output connectors we have implemented.
/// New connectors need to be added here.
pub enum Outputs {
    Kafka(kafka::Output),
    Elastic(es_time::Output),
    Stdout(stdout::Output),
    Debug(debug::Output),
}

/// Implements the Output trait for the enum.
/// this needs to ba adopted for each implementation.
impl Output for Outputs {
    fn send(&mut self, msg: MaybeMessage) -> Result<Option<f64>, TSError> {
        match self {
            Outputs::Kafka(o) => o.send(msg),
            Outputs::Elastic(o) => o.send(msg),
            Outputs::Stdout(o) => o.send(msg),
            Outputs::Debug(o) => o.send(msg),
        }
    }
}
