//! This module defines the interface for input connectors.
//! Input connectors are used to get data into the system
//! to then be processed.

mod file;
mod kafka;
mod stdin;
use pipeline::Msg;
use std::sync::mpsc;

use prometheus::Counter;

lazy_static! {
    /*
     * Number of errors read received from the input
     */
    static ref INPUT_ERR: Counter =
        register_counter!(opts!("ts_input_errors", "Errors from input.")).unwrap();
    /*
     * Number of successes read received from the input
     */
    static ref INPUT_OK: Counter =
        register_counter!(opts!("ts_input_successes", "Successes from input.")).unwrap();
}

pub trait Input {
    fn enter_loop(&self, pipeline: Vec<mpsc::SyncSender<Msg>>);
}

pub fn new(name: &str, opts: &str) -> Inputs {
    match name {
        "kafka" => Inputs::Kafka(kafka::Input::new(opts)),
        "stdin" => Inputs::Stdin(stdin::Input::new(opts)),
        "file" => Inputs::File(file::Input::new(opts)),
        _ => panic!("Unknown classifier: {}", name),
    }
}

pub enum Inputs {
    Kafka(kafka::Input),
    Stdin(stdin::Input),
    File(file::Input),
}

impl Input for Inputs {
    fn enter_loop(&self, pipelines: Vec<mpsc::SyncSender<Msg>>) {
        match self {
            Inputs::Kafka(i) => i.enter_loop(pipelines),
            Inputs::Stdin(i) => i.enter_loop(pipelines),
            Inputs::File(i) => i.enter_loop(pipelines),
        }
    }
}
