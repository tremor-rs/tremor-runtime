//! This module defines the interface for input connectors.
//! Input connectors are used to get data into the system
//! to then be processed.

mod blaster;
mod file;
mod kafka;
mod mssql;
mod stdin;
use pipeline::Msg;

extern crate spmc;
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
    #[cfg(not(feature = "try_spmc"))]
    fn enter_loop(&mut self, pipeline: Vec<mpsc::SyncSender<Msg>>);

    #[cfg(feature = "try_spmc")]
    fn enter_loop2(&mut self, _pipeline: Vec<spmc::Sender<Msg>>) {
        panic!("!enter_loop2 not implemented for this cofiguration error");
    }
}

pub fn new(name: &str, opts: &str) -> Inputs {
    match name {
        "blaster" => Inputs::Blaster(blaster::Input::new(opts)),
        "kafka" => Inputs::Kafka(kafka::Input::new(opts)),
        "stdin" => Inputs::Stdin(stdin::Input::new(opts)),
        "file" => Inputs::File(file::Input::new(opts)),
        "mssql" => Inputs::MSSql(mssql::Input::new(opts)),
        _ => panic!("Unknown classifier: {}", name),
    }
}

pub enum Inputs {
    Blaster(blaster::Input),
    Kafka(kafka::Input),
    Stdin(stdin::Input),
    MSSql(mssql::Input),
    File(file::Input),
}

impl Input for Inputs {
    #[cfg(not(feature = "try_spmc"))]
    fn enter_loop(&mut self, pipelines: Vec<mpsc::SyncSender<Msg>>) {
        match self {
            Inputs::Blaster(i) => i.enter_loop(pipelines),
            Inputs::Kafka(i) => i.enter_loop(pipelines),
            Inputs::Stdin(i) => i.enter_loop(pipelines),
            Inputs::File(i) => i.enter_loop(pipelines),
            Inputs::MSSql(i) => i.enter_loop(pipelines),
        }
    }
    #[cfg(feature = "try_spmc")]
    fn enter_loop2(&mut self, pipelines: Vec<spmc::Sender<Msg>>) {
        match self {
            Inputs::Blaster(i) => i.enter_loop2(pipelines),
            Inputs::Kafka(i) => i.enter_loop2(pipelines),
            Inputs::Stdin(i) => i.enter_loop2(pipelines),
            Inputs::File(i) => i.enter_loop(pipelines),
            Inputs::MSSql(i) => i.enter_loop(pipelines),
        }
    }
}
