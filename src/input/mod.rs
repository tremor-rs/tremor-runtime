//! This module defines the interface for input connectors.
//! Input connectors are used to get data into the system
//! to then be processed.

mod kafka;
use pipeline::Msg;
use std::io::{self, BufRead, BufReader};
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
        "stdin" => Inputs::Stdin(StdinInput::new(opts)),
        _ => panic!("Unknown classifier: {}", name),
    }
}

pub enum Inputs {
    Kafka(kafka::Input),
    Stdin(StdinInput),
}

impl Input for Inputs {
    fn enter_loop(&self, pipelines: Vec<mpsc::SyncSender<Msg>>) {
        match self {
            Inputs::Kafka(i) => i.enter_loop(pipelines),
            Inputs::Stdin(i) => i.enter_loop(pipelines),
        }
    }
}

pub struct StdinInput {
    ff: bool,
}

impl StdinInput {
    fn new(opts: &str) -> Self {
        if opts == "fire-and-forget" {
            Self { ff: true }
        } else {
            Self { ff: false }
        }
    }
}

impl Input for StdinInput {
    fn enter_loop(&self, pipelines: Vec<mpsc::SyncSender<Msg>>) {
        let stdin = io::stdin();
        let stdin = BufReader::new(stdin);
        for line in stdin.lines() {
            debug!("Line: {:?}", line);
            match line {
                Ok(line) => {
                    INPUT_OK.inc();
                    let msg = Msg::new(None, line);
                    if self.ff {
                        let _ = pipelines[0].try_send(msg);
                    } else {
                        let _ = pipelines[0].send(msg);
                    }
                }
                Err(_) => INPUT_ERR.inc(),
            }
        }
    }
}
