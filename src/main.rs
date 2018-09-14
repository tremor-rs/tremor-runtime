#![cfg_attr(feature = "cargo-clippy", deny(clippy))]
#[macro_use]
extern crate log;
extern crate env_logger;
#[macro_use]
extern crate clap;
extern crate futures;
extern crate rand;
extern crate rdkafka;
extern crate rdkafka_sys;
extern crate reqwest;
#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate serde;
#[macro_use]
extern crate prometheus;
#[macro_use]
extern crate lazy_static;
extern crate chrono;
extern crate elastic;
extern crate futures_state_stream;
extern crate hdrhistogram;
extern crate hyper;
extern crate libc;
extern crate mimir;
extern crate regex;
extern crate spmc;
extern crate threadpool;
extern crate tiberius;
extern crate tokio;
extern crate tokio_current_thread;
#[allow(unused_imports)]
#[macro_use]
extern crate maplit;
extern crate window;
extern crate xz2;

mod args;
pub mod classifier;
pub mod error;
pub mod grouping;
pub mod input;
pub mod limiting;
pub mod metrics;
pub mod output;
pub mod parser;
pub mod pipeline;
mod utils;
mod version;

use input::Input;
use pipeline::{Msg, Pipeline, Pipelineable};

use hyper::rt::Future;
use hyper::service::service_fn;
use hyper::Server;

use std::io::Write;
use std::sync::mpsc;
use std::thread;

use utils::nanotime;

// consumer example: https://github.com/fede1024/rust-rdkafka/blob/db7cf0883b6086300b7f61998e9fbcfe67cc8e73/examples/at_least_once.rs

/// println_stderr and run_command_or_fail are copied from rdkafka-sys
macro_rules! println_stderr(
    ($($arg:tt)*) => { {
        let r = writeln!(&mut ::std::io::stderr(), $($arg)*);
        r.expect("failed printing to stderr");
    } }
);

fn setup_worker<'a>(matches: &clap::ArgMatches<'a>) -> Box<Pipelineable + Send + 'static> {
    let output = matches.value_of("off-ramp").unwrap();
    let output_config = matches.value_of("off-ramp-config").unwrap();
    let output = output::new(output, output_config);

    let drop_output = matches.value_of("drop-off-ramp").unwrap();
    let drop_output_config = matches.value_of("drop-off-ramp-config").unwrap();
    let drop_output = output::new(drop_output, drop_output_config);

    let parser = matches.value_of("parser").unwrap();
    let parser_config = matches.value_of("parser-config").unwrap();
    let parser = parser::new(parser, parser_config);

    let classifier = matches.value_of("classifier").unwrap();
    let classifier_config = matches.value_of("classifier-config").unwrap();
    let classifier = classifier::new(classifier, classifier_config);

    let grouping = matches.value_of("grouping").unwrap();
    let grouping_config = matches.value_of("grouping-config").unwrap();
    let grouping = grouping::new(grouping, grouping_config);

    let limiting = matches.value_of("limiting").unwrap();
    let limiting_config = matches.value_of("limiting-config").unwrap();
    let limiting = limiting::new(limiting, limiting_config);

    Pipeline::new(
        parser,
        classifier,
        grouping,
        limiting,
        output,
        drop_output,
        nanotime(),
    )
}

fn main() {
    env_logger::init();

    version::print();

    let matches = args::parse().get_matches();

    let mut txs: Vec<mpsc::SyncSender<Msg>> = Vec::new();
    let threads = value_t!(matches.value_of("pipeline-threads"), u32).unwrap();
    let input_name = matches.value_of("on-ramp").unwrap();
    let input_config = matches.value_of("on-ramp-config").unwrap();
    let mut input = input::new(input_name, input_config);
    let mut handles: Vec<thread::JoinHandle<()>> = Vec::new();

    for _tid in 0..threads {
        let (tx, rx) = mpsc::sync_channel(10);
        txs.push(tx);
        let matches = matches.clone();
        let h = thread::spawn(move || {
            let mut pipeline = setup_worker(&matches.clone());
            for msg in rx.iter() {
                let _ = pipeline.run(&msg);
            }
        });
        handles.push(h);
    }

    // We spawn the HTTP endpoint in an own thread so it doens't block the main loop.
    thread::spawn(|| {
        let addr = ([0, 0, 0, 0], 9898).into();
        println_stderr!("Listening at: http://{}", addr);
        let server = Server::bind(&addr)
            .serve(|| service_fn(metrics::dispatch))
            .map_err(|e| error!("server error: {}", e));
        hyper::rt::run(server);
    });

    #[cfg(feature = "try_spmc")]
    input.enter_loop2(txs);

    #[cfg(not(feature = "try_spmc"))]
    input.enter_loop(txs);

    let mut is_bad = false;
    while let Some(h) = handles.pop() {
        if h.join().is_err() {
            is_bad = true;
        };
    }
    if is_bad {
        ::std::process::exit(1);
    }
}
