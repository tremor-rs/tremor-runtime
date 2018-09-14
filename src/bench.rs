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
#[cfg(feature = "try_spmc")]
extern crate spmc;
extern crate threadpool;
extern crate tiberius;
extern crate tokio_current_thread;
extern crate window;
extern crate xz2;
#[allow(unused_imports)]
#[macro_use]
extern crate maplit;
extern crate reqwest;

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

use pipeline::{Msg, Pipeline, Pipelineable};

use clap::Arg;
use hyper::rt::Future;
use hyper::service::service_fn;
use hyper::Server;

#[cfg(not(feature = "try_spmc"))]
use std::sync::mpsc;
use std::thread;

use input::Input;
use utils::nanotime;

fn bench_args<'a>(app: clap::App<'a, 'a>) -> clap::App<'a, 'a> {
    app.arg(
        Arg::with_name("bench-with-metrics-endpoint")
            .long("bench-with-metrics-endpoint")
            .help("Expose a prometheus API metrics endpoint.")
            .takes_value(true)
            .default_value("false"),
    )
}

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

    let matches = bench_args(args::parse()).get_matches();

    let with_metrics_endpoint: bool = matches
        .value_of("bench-with-metrics-endpoint")
        .unwrap()
        .parse()
        .unwrap();
    if with_metrics_endpoint {
        thread::spawn(move || {
            let addr = ([0, 0, 0, 0], 9898).into();
            println!("Spawning metrics endpoint on: http://{}", addr);
            let server = Server::bind(&addr)
                .serve(|| service_fn(metrics::dispatch))
                .map_err(|e| error!("server error: {}", e));
            hyper::rt::run(server);
        });
    } else {
        println!("Not spawning metrics endpoint");
    }

    let threads = value_t!(matches.value_of("pipeline-threads"), u32).unwrap() as usize;
    let input_name = matches.value_of("on-ramp").unwrap();
    let input_config = matches.value_of("on-ramp-config").unwrap();
    let mut input = input::new(input_name, input_config);
    let mut workers = vec![];

    #[cfg(feature = "try_spmc")]
    {
        let mut txs: Vec<spmc::Sender<Msg>> = Vec::new();

        let (tx, rx) = spmc::channel();
        txs.push(tx);

        {
            for _tid in 0..threads {
                let rx = rx.clone();
                let mut pipeline = setup_worker(&matches.clone());
                let _child = thread::spawn(move || loop {
                    let msg = rx.recv().unwrap();
                    let _ = pipeline.run(&msg);
                });
                workers.push(_child);
            }
            input.enter_loop2(txs);
        }
    }

    #[cfg(not(feature = "try_spmc"))]
    {
        let mut txs: Vec<mpsc::SyncSender<Msg>> = Vec::new();
        {
            for _tid in 0..threads {
                let (tx, rx) = mpsc::sync_channel(10);
                txs.push(tx);
                let matches = matches.clone();
                let _child = thread::spawn(move || {
                    let mut pipeline = setup_worker(&matches.clone());

                    for msg in rx.iter() {
                        let _ = pipeline.run(&msg);
                    }

                    pipeline.shutdown();

                    pipeline
                });
                workers.push(_child);
            }
        }
        input.enter_loop(txs);
    }

    for worker in workers {
        worker.join().expect("Failed to join worker thread");
    }
}
