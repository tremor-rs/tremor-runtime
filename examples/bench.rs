#![cfg_attr(feature = "cargo-clippy", deny(clippy))]
#[macro_use]
extern crate log;
extern crate env_logger;

#[macro_use]
extern crate clap;
extern crate chrono;
extern crate elastic;
extern crate futures;
extern crate futures_state_stream;
extern crate hdrhistogram;
extern crate hyper;
extern crate lazy_static;
extern crate libc;
extern crate mimir;
extern crate prometheus;
extern crate rand;
extern crate rdkafka;
extern crate rdkafka_sys;
extern crate regex;
extern crate serde;
extern crate serde_json;
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
extern crate tremor_runtime;

use tremor_runtime::args;
use tremor_runtime::input;
use tremor_runtime::metrics;
use tremor_runtime::pipeline;
use tremor_runtime::utils;
use tremor_runtime::version;

use pipeline::{Event, Msg};

use hyper::rt::Future;
use hyper::service::service_fn;
use hyper::Server;

use std::io::Write;
#[cfg(not(feature = "try_spmc"))]
use std::sync::mpsc;
use std::thread;
use utils::nanotime;

use input::Input;

/// println_stderr and run_command_or_fail are copied from rdkafka-sys
macro_rules! println_stderr(
    ($($arg:tt)*) => { {
        let r = writeln!(&mut ::std::io::stderr(), $($arg)*);
        r.expect("failed printing to stderr");
    } }
);

fn main() {
    env_logger::init();

    version::print();

    let matches = args::parse().get_matches();

    if !matches.is_present("no-metrics-endpoint") {
        thread::spawn(move || {
            let addr = ([0, 0, 0, 0], 9898).into();
            println!("Spawning metrics endpoint on: http://{}", addr);
            let server = Server::bind(&addr)
                .serve(|| service_fn(metrics::dispatch))
                .map_err(|e| error!("server error: {}", e));
            hyper::rt::run(server);
        });
    } else {
        println_stderr!("Not spawning metrics endpoint");
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
                let mut pipeline = args::setup_worker(&matches.clone());
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
                    let mut pipeline = args::setup_worker(&matches.clone());
                    let app_epoch = nanotime();
                    for msg in rx.iter() {
                        let event = Event::new(msg.payload.as_str(), msg.ctx, app_epoch);
                        let _ = pipeline.run(event);
                    }

                    pipeline.shutdown();
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
