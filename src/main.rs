#[macro_use]
extern crate log;
extern crate env_logger;

extern crate clap;
extern crate futures;
extern crate rand;
extern crate rdkafka;
extern crate rdkafka_sys;
extern crate serde_json;
#[macro_use]
extern crate prometheus;
#[macro_use]
extern crate lazy_static;
extern crate hyper;
extern crate mimir_rs;
extern crate regex;

pub mod classifier;
pub mod error;
pub mod grouping;
pub mod input;
pub mod limiting;
pub mod metrics;
pub mod output;
pub mod parser;
pub mod pipeline;
pub mod window;
use clap::{App, Arg};
use input::Input;
use pipeline::Pipeline;

use hyper::rt::Future;
use hyper::service::service_fn;
use hyper::Server;

use rdkafka::util::get_rdkafka_version;

use std::thread;

// consumer example: https://github.com/fede1024/rust-rdkafka/blob/db7cf0883b6086300b7f61998e9fbcfe67cc8e73/examples/at_least_once.rs

fn main() {
    env_logger::init();

    println!("mimir version: {}", mimir_rs::mimir::version());
    let (version_n, version_s) = get_rdkafka_version();
    println!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let matches = App::new("traffic shaping utility")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line consumer")
        .arg(
            Arg::with_name("input")
                .short("i")
                .long("input")
                .help("input to read from. Valid options are 'stdin' and 'kafka'")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("input-config")
                .long("input-config")
                .help("Configuration for the input if required.")
                .takes_value(true)
                .default_value(""),
        )
        .arg(
            Arg::with_name("output")
                .short("o")
                .long("output")
                .help("output to send to. Valid options are 'stdout', 'kafka'")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("output-config")
                .long("output-config")
                .help("Configuration for the output of required.")
                .takes_value(true)
                .default_value(""),
        )
        .arg(
            Arg::with_name("parser")
                .short("p")
                .long("parser")
                .help("parser to use. Valid options are 'raw', and 'json'")
                .takes_value(true)
                .default_value("raw"),
        )
        .arg(
            Arg::with_name("parser-config")
                .long("parser-config")
                .help("Configuration for the parser if required.")
                .takes_value(true)
                .default_value(""),
        )
        .arg(
            Arg::with_name("classifier")
                .short("c")
                .long("classifier")
                .help("classifier to use. Valid options are 'constant'")
                .default_value("constant")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("classifier-config")
                .long("classifier-config")
                .help("Configuration for the classifier if required.")
                .takes_value(true)
                .default_value(""),
        )
        .arg(
            Arg::with_name("grouping")
                .short("g")
                .long("grouping")
                .help("grouping logic to use. Valid options are 'bucket', drop' and 'pass'")
                .takes_value(true)
                .default_value("pass"),
        )
        .arg(
            Arg::with_name("grouping-config")
                .long("grouping-config")
                .help("Configuration for the grouping.")
                .takes_value(true)
                .default_value(""),
        )
        .arg(
            Arg::with_name("limiting")
                .short("l")
                .long("limiting")
                .help("limiting logic to use. Valid options are 'percentile', 'drop', 'pass'")
                .takes_value(true)
                .default_value("pass"),
        )
        .arg(
            Arg::with_name("limiting-config")
                .long("limiting-config")
                .help("Configuration for the limiter.")
                .takes_value(true)
                .default_value(""),
        )
        .get_matches();

    let input_name = matches.value_of("input").unwrap();
    let input_config = matches.value_of("input-config").unwrap();
    let input = input::new(input_name, input_config);

    let output = matches.value_of("output").unwrap();
    let output_config = matches.value_of("output-config").unwrap();
    let output = output::new(output, output_config);

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

    let mut pipeline = Pipeline::new(parser, classifier, grouping, limiting, output);

    // We spawn the HTTP endpoint in an own thread so it doens't block the main loop.
    thread::spawn(move || {
        let addr = ([0, 0, 0, 0], 9898).into();
        println!("Listening at: http://{}", addr);
        let server = Server::bind(&addr)
            .serve(|| service_fn(metrics::dispatch))
            .map_err(|e| error!("server error: {}", e));
        hyper::rt::run(server);
    });

    let _ = input.enter_loop(&mut pipeline);
}
