extern crate clap;

use clap::{App, Arg};

pub fn parse<'a>() -> clap::App<'a, 'a> {
    App::new("tremor-runtime")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line consumer")
        .arg(
            Arg::with_name("on-ramp")
                .short("i")
                .long("on-ramp")
                .help("on-ramp to read from. Valid options are 'stdin' and 'kafka'")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("on-ramp-config")
                .long("on-ramp-config")
                .help("Configuration for the on-ramp if required.")
                .takes_value(true)
                .default_value(""),
        )
        .arg(
            Arg::with_name("off-ramp")
                .short("o")
                .long("off-ramp")
                .help("off-ramp to send to. Valid options are 'stdout', 'kafka', 'es' and 'debug'")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("off-ramp-config")
                .long("off-ramp-config")
                .help("Configuration for the off-ramp of required.")
                .takes_value(true)
                .default_value(""),
        )

        .arg(
            Arg::with_name("drop-off-ramp")
                .short("d")
                .long("drop-off-ramp")
                .help("off-ramp to send messages that are supposed to be dropped. Valid options are 'stdout', 'kafka', 'es' and 'debug'")
                .default_value("null")
                .required(true),
        )
        .arg(
            Arg::with_name("drop-off-ramp-config")
                .long("drop-off-ramp-config")
                .help("Configuration for the drop-off-ramp of required.")
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
                .help("classifier to use. Valid options are 'constant' or 'mimir'")
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
        .arg(
            Arg::with_name("pipeline-threads")
                .long("pipeline-threads")
                .help("Number of threads to run the pipeline.")
                .takes_value(true)
                .default_value("1"),
        )
}
