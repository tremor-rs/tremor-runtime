extern crate clap;

use clap::{App, Arg};

use classifier;
use grouping;
use limiting;
use output;
use parser;
use pipeline::{FilterEmpty, MainPipeline, PipelineStep, Pipelineable, SplitMultiLineBatch};

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
        .arg(
        Arg::with_name("--split-lines")
            .long("split-lines")
            .short("s")
            .required(false),
        )
        .arg(
        Arg::with_name("no-metrics-endpoint")
            .long("no-metrics-endpoint")
            .short("m")
            .help("disables the prometheus API metrics endpoint.")
            .required(false),
        )
}

pub fn setup_worker<'a>(matches: &clap::ArgMatches<'a>) -> Box<Pipelineable> {
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

    let split_lines = matches.is_present("split-lines");

    //    let drop = Rc::new(TerminalPipelineStep::new(drop_output));
    let p = MainPipeline::new(limiting, output, drop_output);
    let p = PipelineStep::new(grouping, p);
    let p = PipelineStep::new(classifier, p);
    let p = PipelineStep::new(parser, p);
    let p = FilterEmpty::new(p);
    if split_lines {
        Box::new(SplitMultiLineBatch::new(p))
    } else {
        Box::new(p)
    }
}
