// Copyright 2018, Wayfair GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use clap;

use clap::{App, Arg};

pub fn parse<'a>() -> clap::App<'a, 'a> {
    App::new("tremor-runtime")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line consumer")
        .arg(
            Arg::with_name("config")
                .long("config")
                .short("c")
                .help("config file to load")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("logger")
                .long("logger-config")
                .short("l")
                .help("log4rs configuration file")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("instance")
                .long("instance")
                .short("i")
                .help("instance id")
                .takes_value(true)
                .default_value("tremor"),
        )
        .arg(
            Arg::with_name("no-metrics-endpoint")
                .long("no-metrics-endpoint")
                .short("m")
                .help("disables the prometheus API metrics endpoint.")
                .required(false),
        )
        .arg(
            Arg::with_name("render")
                .long("render")
                .short("r")
                .help("renders the pipeline and quits")
                .required(false),
        )
}

/*
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
*/
