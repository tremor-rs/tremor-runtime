// Copyright 2018-2019, Wayfair GmbH
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
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("mapping")
                .long("mapping")
                .short("m")
                .help("disables the prometheus API metrics endpoint.")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("host")
                .long("host")
                .short("h")
                .help("host to listen to")
                .takes_value(true)
                .default_value("0.0.0.0:9898"),
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
            Arg::with_name("queue-size")
                .long("queue-sizer")
                .short("q")
                .help("queue size for the pipeline")
                .takes_value(true)
                .default_value("50"),
        )
}
