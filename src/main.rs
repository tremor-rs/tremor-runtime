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

#![warn(unused_extern_crates)]
#![recursion_limit = "1024"]
#![cfg_attr(feature = "cargo-clippy", deny(clippy::all))]
#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate prometheus;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
#[cfg(feature = "kafka")]
extern crate rdkafka_sys;
#[cfg(feature = "mssql")]
extern crate tiberius;
#[cfg(feature = "kafka")]
extern crate tokio_threadpool;
#[macro_use]
extern crate maplit;
#[cfg(test)]
#[macro_use]
extern crate matches;

use actix;
use base64;
use chrono;
use dot;
use elastic;
use env_logger;
use libc;
use mimir;
use reqwest;
use serde_yaml;
use uuid;

#[macro_use]
mod macros;
mod args;
mod async_sink;
mod dflt;
mod error;
mod errors;
mod metrics;
pub mod onramp;
pub mod op;
mod pipeline;
mod utils;
mod version;

use crate::errors::*;
use crate::onramp::Onramp;
use crate::pipeline::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::io::Write;
use std::mem;
use std::thread;

use actix_web::{
    http,
    //error,
    middleware,
    server,
    App,
};

fn run() -> Result<()> {
    let matches = args::parse().get_matches();

    let config_file = matches.value_of("config").unwrap();
    unsafe {
        // We know that instance will only get set once at
        // the very beginning nothing can access it yet,
        // this makes it allowable to use unsafe here.
        let s = matches.value_of("instance").unwrap();
        let forget_s = mem::transmute(&s as &str);
        // This means we're going to LEAK this memory, however
        // it is fine since as we do actually need it for the
        // rest of the program execution.
        metrics::INSTANCE = forget_s;
    }
    //INSTANCE = instance.to_str();

    let mut file = File::open(config_file)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    let config: Config = serde_yaml::from_str(&contents)?;

    let render = matches.is_present("render");

    // Don't start metrics endpoint if we don't want it.
    if !render {
        let host = "0.0.0.0";
        let port = "9898";
        let host = format!("{}:{}", host, port);
        let metrics_endpoint = !matches.is_present("no-metrics-endpoint");
        thread::spawn(move || {
            let s = server::new(move || {
                let app = App::new().middleware(middleware::Logger::default());
                if metrics_endpoint {
                    app.resource("/metrics", |r| r.method(http::Method::GET).f(metrics::get))
                } else {
                    app
                }
            });
            println_stderr!("Listening at: http://{}", host);

            s.bind(&host)
                .unwrap_or_else(|_| panic!("Can not bind to {}", host))
                .run()
        });
    } else {
        println_stderr!("Not spawning metrics endpoint");
    }

    // ======
    // Pipeline setup
    // ======
    let mut pipelines = config.pipelines.clone();
    pipelines.reverse();
    let pipelines = if let Some(p) = pipelines.pop() {
        // We'll first start pipelines in order (reverse then pop), that way
        // the n'th pipeline can reference the n-1'th
        pipeline::start_pipelines(p, pipelines, HashMap::new())?
    } else {
        HashMap::new()
    };

    // Next we create our onramps
    let mut onramps = Vec::new();
    let mut i = 0;
    for onramp_yaml in config.onramps {
        i += 1;
        // Generate the config and name.
        let onramp_cfg = pipeline::value_to_conf(onramp_yaml.clone()).unwrap();
        let name = match onramp_cfg.config["name"] {
            ConfValue::String(ref name) => name.clone(),
            _ => format!("onramp-{}", i),
        };
        if onramp_cfg.namespace != "onramp" {
            return Err(format!(
                "ERROR: first step needs to be an onramp not {}",
                onramp_cfg.namespace
            )
            .into());
        }

        // Find pipelines that it connects to
        let pipeline_names: Vec<String> = match (
            &onramp_cfg.config["pipeline"],
            &onramp_cfg.config["pipelines"],
        ) {
            (ConfValue::String(ref pipeline), ConfValue::Null) => vec![pipeline.clone()],
            (ConfValue::Null, ConfValue::Sequence(ref pipelines)) => pipelines
                .iter()
                .filter_map(|b| {
                    if let ConfValue::String(s) = b {
                        Some(s.clone())
                    } else {
                        None
                    }
                })
                .collect(),
            _ => return Err(ErrorKind::OnrampMissingPipeline(onramp_cfg.namespace).into()),
        };

        // We create a list of pipelines and graphs for rendering.
        let mut onramp_pipelines = Vec::new();
        let mut onramp_graph = Vec::new();

        for name in pipeline_names {
            let p = match pipelines.get(&name.clone()) {
                Some(p) => {
                    onramp_graph.push(p.clone());
                    let p = p.lock().unwrap();
                    // Our pipeline needs to start with an onramp node.
                    match *p {
                        Graph::Pipeline { ref onramp, .. } => onramp.clone(),
                        _ => {
                            return Err(ErrorKind::OnrampMissingPipeline(onramp_cfg.namespace).into())
                        }
                    }
                }
                None => return Err(ErrorKind::UnknownPipeline(name).into()),
            };
            onramp_pipelines.push(p);
        }

        // Error if we have zero pipelines.
        if onramp_pipelines.is_empty() {
            return Err(ErrorKind::OnrampMissingPipeline(onramp_cfg.namespace).into());
        }

        // Decide if we just want to render and not start the pipelines or start
        // the pipelines.
        if render {
            let r = Graph::OnRamp {
                uuid: uuid::Uuid::new_v4(),
                name: name.clone(),
                outputs: onramp_graph,
            };
            println_stderr!("Writing: {}.dot", name);
            r.write_dot(&format!("{}.dot", name));
        } else {
            let mut onramp = onramp::create(&onramp_cfg.name, &onramp_cfg.config)?;
            onramps.push(onramp.enter_loop(onramp_pipelines.clone()));
        }
    }

    let mut i = 0;
    while let Some(h) = onramps.pop() {
        if let Err(_e) = h.join() {
            return Err(ErrorKind::OnrampError(i).into());
        };
        i += 1;
    }
    Ok(())
}

fn main() {
    env_logger::init();

    version::print();
    if let Err(ref e) = run() {
        use std::io::Write;
        let stderr = &mut ::std::io::stderr();
        let errmsg = "Error writing to stderr";

        writeln!(stderr, "error: {}", e).expect(errmsg);

        for e in e.iter().skip(1) {
            writeln!(stderr, "caused by: {}", e).expect(errmsg);
        }

        // The backtrace is not always generated. Try to run this example
        // with `RUST_BACKTRACE=1`.
        if let Some(backtrace) = e.backtrace() {
            writeln!(stderr, "backtrace: {:?}", backtrace).expect(errmsg);
        }

        ::std::process::exit(1);
    }
}
