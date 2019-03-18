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

#![warn(unused_extern_crates)]
#![recursion_limit = "1024"]
#![cfg_attr(feature = "cargo-clippy", deny(clippy::all))]
#[macro_use]
extern crate serde_derive;
#[cfg(feature = "kafka")]
extern crate rdkafka_sys;
#[cfg(feature = "mssql")]
extern crate tiberius;
#[cfg(feature = "kafka")]
extern crate tokio_threadpool;

use clap::load_yaml;
use http::status::StatusCode;
use tremor_runtime::config;
use tremor_runtime::dynamic;
use tremor_runtime::errors;
use tremor_runtime::rest;
use tremor_runtime::utils;

use clap::ArgMatches;
use std::io;
use tremor_pipeline;

use crate::errors::*;
use hashbrown::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufRead, BufReader};
use std::path::Path;
use tremor_script::Script;

enum FormatKind {
    Json,
    Yaml,
}

struct TremorApp<'a> {
    app: clap::ArgMatches<'a>,
    format: FormatKind,
}

impl<'a> TremorApp<'a> {
    fn new(app: &'a clap::App) -> Self {
        let cmd = app.clone().get_matches();
        let format = match cmd.value_of("format") {
            Some("json") => FormatKind::Json,
            Some("yaml") => FormatKind::Yaml,
            _ => FormatKind::Yaml,
        };
        Self { app: cmd, format }
    }
}

fn usage(app: &TremorApp) {
    println!("{}", app.app.usage());
    println!();
    println!();
}

fn slurp(file: &str) -> config::Config {
    let file = File::open(file).expect("could not open file");
    let buffered_reader = BufReader::new(file);
    serde_yaml::from_reader(buffered_reader).unwrap()
}

fn main() {
    use clap::App;
    let yaml = load_yaml!("./cli.yaml");
    let app = App::from_yaml(yaml);
    let app = TremorApp::new(&app);
    let cmd = &app.app;

    if cmd.is_present("script") {
        script_cmd(&app, &cmd.subcommand_matches("script").unwrap());
    } else if cmd.is_present("pipe") {
        pipe_cmd(&app, &cmd.subcommand_matches("pipe").unwrap());
    } else if cmd.is_present("api") {
        conductor_cmd(&app, &cmd.subcommand_matches("api").unwrap());
    } else {
        usage(&app);
    }
}

fn script_cmd(app: &TremorApp, cmd: &ArgMatches) {
    if cmd.is_present("run") {
        script_run_cmd(&cmd.subcommand_matches("run").unwrap());
    } else {
        usage(&app);
    }
}

fn script_run_cmd(cmd: &ArgMatches) {
    let f = cmd.value_of("SCRIPT").unwrap();
    let mut file = File::open(f).expect("Unable to open the file");
    let mut script = String::new();
    file.read_to_string(&mut script)
        .expect("Unable to read the file");

    let input: Box<BufRead> = match cmd.value_of("DATA") {
        None => Box::new(BufReader::new(io::stdin())),
        Some(data) => Box::new(BufReader::new(File::open(data).unwrap())),
    };

    let mut s = Script::parse(&script, &tremor_script::registry()).unwrap();
    for (num, line) in input.lines().enumerate() {
        let l = line.unwrap();
        match serde_json::from_str(&l) {
            Ok(mut json) => {
                let mut m = tremor_script::ValueMap::new();
                let _result = s.run(&(), &mut json, &mut m).unwrap();
                match s.run(&(), &mut json, &mut m) {
                    Ok(_success) => {
                        println!(
                            "{}",
                            serde_json::json!({"status": true, "data": l, "meta": m, "line": num})
                        );
                    }
                    Err(reason) => {
                        let err_str = reason.to_string();
                        println!(
                            "{}",
                            serde_json::json!({"status": false, "data": l, "error": err_str, "meta": m, "line": num})
                        );
                    }
                };
            }
            Err(reason) => {
                let err_str = reason.to_string();
                println!(
                    "{}",
                    serde_json::json!({"status": false, "data": l, "error": err_str, "line": num})
                );
            }
        }
    }
}

fn pipe_cmd(app: &TremorApp, cmd: &ArgMatches) {
    if cmd.is_present("run") {
        pipe_run_cmd(app, &cmd.subcommand_matches("run").unwrap());
    } else if cmd.is_present("dot") {
        pipe_to_dot_cmd(app, &cmd.subcommand_matches("dot").unwrap());
    } else {
        usage(app);
    }
}

fn pipe_run_cmd(_app: &TremorApp, cmd: &ArgMatches) {
    let script = cmd.value_of("CONFIG").unwrap();
    let config = slurp(script);
    let runtime = dynamic::incarnate(config).unwrap();
    let pipeline = &runtime.pipes[0];
    let mut flow = pipeline
        .to_executable_graph(tremor_pipeline::buildin_ops)
        .unwrap();

    let input: Box<BufRead> = match cmd.value_of("DATA") {
        None => Box::new(BufReader::new(io::stdin())),
        Some(data) => Box::new(BufReader::new(File::open(data).unwrap())),
    };

    for (num, line) in input.lines().enumerate() {
        let l = line.unwrap();
        let json: serde_json::Value = serde_json::from_str(&l).unwrap();
        let m = tremor_script::ValueMap::new();

        let eventset = flow
            .enqueue(
                "in1",
                tremor_pipeline::Event {
                    is_batch: false,
                    id: num as u64,
                    ingest_ns: utils::nanotime(),
                    meta: m,
                    value: tremor_pipeline::EventValue::JSON(json),
                    kind: None,
                },
            )
            .unwrap();

        for (stream, event) in eventset {
            println!("{} {}: {:?}", num, stream, event);
        }
    }
}

fn pipe_to_dot_cmd(_app: &TremorApp, cmd: &ArgMatches) {
    let script = cmd.value_of("CONFIG").unwrap();
    let config = slurp(script);
    let runtime = dynamic::incarnate(config).unwrap();
    let pipeline = &runtime.pipes[0];
    println!("{}", pipeline.to_dot());
}

fn conductor_cmd(app: &TremorApp, cmd: &ArgMatches) {
    if cmd.is_present("version") {
        conductor_version_cmd(app, &cmd.subcommand_matches("version").unwrap());
    } else if cmd.is_present("binding") {
        conductor_binding_cmd(app, &cmd.subcommand_matches("binding").unwrap());
    } else if cmd.is_present("pipeline") {
        conductor_pipeline_cmd(app, &cmd.subcommand_matches("pipeline").unwrap());
    } else if cmd.is_present("onramp") {
        conductor_onramp_cmd(app, &cmd.subcommand_matches("onramp").unwrap());
    } else if cmd.is_present("offramp") {
        conductor_offramp_cmd(app, &cmd.subcommand_matches("offramp").unwrap());
    } else if cmd.is_present("target") {
        conductor_target_cmd(app, &cmd.subcommand_matches("target").unwrap());
    } else {
        usage(app);
    }
}

/////////////////////////
// API resource models //
/////////////////////////

#[derive(Deserialize, Debug, Serialize)]
struct Version {
    version: String,
}

#[derive(Deserialize, Debug, Serialize)]
struct Binding {
    id: String,
    description: String,
    links: HashMap<String, Vec<String>>,
    // TODO    instances: Vec<>
}

/////////////////////////
// API host targetting //
/////////////////////////

fn conductor_target_cmd(_app: &TremorApp, _cmd: &ArgMatches) {
    // TODO
}

//////////////////
// API Version  //
//////////////////

fn conductor_version_cmd(_app: &TremorApp, cmd: &ArgMatches) {
    let endpoint = "http://localhost:9898/version";
    let mut response = reqwest::get(endpoint).unwrap();
    let version: Version = response.json().unwrap();
    println!(
        "{}",
        match cmd.value_of("format") {
            Some("json") => serde_json::to_string(&version).unwrap(),
            Some("yaml") => serde_yaml::to_string(&version).unwrap(),
            _ => serde_json::to_string(&version).unwrap(),
        }
    );
}

////////////////////////////
// API Onramp subcommands //
////////////////////////////

//////////////////////////////
// API Pipeline subcommands //
//////////////////////////////

fn conductor_pipeline_cmd(app: &TremorApp, cmd: &ArgMatches) {
    if cmd.is_present("list") {
        conductor_pipeline_list_cmd(app, &cmd.subcommand_matches("list").unwrap());
    } else if cmd.is_present("fetch") {
        conductor_pipeline_get_cmd(app, &cmd.subcommand_matches("fetch").unwrap());
    } else if cmd.is_present("delete") {
        conductor_pipeline_delete_cmd(app, &cmd.subcommand_matches("delete").unwrap());
    } else if cmd.is_present("create") {
        conductor_pipeline_create_cmd(app, &cmd.subcommand_matches("create").unwrap());
    } else if cmd.is_present("instance") {
        conductor_pipeline_instance_cmd(app, &cmd.subcommand_matches("instance").unwrap());
    } else {
        usage(app);
    }
}

fn conductor_pipeline_get_cmd(_app: &TremorApp, cmd: &ArgMatches) {
    let restc = rest::HttpC::new("http://localhost:9898".to_string());
    let id = cmd.value_of("ARTEFACT_ID").unwrap();
    let endpoint = format!("/pipeline/{id}", id = id);
    let mut response = restc.get(endpoint).send().unwrap();
    handle_response(&mut response);
}

fn conductor_pipeline_delete_cmd(_app: &TremorApp, cmd: &ArgMatches) {
    let restc = rest::HttpC::new("http://localhost:9898".to_string());
    let id = cmd.value_of("ARTEFACT_ID").unwrap();
    let endpoint = format!("/pipeline/{id}", id = id);
    let mut response = restc.delete(endpoint).send().unwrap();
    handle_response(&mut response);
}

fn conductor_pipeline_list_cmd(_app: &TremorApp, _cmd: &ArgMatches) {
    let endpoint = "http://localhost:9898/pipeline".to_string();
    let mut response = reqwest::get(&endpoint).unwrap();
    handle_response(&mut response);
}

fn conductor_pipeline_create_cmd(app: &TremorApp, cmd: &ArgMatches) {
    let restc = rest::HttpC::new("http://localhost:9898".to_string());
    let path_to_file = cmd.value_of("SOURCE").unwrap();
    let json = load(path_to_file.to_string()).unwrap();
    let ser = ser(&app, &json).unwrap();
    let endpoint = "/pipeline";
    let mut response = restc
        .post(endpoint.to_string())
        .header("content-type", content_type(app))
        .header("accept", accept(app))
        .body(ser)
        .send()
        .unwrap();
    handle_response(&mut response);
}

fn conductor_pipeline_instance_cmd(_app: &TremorApp, cmd: &ArgMatches) {
    let _restc = rest::HttpC::new("http://localhost:9898".to_string());
    let aid = cmd.value_of("ARTEFACT_ID").unwrap();
    let sid = cmd.value_of("INSTANCE_ID").unwrap();
    let endpoint = format!("/pipeline/{id}/{instance}", id = aid, instance = sid);
    let mut response = reqwest::get(&endpoint).unwrap();
    handle_response(&mut response);
}

/////////////////////////////
// API Binding subcommands //
/////////////////////////////

fn conductor_binding_cmd(app: &TremorApp, cmd: &ArgMatches) {
    if cmd.is_present("list") {
        conductor_binding_list_cmd(app, &cmd.subcommand_matches("list").unwrap());
    } else if cmd.is_present("fetch") {
        conductor_binding_get_cmd(app, &cmd.subcommand_matches("fetch").unwrap());
    } else if cmd.is_present("delete") {
        conductor_binding_delete_cmd(app, &cmd.subcommand_matches("delete").unwrap());
    } else if cmd.is_present("create") {
        conductor_binding_create_cmd(app, &cmd.subcommand_matches("create").unwrap());
    } else if cmd.is_present("instance") {
        conductor_binding_instance_cmd(app, &cmd.subcommand_matches("instance").unwrap());
    } else if cmd.is_present("activate") {
        conductor_binding_activate_cmd(app, &cmd.subcommand_matches("activate").unwrap());
    } else if cmd.is_present("deactivate") {
        conductor_binding_deactivate_cmd(app, &cmd.subcommand_matches("deactivate").unwrap());
    } else {
        usage(app);
    }
}

fn conductor_binding_get_cmd(_app: &TremorApp, cmd: &ArgMatches) {
    let restc = rest::HttpC::new("http://localhost:9898".to_string());
    let id = cmd.value_of("ARTEFACT_ID").unwrap();
    let endpoint = format!("/binding/{id}", id = id);
    let mut response = restc.get(endpoint).send().unwrap();
    handle_response(&mut response);
}

fn conductor_binding_delete_cmd(_app: &TremorApp, cmd: &ArgMatches) {
    let restc = rest::HttpC::new("http://localhost:9898".to_string());
    let id = cmd.value_of("ARTEFACT_ID").unwrap();
    let endpoint = format!("/binding/{id}", id = id);
    let mut response = restc.delete(endpoint).send().unwrap();
    handle_response(&mut response);
}

fn conductor_binding_list_cmd(_app: &TremorApp, _cmd: &ArgMatches) {
    let endpoint = "http://localhost:9898/binding".to_string();
    let mut response = reqwest::get(&endpoint).unwrap();
    handle_response(&mut response);
}

fn conductor_binding_create_cmd(app: &TremorApp, cmd: &ArgMatches) {
    let restc = rest::HttpC::new("http://localhost:9898".to_string());
    let path_to_file = cmd.value_of("SOURCE").unwrap();
    let json = load(path_to_file.to_string()).unwrap();
    let ser = ser(&app, &json).unwrap();
    let endpoint = "/binding";
    let mut response = restc
        .post(endpoint.to_string())
        .header("content-type", content_type(app))
        .header("accept", accept(app))
        .body(ser)
        .send()
        .unwrap();
    handle_response(&mut response);
}

fn conductor_binding_instance_cmd(_app: &TremorApp, cmd: &ArgMatches) {
    let _restc = rest::HttpC::new("http://localhost:9898".to_string());
    let aid = cmd.value_of("ARTEFACT_ID").unwrap();
    let sid = cmd.value_of("INSTANCE_ID").unwrap();
    let endpoint = format!("/binding/{id}/{instance}", id = aid, instance = sid);
    let mut response = reqwest::get(&endpoint).unwrap();
    handle_response(&mut response);
}

fn conductor_binding_activate_cmd(app: &TremorApp, cmd: &ArgMatches) {
    let restc = rest::HttpC::new("http://localhost:9898".to_string());
    let aid = cmd.value_of("ARTEFACT_ID").unwrap();
    let sid = cmd.value_of("INSTANCE_ID").unwrap();
    let endpoint = format!("/binding/{id}/{instance}", id = aid, instance = sid);
    let path_to_file = cmd.value_of("SOURCE").unwrap();
    let json = load(path_to_file.to_string()).unwrap();
    let ser = ser(&app, &json).unwrap();
    let mut response = restc
        .post(endpoint.to_string())
        .header("content-type", content_type(app))
        .header("accept", accept(app))
        .body(ser)
        .send()
        .unwrap();
    handle_response(&mut response);
}

fn conductor_binding_deactivate_cmd(_app: &TremorApp, cmd: &ArgMatches) {
    let restc = rest::HttpC::new("http://localhost:9898".to_string());
    let aid = cmd.value_of("ARTEFACT_ID").unwrap();
    let sid = cmd.value_of("INSTANCE_ID").unwrap();
    let endpoint = format!("/binding/{id}/{instance}", id = aid, instance = sid);
    let mut response = restc.delete(endpoint).send().unwrap();
    handle_response(&mut response);
}

//////////////////////////
// API Offramp commands //
//////////////////////////

fn conductor_offramp_cmd(app: &TremorApp, cmd: &ArgMatches) {
    if cmd.is_present("list") {
        conductor_offramp_list_cmd(app, &cmd.subcommand_matches("list").unwrap());
    } else if cmd.is_present("fetch") {
        conductor_offramp_get_cmd(app, &cmd.subcommand_matches("fetch").unwrap());
    } else if cmd.is_present("delete") {
        conductor_offramp_delete_cmd(app, &cmd.subcommand_matches("delete").unwrap());
    } else if cmd.is_present("create") {
        conductor_offramp_create_cmd(app, &cmd.subcommand_matches("create").unwrap());
    } else if cmd.is_present("instance") {
        conductor_offramp_instance_cmd(app, &cmd.subcommand_matches("instance").unwrap());
    } else {
        usage(app);
    }
}

fn conductor_offramp_get_cmd(_app: &TremorApp, cmd: &ArgMatches) {
    let restc = rest::HttpC::new("http://localhost:9898".to_string());
    let id = cmd.value_of("ARTEFACT_ID").unwrap();
    let endpoint = format!("/offramp/{id}", id = id);
    let mut response = restc.get(endpoint).send().unwrap();
    handle_response(&mut response);
}

fn conductor_offramp_delete_cmd(_app: &TremorApp, cmd: &ArgMatches) {
    let restc = rest::HttpC::new("http://localhost:9898".to_string());
    let id = cmd.value_of("ARTEFACT_ID").unwrap();
    let endpoint = format!("/offramp/{id}", id = id);
    let mut response = restc.delete(endpoint).send().unwrap();
    handle_response(&mut response);
}

fn conductor_offramp_list_cmd(_app: &TremorApp, _cmd: &ArgMatches) {
    let endpoint = "http://localhost:9898/offramp".to_string();
    let mut response = reqwest::get(&endpoint).unwrap();
    handle_response(&mut response);
}

fn conductor_offramp_create_cmd(app: &TremorApp, cmd: &ArgMatches) {
    let restc = rest::HttpC::new("http://localhost:9898".to_string());
    let path_to_file = cmd.value_of("SOURCE").unwrap();
    let json = load(path_to_file.to_string()).unwrap();
    let ser = ser(&app, &json).unwrap();
    let endpoint = "/offramp";
    let mut response = restc
        .post(endpoint.to_string())
        .header("content-type", content_type(app))
        .header("accept", accept(app))
        .body(ser)
        .send()
        .unwrap();
    handle_response(&mut response);
}

fn conductor_offramp_instance_cmd(_app: &TremorApp, cmd: &ArgMatches) {
    let _restc = rest::HttpC::new("http://localhost:9898".to_string());
    let aid = cmd.value_of("ARTEFACT_ID").unwrap();
    let sid = cmd.value_of("INSTANCE_ID").unwrap();
    let endpoint = format!("/offramp/{id}/{instance}", id = aid, instance = sid);
    let mut response = reqwest::get(&endpoint).unwrap();
    handle_response(&mut response);
}

/////////////////////////
// API Onramp commands //
/////////////////////////

fn conductor_onramp_cmd(app: &TremorApp, cmd: &ArgMatches) {
    if cmd.is_present("list") {
        conductor_onramp_list_cmd(app, &cmd.subcommand_matches("list").unwrap());
    } else if cmd.is_present("fetch") {
        conductor_onramp_get_cmd(app, &cmd.subcommand_matches("fetch").unwrap());
    } else if cmd.is_present("delete") {
        conductor_onramp_delete_cmd(app, &cmd.subcommand_matches("delete").unwrap());
    } else if cmd.is_present("create") {
        conductor_onramp_create_cmd(app, &cmd.subcommand_matches("create").unwrap());
    } else if cmd.is_present("instance") {
        conductor_onramp_instance_cmd(app, &cmd.subcommand_matches("instance").unwrap());
    } else {
        usage(app);
    }
}

fn conductor_onramp_get_cmd(_app: &TremorApp, cmd: &ArgMatches) {
    let restc = rest::HttpC::new("http://localhost:9898".to_string());
    let id = cmd.value_of("ARTEFACT_ID").unwrap();
    let endpoint = format!("/onramp/{id}", id = id);
    let mut response = restc.get(endpoint).send().unwrap();
    handle_response(&mut response);
}

fn conductor_onramp_delete_cmd(_app: &TremorApp, cmd: &ArgMatches) {
    let restc = rest::HttpC::new("http://localhost:9898".to_string());
    let id = cmd.value_of("ARTEFACT_ID").unwrap();
    let endpoint = format!("/onramp/{id}", id = id);
    let mut response = restc.delete(endpoint).send().unwrap();
    handle_response(&mut response);
}

fn conductor_onramp_list_cmd(_app: &TremorApp, _cmd: &ArgMatches) {
    let endpoint = "http://localhost:9898/onramp".to_string();
    let mut response = reqwest::get(&endpoint).unwrap();
    handle_response(&mut response);
}

fn conductor_onramp_create_cmd(app: &TremorApp, cmd: &ArgMatches) {
    let restc = rest::HttpC::new("http://localhost:9898".to_string());
    let path_to_file = cmd.value_of("SOURCE").unwrap();
    let json = load(path_to_file.to_string()).unwrap();
    let ser = ser(&app, &json).unwrap();
    let endpoint = "/onramp";
    let mut response = restc
        .post(endpoint.to_string())
        .header("content-type", content_type(app))
        .header("accept", accept(app))
        .body(ser)
        .send()
        .unwrap();
    handle_response(&mut response);
}

fn conductor_onramp_instance_cmd(_app: &TremorApp, cmd: &ArgMatches) {
    let _restc = rest::HttpC::new("http://localhost:9898".to_string());
    let aid = cmd.value_of("ARTEFACT_ID").unwrap();
    let sid = cmd.value_of("INSTANCE_ID").unwrap();
    let endpoint = format!("/onramp/{id}/{instance}", id = aid, instance = sid);
    let mut response = reqwest::get(&endpoint).unwrap();
    handle_response(&mut response);
}

//////////////////
// Utility code //
//////////////////

fn load(path_to_file: String) -> Result<serde_json::Value> {
    let mut source = File::open(&path_to_file).unwrap();
    let ext = Path::new(&path_to_file)
        .extension()
        .and_then(OsStr::to_str)
        .unwrap();
    let mut raw = vec![];
    source
        .read_to_end(&mut raw)
        .expect("Expected readable file");

    if ext == "yaml" || ext == "yml" {
        Ok(serde_yaml::from_slice(raw.as_slice())?)
    } else if ext == "json" {
        Ok(serde_json::from_slice(raw.as_slice())?)
    } else {
        panic!("Unsupported format: {}", ext)
    }
}

fn content_type(app: &TremorApp) -> &'static str {
    match app.format {
        FormatKind::Json => "application/json",
        FormatKind::Yaml => "application/yaml",
    }
}

fn accept(app: &TremorApp) -> &'static str {
    match app.format {
        FormatKind::Json => "application/json",
        FormatKind::Yaml => "application/yaml",
    }
}

fn handle_response(response: &mut reqwest::Response) {
    let status = response.status();
    match status {
        StatusCode::OK => println!("{}", &response.text().unwrap()),
        StatusCode::CREATED => println!("{}", &response.text().unwrap()),
        StatusCode::NOT_FOUND => eprintln!("Not found"),
        StatusCode::CONFLICT => eprintln!("Conflict"),
        _ => eprintln!("Unexpected response ( status: {} )", status.as_u16()),
    };
}

fn ser(app: &TremorApp, json: &serde_json::Value) -> Result<String> {
    Ok(match app.format {
        FormatKind::Json => serde_json::to_string(&json)?,
        FormatKind::Yaml => serde_yaml::to_string(&json)?,
    })
}
