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
use dirs;
use http::status::StatusCode;
use tremor_runtime;
use tremor_runtime::config;
use tremor_runtime::errors;
use tremor_runtime::rest;
use tremor_runtime::utils;

use clap::ArgMatches;
use std::io;
use tremor_pipeline;

use crate::errors::*;
use hashbrown::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufRead, BufReader};
use std::path::Path;
use tremor_script::Script;

enum FormatKind {
    Json,
    Yaml,
}

#[derive(Deserialize, Debug, Serialize)]
struct TargetConfig {
    instances: HashMap<String, Vec<String>>, // TODO FIXME TremorURL
}

struct TremorApp<'a> {
    app: clap::ArgMatches<'a>,
    format: FormatKind,
    config: TargetConfig,
}

fn tremor_home_dir() -> String {
    let tremor_root =  dirs::home_dir().expect("Expected home_dir");
    let tremor_root = tremor_root.to_str().expect("Expected home_dir");
    format!("{}/{}", tremor_root, ".tremor")
}

fn save_config(config: &TargetConfig) {
    let tremor_root = tremor_home_dir();
    let dot_config = format!("{}/config.yaml", tremor_root);
    let raw = serde_yaml::to_string(&config);
    let file = File::create(&dot_config);
    match raw {
        Ok(raw) => match file {
            Ok(mut f) => {
                f.write_all(&raw.into_bytes())
                    .expect("Config file is not readlable error");
            },
            _ => {
                panic!("Unable to create bootstrap config for tremor-tool");
            }
        },
        _ => {
            panic!("Expected non-empty config.yaml");
        }
    }
}

fn load_config() -> TargetConfig {
    let tremor_root = tremor_home_dir();
    let dot_config = format!("{}/config.yaml", tremor_root);
    let mut default = TargetConfig {
        instances: HashMap::new(),
    };
    default.instances.insert(
        "default".to_string(),
        vec!["http://localhost:9898/".to_string()],
    );
    let meta = fs::metadata(&tremor_root);
    match meta {
        Ok(meta) => {
            if meta.is_dir() {
                let meta = fs::metadata(dot_config.clone());
                match meta {
                    Ok(meta) => {
                        if meta.is_file() {
                            let mut source = File::open(&dot_config).expect("Unable to open tremor tool config file");
                            let mut raw = vec![];
                            source
                                .read_to_end(&mut raw)
                                .expect("Expected readable file");

                            let r: TargetConfig = serde_yaml::from_slice(raw.as_slice()).expect("Unable to map slice to yaml");
                            r
                        } else {
                            default
                        }
                    }
                    Err(_file) => {
                        save_config(&default);
                        load_config()
                    }
                }
            } else {
                default
            }
        }
        Err(_dir) => {
            fs::create_dir(&tremor_root).expect("Unable to create tremor home directory");
            load_config()
        }
    }
}

impl<'a> TremorApp<'a> {
    fn new(app: &'a clap::App) -> Self {
        let cmd = app.clone().get_matches();
        let format = match cmd.value_of("format") {
            Some("json") => FormatKind::Json,
            Some("yaml") => FormatKind::Yaml,
            _ => FormatKind::Yaml,
        };
        Self {
            app: cmd,
            format,
            config: load_config(),
        }
    }
}

fn usage(app: &TremorApp) -> Result<()> {
    println!("{}", app.app.usage());
    println!();
    println!();
    Ok(())
}

fn slurp(file: &str) -> config::Config {
    let file = File::open(file).expect("could not open file");
    let buffered_reader = BufReader::new(file);
    serde_yaml::from_reader(buffered_reader).expect("Failed to read config file")
}

fn run(mut app: TremorApp) -> Result<()> {
    let cmd = app.app.clone();
    if let Some(matches) = cmd.subcommand_matches("script") {
        script_cmd(&mut app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("pipe") {
        pipe_cmd(&app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("api") {
        conductor_cmd(&mut app, &matches)
    } else {
        usage(&app)
    }
}
fn main() {
    use clap::App;
    let yaml = load_yaml!("./cli.yaml");
    let app = App::from_yaml(yaml);
    let app = TremorApp::new(&app);
    if let Err(e) = run(app) {
        eprintln!("Error: {}", e)
    }
}

fn script_cmd(app: &mut TremorApp, cmd: &ArgMatches) -> Result<()> {
    if let Some(matches) = cmd.subcommand_matches("run") {
        script_run_cmd(&matches)
    } else {
        usage(&app)
    }
}

fn script_run_cmd(cmd: &ArgMatches) -> Result<()> {
    let f = cmd.value_of("SCRIPT").ok_or("SCRIPT not provided")?;
    let mut file = File::open(f)?;
    let mut script = String::new();
    file.read_to_string(&mut script)
        .expect("Unable to read the file");

    let input: Box<BufRead> = match cmd.value_of("DATA") {
        None => Box::new(BufReader::new(io::stdin())),
        Some(data) => Box::new(BufReader::new(File::open(data)?)),
    };

    let mut s = Script::parse(&script, &tremor_script::registry())?;
    for (num, line) in input.lines().enumerate() {
        let l = line?;
        match serde_json::from_str(&l) {
            Ok(mut json) => {
                let mut m = tremor_script::ValueMap::new();
                let _result = s.run(&(), &mut json, &mut m)?;
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
    Ok(())
}

fn pipe_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    if let Some(matches) = cmd.subcommand_matches("run") {
        pipe_run_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("dot") {
        pipe_to_dot_cmd(app, &matches)
    } else {
        usage(app)
    }
}

fn pipe_run_cmd(_app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    let script = cmd.value_of("CONFIG").ok_or("CONFIG not provided")?;
    let config = slurp(script);
    let runtime = tremor_runtime::incarnate(config)?;
    let pipeline = &runtime.pipes[0];
    let mut flow = pipeline.to_executable_graph(tremor_pipeline::buildin_ops)?;

    let input: Box<BufRead> = match cmd.value_of("DATA") {
        None => Box::new(BufReader::new(io::stdin())),
        Some(data) => Box::new(BufReader::new(File::open(data)?)),
    };

    for (num, line) in input.lines().enumerate() {
        let l = line?;
        let json: serde_json::Value = serde_json::from_str(&l)?;
        let m = tremor_script::ValueMap::new();

        let eventset = flow.enqueue(
            "in1",
            tremor_pipeline::Event {
                is_batch: false,
                id: num as u64,
                ingest_ns: utils::nanotime(),
                meta: m,
                value: tremor_pipeline::EventValue::JSON(json),
                kind: None,
            },
        )?;

        for (stream, event) in eventset {
            println!("{} {}: {:?}", num, stream, event);
        }
    }
    Ok(())
}

fn pipe_to_dot_cmd(_app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    let script = cmd.value_of("CONFIG").ok_or("CONFIG not provided")?;
    let config = slurp(script);
    let runtime = tremor_runtime::incarnate(config)?;
    let pipeline = &runtime.pipes[0];
    println!("{}", pipeline.to_dot());
    Ok(())
}

fn conductor_cmd(app: &mut TremorApp, cmd: &ArgMatches) -> Result<()> {
    if let Some(matches) = cmd.subcommand_matches("version") {
        conductor_version_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("binding") {
        conductor_binding_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("pipeline") {
        conductor_pipeline_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("onramp") {
        conductor_onramp_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("offramp") {
        conductor_offramp_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("target") {
        conductor_target_cmd(app, &matches)
    } else {
        usage(app)
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
}

/////////////////////////
// API host targetting //
/////////////////////////

fn conductor_target_cmd(app: &mut TremorApp, cmd: &ArgMatches) -> Result<()> {
    if let Some(matches) = cmd.subcommand_matches("list") {
        conductor_target_list_cmd(app, &matches);
    } else if let Some(matches) = cmd.subcommand_matches("create") {
        conductor_target_create_cmd(app, &matches);
    } else if let Some(matches) = cmd.subcommand_matches("delete") {
        conductor_target_delete_cmd(app, &matches);
    } else {
        println!("{}", serde_json::to_string(&app.config.instances)?)
    }
    Ok(())
}

/////////////////////////////
// API endpoint targetting //
/////////////////////////////

fn conductor_target_list_cmd(app: &TremorApp, _cmd: &ArgMatches) {
    println!("{:?}", app.config.instances.keys());
}

fn conductor_target_create_cmd(app: &mut TremorApp, cmd: &ArgMatches) {
    let id = cmd
        .value_of("TARGET_ID")
        .ok_or("TARGET_ID not provided")
        .expect("TARGET_ID not provided");
    let path_to_file = cmd.value_of("SOURCE").ok_or("SOURCE not provided").expect("SOURCE not provided");
    let json = load(path_to_file.to_string()).expect("Bad source path");
    let endpoints: Vec<String> = serde_json::from_value(json).expect("Unable to parse json");
    app.config.instances.insert(id.to_string(), endpoints);
    save_config(&app.config);
}

fn conductor_target_delete_cmd(app: &mut TremorApp, cmd: &ArgMatches) {
    let id = cmd
        .value_of("TARGET_ID")
        .ok_or("TARGET_ID not provided")
        .expect("TARGET_ID not provided");
    app.config.instances.remove(&id.to_string());
    save_config(&app.config);
}

//////////////////
// API Version  //
//////////////////

fn conductor_version_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let endpoint = &format!("{}/version", base_url);
    let mut response = reqwest::get(endpoint)?;
    let version: Version = response.json()?;
    println!(
        "{}",
        match cmd.value_of("format") {
            Some("json") => serde_json::to_string(&version)?,
            Some("yaml") => serde_yaml::to_string(&version)?,
            _ => serde_json::to_string(&version)?,
        }
    );
    Ok(())
}

////////////////////////////
// API Onramp subcommands //
////////////////////////////

//////////////////////////////
// API Pipeline subcommands //
//////////////////////////////

fn conductor_pipeline_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    if let Some(matches) = cmd.subcommand_matches("list") {
        conductor_pipeline_list_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("fetch") {
        conductor_pipeline_get_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("delete") {
        conductor_pipeline_delete_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("create") {
        conductor_pipeline_create_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("instance") {
        conductor_pipeline_instance_cmd(app, &matches)
    } else {
        usage(app)
    }
}

fn conductor_pipeline_get_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let endpoint = format!("/pipeline/{id}", id = id);
    let mut response = restc.get(endpoint)?.send()?;
    handle_response(&mut response)
}

fn conductor_pipeline_delete_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let endpoint = format!("/pipeline/{id}", id = id);
    let mut response = restc.delete(endpoint)?.send()?;
    handle_response(&mut response)
}

fn conductor_pipeline_list_cmd(app: &TremorApp, _cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let endpoint = "/pipeline".to_string();
    let mut response = restc.get(endpoint)?.send()?;
    handle_response(&mut response)
}

fn conductor_pipeline_create_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let path_to_file = cmd.value_of("SOURCE").ok_or("SOURCE not provided")?;
    let json = load(path_to_file.to_string())?;
    let ser = ser(&app, &json)?;
    let endpoint = "/pipeline";
    let mut response = restc
        .post(endpoint.to_string())?
        .header("content-type", content_type(app))
        .header("accept", accept(app))
        .body(ser)
        .send()?;
    handle_response(&mut response)
}

fn conductor_pipeline_instance_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let aid = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let sid = cmd
        .value_of("INSTANCE_ID")
        .ok_or("INSTANCE_ID not provided")?;
    let endpoint = format!("/pipeline/{id}/{instance}", id = aid, instance = sid);
    let mut response = restc.get(endpoint)?.send()?;
    handle_response(&mut response)
}

/////////////////////////////
// API Binding subcommands //
/////////////////////////////

fn conductor_binding_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    if let Some(matches) = cmd.subcommand_matches("list") {
        conductor_binding_list_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("fetch") {
        conductor_binding_get_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("delete") {
        conductor_binding_delete_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("create") {
        conductor_binding_create_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("instance") {
        conductor_binding_instance_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("activate") {
        conductor_binding_activate_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("deactivate") {
        conductor_binding_deactivate_cmd(app, &matches)
    } else {
        usage(app)
    }
}

fn conductor_binding_get_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let endpoint = format!("/binding/{id}", id = id);
    let mut response = restc.get(endpoint)?.send()?;
    handle_response(&mut response)
}

fn conductor_binding_delete_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let endpoint = format!("/binding/{id}", id = id);
    let mut response = restc.delete(endpoint)?.send()?;
    handle_response(&mut response)
}

fn conductor_binding_list_cmd(app: &TremorApp, _cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let endpoint = "/binding".to_string();
    let mut response = restc.get(endpoint)?.send()?;
    handle_response(&mut response)
}

fn conductor_binding_create_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let path_to_file = cmd.value_of("SOURCE").ok_or("SOURCE not provided")?;
    let json = load(path_to_file.to_string())?;
    let ser = ser(&app, &json)?;
    let endpoint = "/binding";
    let mut response = restc
        .post(endpoint.to_string())?
        .header("content-type", content_type(app))
        .header("accept", accept(app))
        .body(ser)
        .send()?;
    handle_response(&mut response)
}

fn conductor_binding_instance_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let aid = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let sid = cmd
        .value_of("INSTANCE_ID")
        .ok_or("INSTANCE_ID not provided")?;
    let endpoint = format!("/binding/{id}/{instance}", id = aid, instance = sid);
    let mut response = restc.get(endpoint)?.send()?;
    handle_response(&mut response)
}

fn conductor_binding_activate_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let aid = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let sid = cmd
        .value_of("INSTANCE_ID")
        .ok_or("INSTANCE_ID not provided")?;
    let endpoint = format!("/binding/{id}/{instance}", id = aid, instance = sid);
    let path_to_file = cmd.value_of("SOURCE").ok_or("SOURCE not provided")?;
    let json = load(path_to_file.to_string())?;
    let ser = ser(&app, &json)?;
    let mut response = restc
        .post(endpoint.to_string())?
        .header("content-type", content_type(app))
        .header("accept", accept(app))
        .body(ser)
        .send()?;
    handle_response(&mut response)
}

fn conductor_binding_deactivate_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let aid = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let sid = cmd
        .value_of("INSTANCE_ID")
        .ok_or("INSTANCE_ID not provided")?;
    let endpoint = format!("/binding/{id}/{instance}", id = aid, instance = sid);
    let mut response = restc.delete(endpoint)?.send()?;
    handle_response(&mut response)
}

//////////////////////////
// API Offramp commands //
//////////////////////////

fn conductor_offramp_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    if let Some(matches) = cmd.subcommand_matches("list") {
        conductor_offramp_list_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("fetch") {
        conductor_offramp_get_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("delete") {
        conductor_offramp_delete_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("create") {
        conductor_offramp_create_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("instance") {
        conductor_offramp_instance_cmd(app, &matches)
    } else {
        usage(app)
    }
}

fn conductor_offramp_get_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let endpoint = format!("/offramp/{id}", id = id);
    let mut response = restc.get(endpoint)?.send()?;
    handle_response(&mut response)
}

fn conductor_offramp_delete_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let endpoint = format!("/offramp/{id}", id = id);
    let mut response = restc.delete(endpoint)?.send()?;
    handle_response(&mut response)
}

fn conductor_offramp_list_cmd(app: &TremorApp, _cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let endpoint = "/offramp".to_string();
    let mut response = restc.get(endpoint)?.send()?;
    handle_response(&mut response)
}

fn conductor_offramp_create_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let path_to_file = cmd.value_of("SOURCE").ok_or("SOURCE not provided")?;
    let json = load(path_to_file.to_string())?;
    let ser = ser(&app, &json)?;
    let endpoint = "/offramp";
    let mut response = restc
        .post(endpoint.to_string())?
        .header("content-type", content_type(app))
        .header("accept", accept(app))
        .body(ser)
        .send()?;
    handle_response(&mut response)
}

fn conductor_offramp_instance_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let aid = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let sid = cmd
        .value_of("INSTANCE_ID")
        .ok_or("INSTANCE_ID not provided")?;
    let endpoint = format!("/offramp/{id}/{instance}", id = aid, instance = sid);
    let mut response = restc.get(endpoint)?.send()?;
    handle_response(&mut response)
}

/////////////////////////
// API Onramp commands //
/////////////////////////

fn conductor_onramp_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    if let Some(matches) = cmd.subcommand_matches("list") {
        conductor_onramp_list_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("fetch") {
        conductor_onramp_get_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("delete") {
        conductor_onramp_delete_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("create") {
        conductor_onramp_create_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("instance") {
        conductor_onramp_instance_cmd(app, &matches)
    } else {
        usage(app)
    }
}

fn conductor_onramp_get_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let endpoint = format!("/onramp/{id}", id = id);
    let mut response = restc.get(endpoint)?.send()?;
    handle_response(&mut response)
}

fn conductor_onramp_delete_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let endpoint = format!("/onramp/{id}", id = id);
    let mut response = restc.delete(endpoint)?.send()?;
    handle_response(&mut response)
}

fn conductor_onramp_list_cmd(app: &TremorApp, _cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let endpoint = "/onramp".to_string();
    let mut response = restc.get(endpoint)?.send()?;
    handle_response(&mut response)
}

fn conductor_onramp_create_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let path_to_file = cmd.value_of("SOURCE").ok_or("SOURCE not provided")?;
    let json = load(path_to_file.to_string())?;
    let ser = ser(&app, &json)?;
    let endpoint = "/onramp";
    let mut response = restc
        .post(endpoint.to_string())?
        .header("content-type", content_type(app))
        .header("accept", accept(app))
        .body(ser)
        .send()?;
    handle_response(&mut response)
}

fn conductor_onramp_instance_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let aid = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let sid = cmd
        .value_of("INSTANCE_ID")
        .ok_or("INSTANCE_ID not provided")?;
    let endpoint = format!("/onramp/{id}/{instance}", id = aid, instance = sid);
    let mut response = restc.get(endpoint)?.send()?;
    handle_response(&mut response)
}

//////////////////
// Utility code //
//////////////////

fn load(path_to_file: String) -> Result<serde_json::Value> {
    let mut source = File::open(&path_to_file)?;
    let ext = Path::new(&path_to_file)
        .extension()
        .and_then(OsStr::to_str)
        .ok_or("Could not create fail path")?;
    let mut raw = vec![];
    source
        .read_to_end(&mut raw)
        .expect("Expected readable file");

    if ext == "yaml" || ext == "yml" {
        Ok(serde_yaml::from_slice(raw.as_slice())?)
    } else if ext == "json" {
        Ok(serde_json::from_slice(raw.as_slice())?)
    } else {
        Err(Error::from(format!("Unsupported format: {}", ext)))
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

fn handle_response(response: &mut reqwest::Response) -> Result<()> {
    let status = response.status();
    match status {
        StatusCode::OK => println!("{}", &response.text()?),
        StatusCode::CREATED => println!("{}", &response.text()?),
        StatusCode::NOT_FOUND => eprintln!("Not found"),
        StatusCode::CONFLICT => eprintln!("Conflict"),
        _ => eprintln!("Unexpected response ( status: {} )", status.as_u16()),
    };
    Ok(())
}

fn ser(app: &TremorApp, json: &serde_json::Value) -> Result<String> {
    Ok(match app.format {
        FormatKind::Json => serde_json::to_string(&json)?,
        FormatKind::Yaml => serde_yaml::to_string(&json)?,
    })
}
