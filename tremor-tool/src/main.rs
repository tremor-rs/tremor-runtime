// Copyright 2018-2020, Wayfair GmbH
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

#![forbid(warnings)]
// This isn't a external crate so we don't worry about docs
// #![deny(missing_docs)]
#![recursion_limit = "1024"]
#![deny(
    clippy::all,
    clippy::result_unwrap_used,
    clippy::option_unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic
)]
#![allow(clippy::must_use_candidate)]

#[macro_use]
extern crate serde_derive;
// This is silly but serde is forcing you to import serde if you want serde_derive
#[allow(unused_extern_crates)]
extern crate serde;

use crate::errors::*;
use async_std::task;
use clap::load_yaml;
use clap::ArgMatches;
use dirs;
use halfbrown::HashMap;
use http::status::StatusCode;
use simd_json::borrowed::{Object, Value};
use simd_json::value::ValueBuilder;
use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::io::{BufRead, BufReader};
use std::path::Path;
use tremor_pipeline;
use tremor_runtime;
use tremor_runtime::config;
use tremor_runtime::errors;
use tremor_runtime::functions as tr_fun;
use tremor_runtime::rest;
use tremor_runtime::utils;
use tremor_script::grok;
use tremor_script::interpreter::AggrType;
use tremor_script::EventContext as Context;

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

fn tremor_home_dir() -> Result<String> {
    dirs::home_dir()
        .and_then(|s| s.to_str().map(ToString::to_string))
        .ok_or_else(|| Error::from("Expected home_dir"))
        .map(|tremor_root| format!("{}/{}", tremor_root, ".tremor"))
}

fn save_config(config: &TargetConfig) -> Result<()> {
    let tremor_root = tremor_home_dir()?;
    let dot_config = format!("{}/config.yaml", tremor_root);
    let raw = serde_yaml::to_vec(&config)?;
    let mut file = File::create(&dot_config)?;
    Ok(file.write_all(&raw)?)
}

fn load_config() -> Result<TargetConfig> {
    let tremor_root = tremor_home_dir()?;
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
                            let mut source = File::open(&dot_config)?;
                            let mut raw = vec![];
                            source.read_to_end(&mut raw)?;
                            Ok(serde_yaml::from_slice(raw.as_slice())?)
                        } else {
                            Ok(default)
                        }
                    }
                    Err(_file) => {
                        save_config(&default)?;
                        load_config()
                    }
                }
            } else {
                Ok(default)
            }
        }
        Err(_dir) => {
            fs::create_dir(&tremor_root)?;
            load_config()
        }
    }
}

impl<'a> TremorApp<'a> {
    fn try_new(app: &'a clap::App) -> Result<Self> {
        let cmd = app.clone().get_matches();
        let format = match cmd.value_of("format") {
            Some("json") => FormatKind::Json,
            Some("yaml") | _ => FormatKind::Yaml,
        };
        Ok(Self {
            app: cmd,
            format,
            config: load_config()?,
        })
    }
}

fn usage(app: &TremorApp<'_>) -> Result<()> {
    println!("{}", app.app.usage());
    println!();
    println!();
    Ok(())
}

fn slurp(file: &str) -> Result<config::Config> {
    let file = File::open(file)?;
    let buffered_reader = BufReader::new(file);
    Ok(serde_yaml::from_reader(buffered_reader)?)
}

fn run(mut app: TremorApp) -> Result<()> {
    let cmd = app.app.clone();
    if let Some(matches) = cmd.subcommand_matches("script") {
        script_cmd(&mut app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("grok") {
        grok_cmd(&mut app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("pipe") {
        pipe_cmd(&app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("api") {
        task::block_on(conductor_cmd(&mut app, &matches))
    } else {
        usage(&app)
    }
}

#[cfg_attr(tarpaulin, skip)]
fn main() -> Result<()> {
    use clap::App;
    let yaml = load_yaml!("./cli.yaml");
    let app = App::from_yaml(yaml);
    let app = TremorApp::try_new(&app)?;
    run(app)
}

fn script_cmd(app: &mut TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    tr_fun::load()?;
    if let Some(matches) = cmd.subcommand_matches("run") {
        script_run_cmd(&matches)
    } else {
        usage(&app)
    }
}

fn script_run_cmd(cmd: &ArgMatches<'_>) -> Result<()> {
    let f = cmd.value_of("SCRIPT").ok_or("SCRIPT not provided")?;
    let mut file = File::open(f)?;
    let mut script = String::new();
    file.read_to_string(&mut script)?;

    let input: Box<dyn BufRead> = match cmd.value_of("DATA") {
        None => Box::new(BufReader::new(io::stdin())),
        Some(data) => Box::new(BufReader::new(File::open(data)?)),
    };

    let context = Context::new(666, None);
    let s = tremor_script::Script::parse(&script, &*tremor_pipeline::FN_REGISTRY.lock()?)?;
    let mut state = Value::null();
    for (num, line) in input.lines().enumerate() {
        let l = line?;
        if l.is_empty() || l.starts_with('#') {
            continue;
        }
        let mut codec = tremor_runtime::codec::lookup("json")?;

        let enbuf = l.as_bytes().to_vec();
        let debuf = codec.decode(enbuf, 0);
        match debuf {
            Ok(Some(ref json)) => {
                let mut global_map = Value::from(Object::new());
                let (mut unwind_event, _) = json.parts();
                match s.run(
                    &context,
                    AggrType::Emit,
                    &mut unwind_event,
                    &mut state,
                    &mut global_map,
                ) {
                    Ok(_result) => {
                        println!(
                            "{}",
                            serde_json::json!({"status": true, "data": l, "meta": global_map, "line": num, "result": _result})
                        );
                    }
                    Err(reason) => {
                        let err_str = reason.to_string();
                        println!(
                            "{}",
                            serde_json::json!({"status": false, "data": l, "error": err_str, "meta": global_map, "line": num})
                        );
                    }
                };
            }
            Ok(_) => {
                println!(
                    "{}",
                    serde_json::json!({"status": false, "data": l, "error": "failed to decode", "line": num})
                );
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

fn grok_cmd(app: &mut TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    if let Some(matches) = cmd.subcommand_matches("run") {
        grok_run_cmd(&matches)
    } else {
        usage(&app)
    }
}

fn grok_run_cmd(cmd: &ArgMatches<'_>) -> Result<()> {
    let test_pattern = cmd
        .value_of("TEST_PATTERN")
        .ok_or("TEST_PATTERN not provided")?;
    let patterns_file = cmd.value_of("patterns").ok_or("patterns not provided")?;

    let input: Box<dyn BufRead> = match cmd.value_of("DATA") {
        None => Box::new(BufReader::new(io::stdin())),
        Some(data) => Box::new(BufReader::new(File::open(data)?)),
    };

    let grok = if cmd.is_present("patterns") {
        grok::Pattern::from_file(patterns_file, test_pattern)?
    } else {
        grok::Pattern::new(test_pattern.to_string())?
    };
    for (num, line) in input.lines().enumerate() {
        let l = line?;
        if l.is_empty() || l.starts_with('#') {
            continue;
        }
        let result = grok.matches(l.as_bytes());

        match result {
            Ok(j) => {
                println!(
                    "{}",
                    serde_json::json!({"status": true, "data": l, "meta": &test_pattern, "line": num, "grokked": j})
                );
            }
            Err(reason) => {
                let err_str = reason.to_string();
                println!(
                    "{}",
                    serde_json::json!({"status": false, "data": l, "error": err_str, "meta": "PATTERN", "line": num})
                );
            }
        }
    }
    Ok(())
}

fn pipe_cmd(app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    if let Some(matches) = cmd.subcommand_matches("run") {
        pipe_run_cmd(app, &matches)
    } else if let Some(matches) = cmd.subcommand_matches("dot") {
        pipe_to_dot_cmd(app, &matches)
    } else {
        usage(app)
    }
}

fn pipe_run_cmd(_app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    let script = cmd.value_of("CONFIG").ok_or("CONFIG not provided")?;
    let config = slurp(script)?;
    let runtime = tremor_runtime::incarnate(config)?;
    let pipeline = &runtime.pipes[0];
    let mut flow = pipeline.to_executable_graph(tremor_pipeline::buildin_ops)?;

    let input: Box<dyn BufRead> = match cmd.value_of("DATA") {
        None => Box::new(BufReader::new(io::stdin())),
        Some(data) => Box::new(BufReader::new(File::open(data)?)),
    };

    for (num, line) in input.lines().enumerate() {
        let l = line?;
        let mut codec = tremor_runtime::codec::lookup("json")?;
        let data = codec
            .decode(l.as_bytes().to_vec(), 0)?
            .ok_or_else(|| Error::from("Failed to decode input JSON"))?;

        let mut eventset = Vec::new();
        flow.enqueue(
            "in1",
            tremor_pipeline::Event {
                id: num as u64,
                ingest_ns: utils::nanotime(),
                data,
                ..tremor_pipeline::Event::default()
            },
            &mut eventset,
        )?;

        for (stream, event) in eventset {
            println!("{} {}: {:?}", num, stream, event);
        }
    }
    Ok(())
}

fn pipe_to_dot_cmd(_app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    let script = cmd.value_of("CONFIG").ok_or("CONFIG not provided")?;
    let config = slurp(script)?;
    let runtime = tremor_runtime::incarnate(config)?;
    let pipeline = &runtime.pipes[0];
    println!("{}", pipeline.to_dot());
    Ok(())
}

async fn conductor_cmd(app: &mut TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    if let Some(matches) = cmd.subcommand_matches("version") {
        conductor_version_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("binding") {
        conductor_binding_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("pipeline") {
        conductor_pipeline_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("onramp") {
        conductor_onramp_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("offramp") {
        conductor_offramp_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("target") {
        conductor_target_cmd(app, &matches).await
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

async fn conductor_target_cmd(app: &mut TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    if let Some(matches) = cmd.subcommand_matches("list") {
        conductor_target_list_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("create") {
        conductor_target_create_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("delete") {
        conductor_target_delete_cmd(app, &matches).await
    } else {
        println!("{}", serde_json::to_string(&app.config.instances)?);
        Ok(())
    }
}

/////////////////////////////
// API endpoint targetting //
/////////////////////////////

async fn conductor_target_list_cmd(app: &TremorApp<'_>, _cmd: &ArgMatches<'_>) -> Result<()> {
    println!(
        "{:?}",
        app.config
            .instances
            .keys()
            .cloned()
            .collect::<Vec<String>>()
    );
    Ok(())
}

async fn conductor_target_create_cmd(app: &mut TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    let id = cmd.value_of("TARGET_ID").ok_or("TARGET_ID not provided")?;
    let path_to_file = cmd.value_of("SOURCE").ok_or("SOURCE not provided")?;
    let json = load(path_to_file)?;
    let endpoints: Vec<String> = serde_json::from_value(json)?;
    app.config.instances.insert(id.to_string(), endpoints);
    save_config(&app.config)
}

async fn conductor_target_delete_cmd(app: &mut TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    let id = cmd.value_of("TARGET_ID").ok_or("TARGET_ID not provided")?;
    app.config.instances.remove(&id.to_string());
    save_config(&app.config)
}

//////////////////
// API Version  //
//////////////////

async fn conductor_version_cmd(app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let endpoint = &format!("{}version", base_url);
    let response = reqwest::get(endpoint).await?;
    let version: Version = response.json().await?;
    println!(
        "{}",
        match cmd.value_of("format") {
            Some("yaml") => serde_yaml::to_string(&version)?,
            Some("json") | _ => serde_json::to_string(&version)?,
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

async fn conductor_pipeline_cmd(app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    if let Some(matches) = cmd.subcommand_matches("list") {
        conductor_pipeline_list_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("fetch") {
        conductor_pipeline_get_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("delete") {
        conductor_pipeline_delete_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("create") {
        conductor_pipeline_create_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("instance") {
        conductor_pipeline_instance_cmd(app, &matches).await
    } else {
        usage(app)
    }
}

async fn conductor_pipeline_get_cmd(app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let endpoint = format!("pipeline/{id}", id = id);
    let response = restc.get(&endpoint)?.send().await?;
    handle_response(response).await
}

async fn conductor_pipeline_delete_cmd(app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let endpoint = format!("pipeline/{id}", id = id);
    let response = restc.delete(&endpoint)?.send().await?;
    handle_response(response).await
}

async fn conductor_pipeline_list_cmd(app: &TremorApp<'_>, _cmd: &ArgMatches<'_>) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let endpoint = "pipeline".to_string();
    let response = restc.get(&endpoint)?.send().await?;
    handle_response(response).await
}

async fn conductor_pipeline_create_cmd(app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let path_to_file = cmd.value_of("SOURCE").ok_or("SOURCE not provided")?;
    let json = load(path_to_file)?;
    let ser = ser(&app, &json)?;
    let endpoint = "pipeline";
    let response = restc
        .post(endpoint)?
        .header("content-type", content_type(app))
        .header("accept", accept(app))
        .body(ser)
        .send()
        .await?;
    handle_response(response).await
}

async fn conductor_pipeline_instance_cmd(app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let a_id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let s_id = cmd
        .value_of("INSTANCE_ID")
        .ok_or("INSTANCE_ID not provided")?;
    let endpoint = format!("pipeline/{id}/{instance}", id = a_id, instance = s_id);
    let response = restc.get(&endpoint)?.send().await?;
    handle_response(response).await
}

/////////////////////////////
// API Binding subcommands //
/////////////////////////////

async fn conductor_binding_cmd(app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    if let Some(matches) = cmd.subcommand_matches("list") {
        conductor_binding_list_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("fetch") {
        conductor_binding_get_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("delete") {
        conductor_binding_delete_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("create") {
        conductor_binding_create_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("instance") {
        conductor_binding_instance_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("activate") {
        conductor_binding_activate_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("deactivate") {
        conductor_binding_deactivate_cmd(app, &matches).await
    } else {
        usage(app)
    }
}

async fn conductor_binding_get_cmd(app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let endpoint = format!("binding/{id}", id = id);
    let response = restc.get(&endpoint)?.send().await?;
    handle_response(response).await
}

async fn conductor_binding_delete_cmd(app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let endpoint = format!("binding/{id}", id = id);
    let response = restc.delete(&endpoint)?.send().await?;
    handle_response(response).await
}

async fn conductor_binding_list_cmd(app: &TremorApp<'_>, _cmd: &ArgMatches<'_>) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let endpoint = "binding".to_string();
    let response = restc.get(&endpoint)?.send().await?;
    handle_response(response).await
}

async fn conductor_binding_create_cmd(app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let path_to_file = cmd.value_of("SOURCE").ok_or("SOURCE not provided")?;
    let json = load(path_to_file)?;
    let ser = ser(&app, &json)?;
    let endpoint = "binding";
    let response = restc
        .post(&endpoint)?
        .header("content-type", content_type(app))
        .header("accept", accept(app))
        .body(ser)
        .send()
        .await?;
    handle_response(response).await
}

async fn conductor_binding_instance_cmd(app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let a_id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let s_id = cmd
        .value_of("INSTANCE_ID")
        .ok_or("INSTANCE_ID not provided")?;
    let endpoint = format!("binding/{id}/{instance}", id = a_id, instance = s_id);
    let response = restc.get(&endpoint)?.send().await?;
    handle_response(response).await
}

async fn conductor_binding_activate_cmd(app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let a_id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let s_id = cmd
        .value_of("INSTANCE_ID")
        .ok_or("INSTANCE_ID not provided")?;
    let endpoint = format!("binding/{id}/{instance}", id = a_id, instance = s_id);
    let path_to_file = cmd.value_of("SOURCE").ok_or("SOURCE not provided")?;
    let json = load(path_to_file)?;
    let ser = ser(&app, &json)?;
    let response = restc
        .post(&endpoint)?
        .header("content-type", content_type(app))
        .header("accept", accept(app))
        .body(ser)
        .send()
        .await?;
    handle_response(response).await
}

async fn conductor_binding_deactivate_cmd(app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let a_id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let s_id = cmd
        .value_of("INSTANCE_ID")
        .ok_or("INSTANCE_ID not provided")?;
    let endpoint = format!("binding/{id}/{instance}", id = a_id, instance = s_id);
    let response = restc.delete(&endpoint)?.send().await?;
    handle_response(response).await
}

//////////////////////////
// API Offramp commands //
//////////////////////////

async fn conductor_offramp_cmd(app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    if let Some(matches) = cmd.subcommand_matches("list") {
        conductor_offramp_list_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("fetch") {
        conductor_offramp_get_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("delete") {
        conductor_offramp_delete_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("create") {
        conductor_offramp_create_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("instance") {
        conductor_offramp_instance_cmd(app, &matches).await
    } else {
        usage(app)
    }
}

async fn conductor_offramp_get_cmd(app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let endpoint = format!("offramp/{id}", id = id);
    let response = restc.get(&endpoint)?.send().await?;
    handle_response(response).await
}

async fn conductor_offramp_delete_cmd(app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let endpoint = format!("offramp/{id}", id = id);
    let response = restc.delete(&endpoint)?.send().await?;
    handle_response(response).await
}

async fn conductor_offramp_list_cmd(app: &TremorApp<'_>, _cmd: &ArgMatches<'_>) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let endpoint = "offramp".to_string();
    let response = restc.get(&endpoint)?.send().await?;
    handle_response(response).await
}

async fn conductor_offramp_create_cmd(app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let path_to_file = cmd.value_of("SOURCE").ok_or("SOURCE not provided")?;
    let json = load(path_to_file)?;
    let ser = ser(&app, &json)?;
    let endpoint = "offramp";
    let response = restc
        .post(&endpoint)?
        .header("content-type", content_type(app))
        .header("accept", accept(app))
        .body(ser)
        .send()
        .await?;
    handle_response(response).await
}

async fn conductor_offramp_instance_cmd(app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let a_id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let s_id = cmd
        .value_of("INSTANCE_ID")
        .ok_or("INSTANCE_ID not provided")?;
    let endpoint = format!("offramp/{id}/{instance}", id = a_id, instance = s_id);
    let response = restc.get(&endpoint)?.send().await?;
    handle_response(response).await
}

/////////////////////////
// API Onramp commands //
/////////////////////////

async fn conductor_onramp_cmd(app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    if let Some(matches) = cmd.subcommand_matches("list") {
        conductor_onramp_list_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("fetch") {
        conductor_onramp_get_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("delete") {
        conductor_onramp_delete_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("create") {
        conductor_onramp_create_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("instance") {
        conductor_onramp_instance_cmd(app, &matches).await
    } else {
        usage(app)
    }
}

async fn conductor_onramp_get_cmd(app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let endpoint = format!("onramp/{id}", id = id);
    let response = restc.get(&endpoint)?.send().await?;
    handle_response(response).await
}

async fn conductor_onramp_delete_cmd(app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let endpoint = format!("onramp/{id}", id = id);
    let response = restc.delete(&endpoint)?.send().await?;
    handle_response(response).await
}

async fn conductor_onramp_list_cmd(app: &TremorApp<'_>, _cmd: &ArgMatches<'_>) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let endpoint = "onramp".to_string();
    let response = restc.get(&endpoint)?.send().await?;
    handle_response(response).await
}

async fn conductor_onramp_create_cmd(app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let path_to_file = cmd.value_of("SOURCE").ok_or("SOURCE not provided")?;
    let json = load(path_to_file)?;
    let ser = ser(&app, &json)?;
    let endpoint = "onramp";
    let response = restc
        .post(endpoint)?
        .header("content-type", content_type(app))
        .header("accept", accept(app))
        .body(ser)
        .send()
        .await?;
    handle_response(response).await
}

async fn conductor_onramp_instance_cmd(app: &TremorApp<'_>, cmd: &ArgMatches<'_>) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let restc = rest::HttpC::new(base_url.to_string());
    let a_id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let s_id = cmd
        .value_of("INSTANCE_ID")
        .ok_or("INSTANCE_ID not provided")?;
    let endpoint = format!("onramp/{id}/{instance}", id = a_id, instance = s_id);
    let response = restc.get(&endpoint)?.send().await?;
    handle_response(response).await
}

//////////////////
// Utility code //
//////////////////

fn load(path_to_file: &str) -> Result<serde_json::Value> {
    let mut source = File::open(path_to_file)?;
    let ext = Path::new(path_to_file)
        .extension()
        .and_then(OsStr::to_str)
        .ok_or("Could not create fail path")?;
    let mut raw = vec![];
    source.read_to_end(&mut raw)?;

    if ext == "yaml" || ext == "yml" {
        Ok(serde_yaml::from_slice(raw.as_slice())?)
    } else if ext == "json" {
        Ok(serde_json::from_slice(raw.as_slice())?)
    } else {
        Err(Error::from(format!("Unsupported format: {}", ext)))
    }
}

fn content_type(app: &TremorApp<'_>) -> &'static str {
    match app.format {
        FormatKind::Json => "application/json",
        FormatKind::Yaml => "application/yaml",
    }
}

fn accept(app: &TremorApp<'_>) -> &'static str {
    match app.format {
        FormatKind::Json => "application/json",
        FormatKind::Yaml => "application/yaml",
    }
}

async fn handle_response(response: surf::Response) -> Result<()> {
    let status = response.status();
    match status {
        StatusCode::OK | StatusCode::CREATED => println!("{}", response.text().await?),
        StatusCode::NOT_FOUND => eprintln!("Not found"),
        StatusCode::CONFLICT => eprintln!("Conflict"),
        _ => eprintln!("Unexpected response ( status: {} )", status.as_u16()),
    };
    Ok(())
}

fn ser(app: &TremorApp<'_>, json: &serde_json::Value) -> Result<String> {
    Ok(match app.format {
        FormatKind::Json => serde_json::to_string(&json)?,
        FormatKind::Yaml => serde_yaml::to_string(&json)?,
    })
}
