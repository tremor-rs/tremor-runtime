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

use crate::errors::{Error, Result};
use clap::{self, ArgMatches};
use halfbrown::HashMap;
use http_types::{headers, StatusCode};
use simd_json::prelude::*;

use crate::util::*;

pub(crate) async fn run_cmd(app: &mut TremorApp, cmd: &ArgMatches) -> Result<()> {
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
        Err("Invalid command".into())
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

async fn conductor_target_cmd(app: &mut TremorApp, cmd: &ArgMatches) -> Result<()> {
    if cmd.subcommand_matches("list").is_some() {
        conductor_target_list_cmd(app).await
    } else if let Some(matches) = cmd.subcommand_matches("create") {
        conductor_target_create_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("delete") {
        conductor_target_delete_cmd(app, &matches).await
    } else {
        println!("{}", simd_json::to_string(&app.config.instances)?);
        Ok(())
    }
}

/////////////////////////////
// API endpoint targetting //
/////////////////////////////

async fn conductor_target_list_cmd(app: &TremorApp) -> Result<()> {
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

async fn conductor_target_create_cmd(app: &mut TremorApp, cmd: &ArgMatches) -> Result<()> {
    let id = cmd.value_of("TARGET_ID").ok_or("TARGET_ID not provided")?;
    let path_to_file = cmd.value_of("SOURCE").ok_or("SOURCE not provided")?;
    let json = load(path_to_file)?;
    let endpoints: Vec<String> = json
        .as_array()
        .ok_or_else(|| Error::from("Invalid Configuration"))?
        .iter()
        .filter_map(|v| (ValueTrait::as_str(v).map(String::from)))
        .collect();
    app.config.instances.insert(id.to_string(), endpoints);
    save_config(&app.config)
}

async fn conductor_target_delete_cmd(app: &mut TremorApp, cmd: &ArgMatches) -> Result<()> {
    let id = cmd.value_of("TARGET_ID").ok_or("TARGET_ID not provided")?;
    app.config.instances.remove(&id.to_string());
    save_config(&app.config)
}

//////////////////
// API Version  //
//////////////////

async fn conductor_version_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let endpoint = &format!("{}version", base_url);
    let mut response = surf::get(endpoint).await?;
    let version: Version = response.body_json().await?;
    println!(
        "{}",
        match cmd.value_of("format") {
            Some("yaml") => serde_yaml::to_string(&version)?,
            _ => simd_json::to_string(&version)?,
        }
    );
    Ok(())
}

//////////////////////////////
// API Pipeline subcommands //
//////////////////////////////

async fn conductor_pipeline_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    if cmd.subcommand_matches("list").is_some() {
        conductor_list_cmd(app, "pipeline").await
    } else if let Some(matches) = cmd.subcommand_matches("fetch") {
        conductor_get_cmd(app, &matches, "pipeline").await
    } else if let Some(matches) = cmd.subcommand_matches("delete") {
        conductor_delete_cmd(app, &matches, "pipeline").await
    } else if let Some(matches) = cmd.subcommand_matches("create") {
        conductor_create_cmd(app, &matches, "pipeline").await
    } else if let Some(matches) = cmd.subcommand_matches("instance") {
        conductor_instance_cmd(app, &matches, "pipeline").await
    } else {
        Err("Invalid command".into())
    }
}

/////////////////////////////
// API Binding subcommands //
/////////////////////////////

async fn conductor_binding_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    if cmd.subcommand_matches("list").is_some() {
        conductor_list_cmd(app, "binding").await
    } else if let Some(matches) = cmd.subcommand_matches("fetch") {
        conductor_get_cmd(app, &matches, "binding").await
    } else if let Some(matches) = cmd.subcommand_matches("delete") {
        conductor_delete_cmd(app, &matches, "binding").await
    } else if let Some(matches) = cmd.subcommand_matches("create") {
        conductor_create_cmd(app, &matches, "binding").await
    } else if let Some(matches) = cmd.subcommand_matches("instance") {
        conductor_instance_cmd(app, &matches, "binding").await
    } else if let Some(matches) = cmd.subcommand_matches("activate") {
        conductor_binding_activate_cmd(app, &matches).await
    } else if let Some(matches) = cmd.subcommand_matches("deactivate") {
        conductor_binding_deactivate_cmd(app, &matches).await
    } else {
        Err("Invalid command".into())
    }
}

async fn conductor_binding_activate_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let a_id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let s_id = cmd
        .value_of("INSTANCE_ID")
        .ok_or("INSTANCE_ID not provided")?;
    let endpoint = format!(
        "{url}binding/{id}/{instance}",
        url = base_url,
        id = a_id,
        instance = s_id
    );
    let path_to_file = cmd.value_of("SOURCE").ok_or("SOURCE not provided")?;
    let json = load(path_to_file)?;
    let ser = ser(&app, &json)?;
    let response = surf::post(&endpoint)
        .set_header(headers::CONTENT_TYPE, content_type(app))
        .set_header(headers::ACCEPT, accept(app))
        .body_string(ser)
        .await?;
    handle_response(response).await
}

async fn conductor_binding_deactivate_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let a_id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let s_id = cmd
        .value_of("INSTANCE_ID")
        .ok_or("INSTANCE_ID not provided")?;
    let endpoint = format!(
        "{url}binding/{id}/{instance}",
        url = base_url,
        id = a_id,
        instance = s_id
    );
    let response = surf::delete(&endpoint).await?;
    handle_response(response).await
}

//////////////////////////
// API Offramp commands //
//////////////////////////

async fn conductor_offramp_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    if cmd.subcommand_matches("list").is_some() {
        conductor_list_cmd(app, "offramp").await
    } else if let Some(matches) = cmd.subcommand_matches("fetch") {
        conductor_get_cmd(app, &matches, "offramp").await
    } else if let Some(matches) = cmd.subcommand_matches("delete") {
        conductor_delete_cmd(app, &matches, "offramp").await
    } else if let Some(matches) = cmd.subcommand_matches("create") {
        conductor_create_cmd(app, &matches, "offramp").await
    } else if let Some(matches) = cmd.subcommand_matches("instance") {
        conductor_instance_cmd(app, &matches, "offramp").await
    } else {
        Err("Invalid command".into())
    }
}

/////////////////////////
// API Onramp commands //
/////////////////////////

async fn conductor_onramp_cmd(app: &TremorApp, cmd: &ArgMatches) -> Result<()> {
    if cmd.subcommand_matches("list").is_some() {
        conductor_list_cmd(app, "onramp").await
    } else if let Some(matches) = cmd.subcommand_matches("fetch") {
        conductor_get_cmd(app, &matches, "onramp").await
    } else if let Some(matches) = cmd.subcommand_matches("delete") {
        conductor_delete_cmd(app, &matches, "onramp").await
    } else if let Some(matches) = cmd.subcommand_matches("create") {
        conductor_create_cmd(app, &matches, "onramp").await
    } else if let Some(matches) = cmd.subcommand_matches("instance") {
        conductor_instance_cmd(app, &matches, "onramp").await
    } else {
        Err("Invalid command".into())
    }
}

/////////////////
// Shared code //
/////////////////

async fn conductor_get_cmd(app: &TremorApp, cmd: &ArgMatches, endpoint: &str) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let endpoint = format!(
        "{url}/{endpoint}/{id}",
        url = base_url,
        endpoint = endpoint,
        id = id
    );
    let response = surf::get(&endpoint).await?;
    handle_response(response).await
}

async fn conductor_list_cmd(app: &TremorApp, endpoint: &str) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let endpoint = format!("{}{}", base_url, endpoint);
    let response = surf::get(&endpoint).await?;
    handle_response(response).await
}

async fn conductor_create_cmd(app: &TremorApp, cmd: &ArgMatches, endpoint: &str) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let path_to_file = cmd.value_of("SOURCE").ok_or("SOURCE not provided")?;
    let json = load(path_to_file)?;
    let ser = ser(&app, &json)?;
    let endpoint = format!("{}{}", base_url, endpoint);
    let response = surf::post(&endpoint)
        .set_header(http_types::headers::CONTENT_TYPE, content_type(app))
        .set_header("accept", accept(app))
        .body_string(ser)
        .await;
    handle_response(response?).await
}

async fn conductor_delete_cmd(app: &TremorApp, cmd: &ArgMatches, endpoint: &str) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let endpoint = format!(
        "{url}/{endpoint}/{id}",
        url = base_url,
        endpoint = endpoint,
        id = id
    );
    let response = surf::delete(&endpoint).await?;
    handle_response(response).await
}

async fn conductor_instance_cmd(app: &TremorApp, cmd: &ArgMatches, endpoint: &str) -> Result<()> {
    let base_url = &app.config.instances[&"default".to_string()][0];
    let a_id = cmd
        .value_of("ARTEFACT_ID")
        .ok_or("ARTEFACT_ID not provided")?;
    let s_id = cmd
        .value_of("INSTANCE_ID")
        .ok_or("INSTANCE_ID not provided")?;
    let endpoint = format!(
        "{url}/{endpoint}/{id}/{instance}",
        url = base_url,
        endpoint = endpoint,
        id = a_id,
        instance = s_id
    );
    let response = surf::get(&endpoint).await?;
    handle_response(response).await
}

async fn handle_response(mut response: surf::Response) -> Result<()> {
    let status = response.status();
    match status {
        StatusCode::Ok | StatusCode::Created => println!("{}", response.body_string().await?),
        StatusCode::NotFound => eprintln!("Not found"),
        StatusCode::Conflict => eprintln!("Conflict"),
        _ => eprintln!(
            "Unexpected response ( status: {} )",
            status.canonical_reason()
        ),
    };
    Ok(())
}
