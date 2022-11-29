// Copyright 2020-2021, The Tremor Team
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

use async_std::prelude::FutureExt;
use log::LevelFilter;
use log4rs::{
    config::{Appender, Root},
    Config,
};
use pretty_assertions::assert_eq;
use serial_test::serial;
use std::fs;
use std::io::Read;
use std::time::Duration;
use tremor_common::{file, ids::OperatorIdGen};
use tremor_pipeline::query::Query;
use tremor_pipeline::ExecutableGraph;
use tremor_pipeline::{Event, EventId};
use tremor_pipeline::{PluggableLoggingAppender, LOGGING_CHANNEL};
use tremor_runtime::system::ShutdownMode;
use tremor_runtime::{
    errors::*,
    system::{World, WorldConfig},
};
use tremor_script::module::Manager;
use tremor_script::utils::*;
use tremor_script::FN_REGISTRY;

fn cd(path: String) {
    std::env::set_current_dir(path).unwrap();
}
fn get_cwd() -> String {
    std::env::current_dir()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string()
}
fn delete(path: String) -> Result<()> {
    fs::remove_file(path)?;
    Ok(())
}

/// Exact copy from the `parse(&str) -> Result<Deploy>` function in "flows.rs"
fn parse(deploy: &str) -> tremor_script::Result<tremor_script::deploy::Deploy> {
    let aggr_reg = tremor_script::aggr_registry();
    let reg = tremor_script::registry::registry();
    tremor_script::deploy::Deploy::parse(deploy, &reg, &aggr_reg)
}

async fn deploy_test_config(contents: String, file: &str) -> Result<()> {
    let out_file = "out".to_string();
    let expected_file = "expected".to_string();

    tremor_runtime::functions::load()?;
    match parse(&contents) {
        Ok(deployable) => {
            cd(format!("tests/queries/{file}").to_string());
            let config = WorldConfig {
                debug_connectors: true,
                ..WorldConfig::default()
            };

            let (world, handle) = World::start(config).await?;
            for flow in deployable.iter_flows() {
                world.start_flow(flow).await?;
            }
            handle.timeout(Duration::from_secs(10)).await??; // let the time to finish previous async flows
            match world.stop(ShutdownMode::Graceful).await {
                Ok(anything) => println!("Shutting down world gave: \"{anything:?}\""),
                Err(error) => println!("Error shutting down world gracefully: {error}"),
            }
        }
        otherwise => {
            println!(
                "Expected valid deployment file, compile phase, but got an unexpected error: {:?}",
                otherwise
            );
            otherwise?;
        }
    }

    println!("Loading output: \"{out_file}\"");
    let out_json = load_event_file(&out_file)?;

    println!("Loading expected: \"{expected_file}\"");
    let mut expected_json = load_event_file(&expected_file)?;
    expected_json.reverse();

    // read expected file, compare to out file
    assert_eq!(
        expected_json.len(),
        out_json.len(),
        "Number of events differ error"
    );
    for out_value in out_json {
        let serialized = sorted_serialize(&out_value)?;
        if let Some(expected) = expected_json.pop() {
            assert_eq!(serialized, sorted_serialize(&expected)?);
        }
    }

    // delete output file created generated for by this test
    delete(out_file)?;

    Ok(())
}

fn trickle_to_pipe(query: String) -> Result<ExecutableGraph> {
    let aggr_reg = tremor_script::aggr_registry();
    let mut idgen = OperatorIdGen::new();
    let q = Query::parse(&query, &*FN_REGISTRY.read()?, &aggr_reg)?;
    Ok(q.to_executable_graph(&mut idgen)?)
}

async fn query_test_config(contents: String, file: &str) -> Result<()> {
    let in_file = format!("tests/queries/{file}/in");
    let out_file = format!("tests/queries/{file}/out");

    let mut pipeline = trickle_to_pipe(contents)?;

    println!("Loading input: \"{in_file}\"");
    let in_json = load_event_file(&in_file)?;

    let mut results = Vec::new();
    for (id, json) in in_json.into_iter().enumerate() {
        let event = Event {
            id: EventId::new(0, 0, id as u64, id as u64),
            data: json.clone_static().into(),
            ingest_ns: id as u64,
            ..Event::default()
        };
        let mut r = vec![];
        pipeline.enqueue("in".into(), event, &mut r).await?;
        results.append(&mut r);
    }

    println!("Loading expected: \"{out_file}\"");
    let mut out_json = load_event_file(&out_file)?;
    out_json.reverse();

    assert_eq!(
        out_json.len(),
        results.len(),
        "Number of events differ error"
    );
    for (_, result) in results {
        for value in result.value_iter() {
            let serialized = sorted_serialize(value)?;
            if let Some(expected) = out_json.pop() {
                assert_eq!(serialized, sorted_serialize(&expected)?);
            }
        }
    }
    Ok(())
}

fn or_exists_then_get<'a>(
    this: (&str, &str),
    that: (&str, &str),
) -> Result<(String, String, fs::File)> {
    let type_name: &str;
    let file_name: &str;
    let file_obj: fs::File;

    match file::open(this.1) {
        Ok(value) => {
            type_name = this.0;
            file_name = this.1;
            file_obj = value;
        }
        Err(_err) => {
            type_name = that.0;
            file_name = that.1;
            file_obj = file::open(that.1).expect("None of the files exists");
        }
    }
    Ok((type_name.to_string(), file_name.to_string(), file_obj))
}

async fn main_config(file: &str) -> Result<()> {
    tremor_runtime::functions::load()?;
    let query_dir = &format!("tests/queries/{file}/");
    let query_file = &format!("tests/queries/{file}/query.trickle");
    let deploy_file = &format!("tests/queries/{file}/query.troy");
    let type_name_query = "query";
    let type_name_deploy = "deploy";

    Manager::clear_path()?;
    Manager::add_path(&"tremor-script/lib")?;
    Manager::add_path(query_dir)?;

    let (type_name, file_name, mut file_obj) = or_exists_then_get(
        (type_name_deploy, deploy_file), // first tested
        (type_name_query, query_file),   // else
    )?;
    println!("Loading {type_name} file: \"{file_name}\"");
    let mut contents = String::new();
    file_obj.read_to_string(&mut contents)?;

    let result: Result<()>;

    let original_path = get_cwd();
    if type_name == type_name_deploy {
        result = deploy_test_config(contents, file).await;
    } else {
        result = query_test_config(contents, file).await;
    }
    cd(original_path);
    result
}

async fn run() {
    let tx = LOGGING_CHANNEL.tx();

    let stdout = PluggableLoggingAppender { tx };

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(Root::builder().appender("stdout").build(LevelFilter::Debug))
        .unwrap();

    log4rs::init_config(config).unwrap();
}

macro_rules! test_cases {
    ($($file:ident),*) => {
        $(
            #[async_std::test]
            #[serial(query, timeout_ms = 120000)]
            async fn $file() -> Result<()> {
                main_config(stringify!($file)).await
            }
        )*
    };
}

macro_rules! test_cases_with_server {
    ($($file:ident),*) => {
        $(
            #[async_std::test]
            #[serial(query, timeout_ms = 120000)]
            async fn $file() -> Result<()> {
                run().await;
                main_config(stringify!($file)).await
            }
        )*
    };
}

test_cases!(
    binary_op_short_circuit,
    array_addition,
    array_addition_optimisations,
    default_rule,
    dimensions,
    example_rule,
    group_by_size,
    group_by_time,
    group_country_region_az,
    group_each,
    group_set,
    having_filter,
    layered_limiting,
    lru,
    merge,
    multi_dimensions,
    mutate,
    simple_passthrough,
    patch,
    rewrite_root,
    script_params_overwrite,
    script_params,
    state,
    state_counter_operator,
    streams,
    tremor_map,
    where_filter,
    window_by_two_scripted,
    window_by_two,
    window_size_tilted,
    pp_win,
    pp_script,
    pp_operator,
    pp_alias_win,
    pp_alias_script,
    pp_alias_operator,
    pp_config_directive,
    // INSERT
    script_ports,
    initial_state,
    unused_node,
    route_emit,
    drop_event,
    pipeline_group_by_size,
    pipeline_complex_args,
    pipeline_nested_script,
    pipeline_nested_operator,
    args_nesting_no_leakage,
    args_nesting_redefine,
    pipeline_nested_pipeline,
    pipeline_passthrough,
    alias_script_params_overwrite,
    cardinality,
    window_mixed_2,
    window_mixed_1,
    pp_const,
    pp_fn,
    script_error,
    guard_where,
    guard_having,
    history,
    roundrobin,
    pluggable_logging_dev
);

test_cases_with_server!(pluggable_logging_operator);
