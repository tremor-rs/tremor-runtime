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
use pretty_assertions::assert_eq;
use std::fs::File;
use std::io::prelude::*;
use tremor_pipeline;
use tremor_pipeline::query::Query;
use tremor_pipeline::Event;
use tremor_pipeline::ExecutableGraph;
use tremor_pipeline::FN_REGISTRY;
use tremor_runtime;
use tremor_runtime::errors::*;
use tremor_script::path::ModulePath;
use tremor_script::utils::*;

fn to_pipe(module_path: &ModulePath, file_name: String, query: &str) -> Result<ExecutableGraph> {
    let aggr_reg = tremor_script::aggr_registry();
    let q = Query::parse(
        &module_path,
        query,
        file_name,
        &*FN_REGISTRY.lock()?,
        &aggr_reg,
    )?;
    Ok(q.to_pipe()?)
}

macro_rules! test_cases {

    ($($file:ident),* ,) => {
        $(
            #[test]
            fn $file() -> Result<()> {

                tremor_runtime::functions::load()?;
                let query_dir = concat!("tests/queries/", stringify!($file), "/").to_string();
                let query_file = concat!("tests/queries/", stringify!($file), "/query.trickle");
                let in_file = concat!("tests/queries/", stringify!($file), "/in.xz");
                let out_file = concat!("tests/queries/", stringify!($file), "/out.xz");
                let module_path = ModulePath { mounts: vec![query_dir] };

                println!("Loading query: {}", query_file);
                let mut file = File::open(query_file)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;
                let mut pipeline = to_pipe(&module_path, query_file.to_string(), &contents)?;

                println!("Loading input: {}", in_file);
                let in_json = load_event_file(in_file)?;
                println!("Loading expected: {}", out_file);
                let mut out_json = load_event_file(out_file)?;

                out_json.reverse();

                let mut results = Vec::new();
                for (id, json) in in_json.into_iter().enumerate() {
                    let event = Event {
                        origin_uri: None,
                        id: id as u64,
                        data: json.clone_static().into(),
                        ingest_ns: id as u64,
                        kind: None,
                        is_batch: false,
                    };
                    let mut r = Vec::new();
                    pipeline.enqueue("in", event, &mut r)?;
                    results.append(&mut r);
                }
                assert_eq!(results.len(), out_json.len(), "Number of events differ error");
                for (_, result) in results {
                    for value in result.value_iter() {
                        if let Some(expected) = out_json.pop() {
                            assert_eq!(sorsorted_serialize(value)?, sorsorted_serialize(&expected)?);
                        }
                    }
                }
                Ok(())
            }
        )*
    };
}

test_cases!(
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
    passthrough,
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
);
