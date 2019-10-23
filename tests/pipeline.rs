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
use pretty_assertions::assert_eq;
use serde_yaml;
use std::fs::File;
use std::io::prelude::*;
use tremor_pipeline;
use tremor_pipeline::Event;
use tremor_runtime;
use tremor_runtime::errors::*;
use tremor_script::utils::*;

macro_rules! test_cases {
    ($($file:ident),*) => {
        $(
            #[test]
            fn $file() -> Result<()> {

                tremor_runtime::functions::load()?;
                let pipeline_file = concat!("tests/pipelines/", stringify!($file), "/pipeline.yaml");
                let in_file = concat!("tests/pipelines/", stringify!($file), "/in.xz");
                let out_file = concat!("tests/pipelines/", stringify!($file), "/out.xz");

                let mut file = File::open(pipeline_file)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;
                let config: tremor_pipeline::config::Pipeline = serde_yaml::from_str(&contents)?;
                let pipeline = tremor_pipeline::build_pipeline(config)?;
                let mut pipeline = pipeline.to_executable_graph(tremor_pipeline::buildin_ops)?;

                let in_json = load_event_file(in_file)?;
                let mut out_json = load_event_file(out_file)?;

                out_json.reverse();

                let mut results = Vec::new();
                for (id, json) in in_json.into_iter().enumerate() {
                    let event = Event {
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
                assert_eq!(results.len(), out_json.len());
                for (_, result) in results {
                    for value in result.value_iter() {
                        if let Some(expected) = out_json.pop() {
                            assert_eq!(sorsorted_serialize(value)?, sorsorted_serialize(&expected)?);
                        }
                    }
                }
                assert!(out_json.is_empty());
                Ok(())
            }
        )*
    };
}

test_cases!(
    default_rule,
    dimensions,
    drop,
    example_rule,
    layered_limiting,
    lru,
    merge,
    multi_dimensions,
    passthrough,
    patch,
    rewrite_root,
    tremor_map
);
