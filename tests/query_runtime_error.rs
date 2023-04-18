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
use pretty_assertions::assert_eq;
use std::io::prelude::*;
use std::path::PathBuf;
use tremor_common::{file, ports::IN, uids::OperatorUIdGen};

use tremor_pipeline::query::Query;
use tremor_pipeline::ExecutableGraph;
use tremor_pipeline::{Event, EventId};
use tremor_script::FN_REGISTRY;

use serial_test::serial;
use tremor_pipeline::errors::{Error as PipelineError, ErrorKind as PipelineErrorKind};
use tremor_runtime::errors::*;
use tremor_script::highlighter::Dumb;
use tremor_script::module::Manager;
use tremor_script::utils::*;

fn to_pipe(query: &str) -> Result<ExecutableGraph> {
    let aggr_reg = tremor_script::aggr_registry();
    let mut idgen = OperatorUIdGen::new();
    let q = Query::parse(&query, &*FN_REGISTRY.read()?, &aggr_reg)?;
    Ok(q.to_executable_graph(&mut idgen)?)
}

const TEST_DIR: &str = "tests/query_runtime_errors";

macro_rules! test_cases {

    ($($file:ident),* ,) => {
        $(
            #[tokio::test(flavor = "multi_thread")]
            #[serial(query_runtime_error)]
            async fn $file() -> Result<()> {

                tremor_runtime::functions::load()?;
                let query_dir = [TEST_DIR,  stringify!($file)].iter().collect::<PathBuf>().to_string_lossy().to_string();
                let query_file: PathBuf = [&query_dir,  "query.trickle"].iter().collect::<PathBuf>();
                let err_file: PathBuf = [&query_dir, "error.txt"].iter().collect();
                let in_file: PathBuf = [&query_dir, "in"].iter().collect();
                let out_file: PathBuf = [&query_dir, "out"].iter().collect();

                Manager::clear_path()?;
                Manager::add_path(&"tremor-script/lib")?;

                println!("Loading query: {}", query_file.display());
                let mut file = file::open(&query_file)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;
                let mut pipeline = to_pipe(&contents)?;

                println!("Loading input: {}", in_file.display());
                let in_json = load_event_file(in_file.to_str().unwrap())?;

                println!("Loading expected: {}", out_file.display());
                let mut out_json = load_event_file(out_file.to_str().unwrap())?;
                out_json.reverse();

                println!("Loading error: {}", err_file.display());
                let mut err = None;
                if let Ok(mut file) = file::open(&err_file) {

                    let mut content = String::new();
                    file.read_to_string(&mut content)?;
                    err = Some(content.trim().to_string());
                }

                let mut results = Vec::new();
                for (id, json) in in_json.into_iter().enumerate() {
                    let event = Event {
                        id: EventId::from_id(0, 0, (id as u64)),
                        data: json.clone_static().into(),
                        ingest_ns: id as u64,
                        ..Event::default()
                    };
                    let mut r = vec![];
                    // run the pipeline, if an error occurs, dont stop but check for equivalence with `error.txt`
                    match pipeline.enqueue(0, IN, event, &mut r) {
                        Err(PipelineError(PipelineErrorKind::Script(e), o)) => {
                            if let Some(err) = err.as_ref() {
                                let e = tremor_script::errors::Error(e, o);
                                let got = Dumb::error_to_string(&e)?;
                                print!("{}", got);
                                assert_eq!(err.trim(), got.trim());
                            } else {
                                println!("Got unexpected error: {:?}", e);
                                assert!(false);
                            }
                        }
                        Err(e) => {
                            println!("got wrong error: {:?}", e);
                            assert!(false);
                        }
                        Ok(()) => {}
                    }
                    results.append(&mut r);
                }
                assert_eq!(results.len(), out_json.len(), "Number of events differ error");
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
        )*
    };
}

test_cases!(
    branch_error_then_ok,
    // INSERT
    script_bad_port,
    meta_and_use,
    binary_operators_or_left,
    binary_operators_or_right,
    binary_operators_and_left,
    binary_operators_and_right,
    binary_operators_xor_left,
    binary_operators_xor_right,
    bad_vec_state_window_return,
    bad_state_window_return,
);
