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
use regex::Regex;
use serial_test::serial;
use std::io::prelude::*;
use std::path::Path;
use tremor_common::{file, uids::OperatorUIdGen};
use tremor_pipeline::query::Query;
use tremor_pipeline::ExecutableGraph;
use tremor_runtime::errors::*;
use tremor_script::highlighter::Dumb;
use tremor_script::module::Manager;
use tremor_script::FN_REGISTRY;

fn to_executable_graph(query: &str) -> Result<ExecutableGraph> {
    let aggr_reg = tremor_script::aggr_registry();
    let mut idgen = OperatorUIdGen::new();
    let q = Query::parse(&query, &*FN_REGISTRY.read()?, &aggr_reg)?;
    Ok(q.to_executable_graph(&mut idgen)?)
}
macro_rules! test_cases {

    ($($file:ident),* ,) => {
        $(
            #[test]
            #[serial(query_error)]
            fn $file() -> Result<()> {
                tremor_runtime::functions::load()?;
                let query_dir = concat!("tests/query_errors/", stringify!($file), "/").to_string();
                let query_file = concat!("tests/query_errors/", stringify!($file), "/query.trickle");
                let err_file = concat!("tests/query_errors/", stringify!($file), "/error.txt");
                let err_re_file = concat!("tests/query_errors/", stringify!($file), "/error.re");


                println!("Loading query: {}", query_file);
                let mut file = file::open(query_file)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;

                Manager::clear_path()?;
                Manager::add_path(&query_dir)?;
                Manager::add_path(&"tremor-script/lib")?;
                let s = to_executable_graph( &contents);
                if Path::new(err_re_file).exists() {
                    println!("Loading error: {}", err_re_file);
                    let mut file = file::open(err_re_file)?;
                    let mut err = String::new();
                    file.read_to_string(&mut err)?;
                    let err = err.trim();
                    let re = Regex::new(err)?;

                    if let Err(got) = s {
                        println!("{}", got);
                        assert!(re.is_match(&format!("{}", got)));
                    } else {
                        println!("Expected error, but got succeess");
                        assert!(false);
                    }
                } else {
                    println!("Loading error: {}", err_file);
                    let mut file = file::open(err_file)?;
                    let mut err = String::new();
                    file.read_to_string(&mut err)?;

                    match s {
                        Err(Error(ErrorKind::Pipeline(tremor_pipeline::errors::ErrorKind::Script(e)), o)) => {
                            let e = tremor_script::errors::Error(e, o);
                            let got = Dumb::error_to_string(&e)?;
                            assert_eq!(err.trim(), got.trim(), "unexpected error message:\n{}", got);
                        }
                        Err(Error(ErrorKind::Script(e), o)) =>{
                            let e = tremor_script::errors::Error(e, o);
                            let got = Dumb::error_to_string(&e)?;
                            assert_eq!(err.trim(), got.trim(), "unexpected error message:\n{}", got);
                        }
                        Err(Error(ErrorKind::Pipeline(e), _)) =>{
                            let got = format!("{}", e);
                            assert_eq!(err.trim(), got.trim(), "unexpected error message:\n{}", got);
                        }
                        Err(e) => {
                            println!("got wrong error: {:?}", e);
                            assert!(false);
                        }
                        _ =>{
                            println!("Expected error, but got succeess");
                            assert!(false);
                        }
                    };
                };
                Ok(())
            }
        )*
    };
}

test_cases!(
    const_in_select,
    let_in_select,
    local_in_having,
    local_in_select,
    local_in_where,
    local_in_group_by,
    pp_mod_not_found,
    pp_unrecognized_token,
    pp_unrecognized_token2,
    pp_unrecognized_token3,
    pp_unrecognized_token4,
    pp_unrecognized_token5,
    pp_embed_unrecognized_token,
    pp_embed_unrecognized_token2,
    pp_embed_unrecognized_token3,
    pp_embed_unrecognized_token4,
    pp_embed_unrecognized_token5,
    // INSERT
    window_mut_meta,
    window_mut_event,
    window_bad_script_from,
    scope_window_script5,
    scope_window_script4,
    scope_window_script3,
    scope_window_script2,
    scope_window_script,
    scope_named_scripts3,
    scope_named_scripts2,
    scope_named_scripts,
    state_not_static,
    script_in_port,
    script_duplicate_port,
    pipeline_stream_name_conflict,
    pipeline_out,
    pipeline_in,
    double_query,
    pipeline_name_overlap,
    non_const_in_args,
    args_in_const,
    pipeline_unknown_port,
    pipeline_duplicate_streams_inside,
    pipeline_select_bad_stream,
    pipeline_stream_port_conflict,
    pipeline_duplicate_define,
    pipeline_undefined,
    pipeline_unknown_param,
    window_meta_in_tick,
    window_event_in_tick,
    window_mutate_meta,
    window_mutate_event,
    duplicate_stream_name,
    window_both_settings,
    window_group_by_event_in_target,
    window_event_in_target,
    aggr_arity,
    aggr_in_aggr,
    bad_into,
    bad_from,
    node_duplicate_name_operator,
    node_duplicate_name_script,
    node_reserved_name_operator,
    node_reserved_name_script,
);
