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
use std::io::prelude::*;
use std::path::Path;
use tremor_common::{file, ids::OperatorIdGen};
use tremor_pipeline;
use tremor_pipeline::query::Query;
use tremor_pipeline::ExecutableGraph;
use tremor_pipeline::FN_REGISTRY;
use tremor_runtime;
use tremor_runtime::errors::*;
use tremor_script::highlighter::{Dumb, Highlighter};
use tremor_script::path::ModulePath;

fn to_pipe(module_path: &ModulePath, file_name: &str, query: &str) -> Result<ExecutableGraph> {
    let aggr_reg = tremor_script::aggr_registry();
    let cus = vec![];
    let mut idgen = OperatorIdGen::new();
    let q = Query::parse(
        module_path,
        query,
        file_name,
        cus,
        &*FN_REGISTRY.lock()?,
        &aggr_reg,
    )?;
    Ok(q.to_pipe(&mut idgen)?)
}

macro_rules! test_cases {

    ($($file:ident),* ,) => {
        $(
            #[cfg(not(tarpaulin_include))]
            #[test]
            fn $file() -> Result<()> {

                tremor_runtime::functions::load()?;
                let query_dir = concat!("tests/query_errors/", stringify!($file), "/").to_string();
                let query_file = concat!("tests/query_errors/", stringify!($file), "/query.trickle");
                let err_file = concat!("tests/query_errors/", stringify!($file), "/error.txt");
                let err_re_file = concat!("tests/query_errors/", stringify!($file), "/error.re");
                let module_path = &ModulePath { mounts: vec![query_dir, "tremor-script/lib/".to_string()] };

                println!("Loading query: {}", query_file);
                let mut file = file::open(query_file)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;

                if Path::new(err_re_file).exists() {
                    println!("Loading error: {}", err_re_file);
                    let mut file = file::open(err_re_file)?;
                    let mut err = String::new();
                    file.read_to_string(&mut err)?;
                    let err = err.trim();
                    let re = Regex::new(err)?;

                    let s = to_pipe(&module_path, err_re_file, &contents);
                    if let Err(e) = s {
                        println!("{} ~ {}", err, format!("{}", e));
                        assert!(re.is_match(&format!("{}", e)));
                    } else {
                        println!("Expected error, but got succeess");
                        assert!(false);
                    }
                } else {
                    println!("Loading error: {}", err_file);
                    let mut file = file::open(err_file)?;
                    let mut err = String::new();
                    file.read_to_string(&mut err)?;
                    let err = err.trim();

                    match to_pipe(&module_path, err_file, &contents) {
                        Err(Error(ErrorKind::Pipeline(tremor_pipeline::errors::ErrorKind::Script(e)), o)) => {
                            let e = tremor_script::errors::Error(e, o);
                            let mut h = Dumb::new();
                            tremor_script::query::Query::format_error_from_script(&contents, &mut h, &e)?;
                            h.finalize()?;
                            let got = h.to_string();
                            let got = got.trim();
                            println!("{}", got);
                            assert_eq!(err, got);
                        }
                        Err(Error(ErrorKind::Script(e), o)) =>{
                            let e = tremor_script::errors::Error(e, o);
                            let mut h = Dumb::new();
                            tremor_script::query::Query::format_error_from_script(&contents, &mut h, &e)?;
                            h.finalize()?;
                            let got = h.to_string();
                            let got = got.trim();
                            println!("{}", got);
                            assert_eq!(err, got);
                        }
                        Err(Error(ErrorKind::Pipeline(e), _)) =>{
                            let got = format!("{}", e);
                            assert_eq!(err, got, "unexpected error message: {}", got);
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
