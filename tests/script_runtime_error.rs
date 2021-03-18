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
use tremor_common::file;
use tremor_pipeline::FN_REGISTRY;
use tremor_runtime;
use tremor_runtime::errors::*;
use tremor_script::errors::CompilerError;
use tremor_script::highlighter::{Dumb, Highlighter};
use tremor_script::path::ModulePath;
use tremor_script::prelude::*;
use tremor_script::utils::*;
use tremor_script::{AggrType, EventContext, Script};
use tremor_script::{Object, Value};

macro_rules! test_cases {
    ($($file:ident),* ,) => {
        $(
            #[test]
            fn $file() -> Result<()> {

                tremor_runtime::functions::load()?;
                let script_dir = concat!("tests/script_runtime_errors/", stringify!($file), "/").to_string();
                let script_file = concat!("tests/script_runtime_errors/", stringify!($file), "/script.tremor");
                let in_file = concat!("tests/script_runtime_errors/", stringify!($file), "/in.xz");
                let err_file = concat!("tests/script_runtime_errors/", stringify!($file), "/error.txt");

                println!("Loading script: {}", script_file);
                let mut file = file::open(script_file)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;
                let contents2 = contents.clone();

                let script = Script::parse(&ModulePath { mounts: vec![script_dir, "tremor-script/lib".into()] }, script_file, contents2, &*FN_REGISTRY.lock()?).map_err(CompilerError::error)?;

                println!("Loading input: {}", in_file);
                let mut in_json = load_event_file(in_file)?;

                println!("Loading error: {}", err_file);
                let mut file = file::open(err_file)?;
                let mut err = String::new();
                file.read_to_string(&mut err)?;
                let err = err.trim();

                if let Some(mut json) =  in_json.pop() {
                    let context = EventContext::new(0, None);
                    let mut meta = Value::from(Object::default());
                    let mut state = Value::null();
                    let s = script.run(&context, AggrType::Tick, &mut json, &mut state, &mut meta);
                    if let Err(e) = s {
                        let got = script.format_error(&e);
                        let got = got.trim();
                        println!("{}", got);
                        assert_eq!(err, got);
                    } else {
                        println!("Expected error, but got succeess");
                        assert!(false);
                    }
                }
                assert!(in_json.is_empty());
                Ok(())
            }
        )*
    };
}

macro_rules! ignore_cases {
    ($($file:ident),* ,) => {
        $(
            #[test]
            fn $file() -> Result<()> {

                tremor_runtime::functions::load()?;
                let script_dir = concat!("tests/script_runtime_errors/", stringify!($file), "/").to_string();
                let script_file = concat!("tests/script_runtime_errors/", stringify!($file), "/script.tremor");
                let in_file = concat!("tests/script_runtime_errors/", stringify!($file), "/in.xz");
                let err_file = concat!("tests/script_runtime_errors/", stringify!($file), "/error.txt");

                println!("Loading script: {}", script_file);
                let mut file = file::open(script_file)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;
                let contents2 = contents.clone();

                let script = Script::parse(&ModulePath { mounts: vec![script_dir, "tremor-script/lib".to_string()] }, script_file, contents2, &*FN_REGISTRY.lock()?).map_err(CompilerError::error)?;

                println!("Loading input: {}", in_file);
                let mut in_json = load_event_file(in_file)?;

                println!("Loading error: {}", err_file);
                let mut file = file::open(err_file)?;
                let mut err = String::new();
                file.read_to_string(&mut err)?;
                let _err = err.trim();

                if let Some(mut json) =  in_json.pop() {
                    let context = EventContext::new(0, None);
                    let mut meta = Value::object();
                    let mut state = Value::null();
                    let s = script.run(&context, AggrType::Tick, &mut json, &mut state, &mut meta);
                    if let Err(e) = s {
                        let mut h = Dumb::new();
                        script.format_error_with(&mut h, &e)?;
                        h.finalize()?;
                        let got = h.to_string();
                        let got = got.trim();
                        println!("{}", got);
                        //assert_eq!(err, got);
                    } else {
                        println!("Expected error, but got succeess");
                        assert!(false);
                    }
                }
                assert!(in_json.is_empty());
                Ok(())
            }
        )*
    };
}

test_cases!(
    arith_bad_shift_3,
    arr_bad_idx,
    arr_bad_idx2,
    bad_binary,
    bad_bitshift,
    bad_index_type,
    bad_unary,
    function_error_1,
    function_error_2,
    function_error_3,
    function_error_n,
    match_bad_guard_type,
    match_no_clause_hit,
    merge_in_place_new_no_object,
    merge_in_place_target_no_object,
    merge_new_no_object,
    merge_target_no_object,
    missing_local,
    non_arr_access,
    non_arr_access2,
    non_obj_access,
    non_obj_access2,
    obj_bad_key,
    obj_bad_key2,
    patch_on_non_obj,
    subslice_and_idx_out_of_bounds,
    subslice_bad_end,
    subslice_bad_start,
    subslice_end_lt_start,
    subslice_neg_start,
    subslice_no_arr,
    subslice_out_of_bounds,
    // INSERT
    undefined_local,
    recursion_limit,
);

// There errors on thise are not optimal
// we want to impove them
ignore_cases!(
    patch_merge_on_non_object,
    patch_update_key_missing,
    patch_key_exists,
    patch_move_key_exists,
    patch_copy_key_exists,
);
