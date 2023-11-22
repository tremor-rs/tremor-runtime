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
use serial_test::serial;
use std::io::prelude::*;
use tremor_common::file;
use tremor_runtime::errors::*;
use tremor_script::{
    highlighter::Dumb, module::Manager, prelude::*, utils::*, AggrType, Script, FN_REGISTRY,
    NO_CONTEXT,
};
use tremor_value::{Object, Value};

macro_rules! test_cases {
    ($($file:ident),* ,) => {
        $(
            #[test]
            #[serial(script_runtime_error)]
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

                Manager::clear_path()?;
                Manager::add_path(&script_dir)?;
                Manager::add_path(&"tremor-script/lib")?;
                let script = Script::parse(&contents, &*FN_REGISTRY.read().map_err(|_| tremor_runtime::errors::ErrorKind::ReadLock)?)?;

                println!("Loading input: {}", in_file);
                let mut in_json = load_event_file(in_file)?;

                println!("Loading error: {}", err_file);
                let mut file = file::open(err_file)?;
                let mut err = String::new();
                file.read_to_string(&mut err)?;
                let err = err.trim();

                if let Some(mut json) =  in_json.pop() {
                    let context = NO_CONTEXT;
                    let mut meta = Value::from(Object::default());
                    let mut state = Value::null();
                    let s = script.run(&context, AggrType::Tick, &mut json, &mut state, &mut meta);
                    if let Err(e) = s {
                        let got = Dumb::error_to_string(&e)?;
                        print!("{}", got);
                        assert_eq!(err.trim(), got.trim());
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
            #[serial(script_runtime_error)]
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

                Manager::clear_path()?;
                Manager::add_path(&script_dir)?;
                Manager::add_path(&"tremor-script/lib")?;
                let script = Script::parse(&contents, &*FN_REGISTRY.read().map_err(|_| tremor_runtime::errors::ErrorKind::ReadLock)?)?;

                println!("Loading input: {}", in_file);
                let mut in_json = load_event_file(in_file)?;

                println!("Loading error: {}", err_file);
                let mut file = file::open(err_file)?;
                let mut err = String::new();
                file.read_to_string(&mut err)?;
                let _err = err.trim();

                let mut state = Value::null();
                if let Some(mut json) =  in_json.pop() {
                    let context = NO_CONTEXT;
                    let mut meta = Value::object();
                    let s = script.run(&context, AggrType::Tick, &mut json, &mut state, &mut meta);
                    if let Err(e) = s {
                        let got = Dumb::error_to_string(&e)?;
                        print!("{}", got);
                        assert_eq!(err.trim(), got.trim());
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
    merge_assign_target_new_no_object,
    merge_assign_target_target_no_object,
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
    fold_bool_or_imut,
    fold_bool_or,
    fold_bool_op_imut,
    fold_bool_op,
    bad_fold_op_record,
    bad_fold_op,
    bad_fold_op_record_imut,
    bad_fold_op_imut,
    bad_fold_type,
    bad_fold_type_imut,
    bad_merge2,
    bad_merge,
    meta_and_use,
    assign_expr,
    assign_reserved,
    assign_const,
    error_after_heredoc,
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
