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
use simd_json::value::borrowed::{Object, Value};
use std::fs::File;
use std::io::prelude::*;
use tremor_pipeline::FN_REGISTRY;
use tremor_runtime;
use tremor_runtime::errors::*;
use tremor_script::utils::*;
use tremor_script::{AggrType, EventContext, Script};

macro_rules! test_cases {
    ($($file:ident),* ,) => {
        $(
            #[test]
            fn $file() -> Result<()> {

                tremor_runtime::functions::load()?;
                let script_file = concat!("tests/script_runtime_errors/", stringify!($file), "/script.tremor");
                let in_file = concat!("tests/script_runtime_errors/", stringify!($file), "/in.xz");
                let err_file = concat!("tests/script_runtime_errors/", stringify!($file), "/error.txt");

                println!("Loading script: {}", script_file);
                let mut file = File::open(script_file)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;
                let script = Script::parse(&contents, &*FN_REGISTRY.lock()?)?;

                println!("Loading input: {}", in_file);
                let mut in_json = load_event_file(in_file)?;

                println!("Loading error: {}", err_file);
                let mut file = File::open(err_file)?;
                let mut err = String::new();
                file.read_to_string(&mut err)?;
                let err = err.trim();

                if let Some(mut json) =  in_json.pop() {

                    let context = EventContext::from_ingest_ns(0);
                    let  mut meta = Value::from(Object::default());
                    let s = script.run(&context, AggrType::Tick, &mut json, &mut meta);
                    if let Err(e) = s {
                        assert_eq!(err, format!("{}", e));
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
    bad_binary,
    bad_index_access,
    bad_unary,
    function_error_1,
    function_error_2,
    function_error_3,
    function_error_n,
    merge_new_no_object,
    merge_target_no_object,
    missing_local,
    no_clause_hit,
    non_arr_access,
    non_arr_access2,
    non_obj_access,
    non_obj_access2,
    obj_bad_key,
    subslice_and_idx_out_of_bounds,
    subslice_bad_end,
    subslice_bad_start,
    subslice_no_arr,
    subslice_out_of_bounds,
    undefined_local,
);
