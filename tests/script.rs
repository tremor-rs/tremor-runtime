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
use simd_json::prelude::*;
use simd_json::value::borrowed::{Object, Value};
use std::fs::File;
use std::io::prelude::*;
use tremor_pipeline::FN_REGISTRY;
use tremor_runtime;
use tremor_runtime::errors::*;
use tremor_script::utils::*;
use tremor_script::{AggrType, EventContext, Return, Script};

macro_rules! test_cases {
    ($($file:ident),* ,) => {
        $(
            #[test]
            fn $file() -> Result<()> {

                tremor_runtime::functions::load()?;
                let script_file = concat!("tests/scripts/", stringify!($file), "/script.tremor");
                let in_file = concat!("tests/scripts/", stringify!($file), "/in.xz");
                let out_file = concat!("tests/scripts/", stringify!($file), "/out.xz");

                println!("Loading script: {}", script_file);
                let mut file = File::open(script_file)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;
                let script = Script::parse(&contents, &*FN_REGISTRY.lock()?)?;

                println!("Loading input: {}", in_file);
                let in_json = load_event_file(in_file)?;
                println!("Loading expected: {}", out_file);
                let mut out_json = load_event_file(out_file)?;

                out_json.reverse();

                let mut results = Vec::new();
                for (id, mut json) in in_json.into_iter().enumerate() {

                    let context = EventContext::new(id as u64, None);
                    let mut meta = Value::from(Object::default());
                    let mut state = Value::null();
                    match script.run(&context, AggrType::Tick, &mut json, &mut state, &mut meta)? {
                        Return::Drop => (),
                        Return::EmitEvent{..} => results.push(json),
                        Return::Emit{value, ..} => results.push(value),
                    };
                }
                assert_eq!(results.len(), out_json.len());
                for value in results {
                    if let Some(expected) = out_json.pop() {
                        assert_eq!(sorsorted_serialize(&value)?, sorsorted_serialize(&expected)?);
                    }
                }
                Ok(())
            }
        )*
    };
}

test_cases!(
    array_comprehension,
    array_paths,
    array_pattern,
    assign_move,
    assing_and_path_match,
    base64,
    binary_float,
    binary_int,
    binary_string,
    binary,
    bit_ops,
    bit_shift,
    cidr_multi,
    cidr,
    const_fn,
    consts,
    datetime,
    dummy,
    emit_port,
    empty_subslice,
    empty_record_pattern,
    eq,
    escape,
    extractor_dissect,
    glob,
    grok,
    heredoc,
    influx,
    json,
    jump,
    kv,
    let_field,
    logical,
    merge,
    multi_case,
    multiline,
    nested_patterns,
    null_match,
    null,
    only_const,
    passthrough,
    patch,
    presence,
    record_comprehension,
    record,
    recordpattern_eq,
    regex,
    simple_match,
    state_null,
    string_concat,
    string_interpolation,
    subslice,
    subslice_repeated,
    unary,
    // regression
    empty_array_pattern,
);
