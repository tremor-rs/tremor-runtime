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
use tremor_pipeline::{EventOriginUri, FN_REGISTRY};
use tremor_runtime;
use tremor_runtime::errors::*;
use tremor_script::errors::CompilerError;
use tremor_script::path::ModulePath;
use tremor_script::prelude::*;
use tremor_script::utils::*;
use tremor_script::{AggrType, EventContext, Return, Script};

macro_rules! test_cases {
    ($($file:ident),* ,) => {
        $(
            #[test]
            fn $file() -> Result<()> {

                tremor_runtime::functions::load()?;
                let script_dir = concat!("tests/scripts/", stringify!($file), "/").to_string();
                let script_file = concat!("tests/scripts/", stringify!($file), "/script.tremor");
                let in_file = concat!("tests/scripts/", stringify!($file), "/in.xz");
                let out_file = concat!("tests/scripts/", stringify!($file), "/out.xz");

                println!("Loading script: {}", script_file);
                let mut file = file::open(script_file)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;
                let contents2 = contents.clone();

                let script = Script::parse(&ModulePath { mounts: vec![script_dir, "tremor-script/lib".to_string()] }, script_file, contents2, &*FN_REGISTRY.lock()?).map_err(CompilerError::error)?;

                println!("Loading input: {}", in_file);
                let in_json = load_event_file(in_file)?;
                println!("Loading expected: {}", out_file);
                let mut out_json = load_event_file(out_file)?;

                out_json.reverse();

                let mut results = Vec::new();
                for (id, mut json) in in_json.into_iter().enumerate() {
                    let uri = EventOriginUri{
                        host: "test".into(),
                        path: vec!["snot".into()],
                        port: Some(23),
                        scheme: "snot".into(),
                        uid: 42

                    };
                    let context = EventContext::new(id as u64, Some(uri));
                    let mut meta = Value::from(Object::default());
                    let mut state = Value::null();
                    match script.run(&context, AggrType::Tick, &mut json, &mut state, &mut meta)? {
                        Return::Drop => (),
                        Return::EmitEvent{..} => results.push(json),
                        Return::Emit{value, ..} => results.push(value),
                    };
                }
                assert_eq!(results.len(), out_json.len());
                for (i, value) in results.iter().enumerate() {
                    if let Some(expected) = out_json.pop() {
                        assert_eq!(sorted_serialize(&value)?, sorted_serialize(&expected)?, "Input event #{} Expected `{}`, but got `{}`", i, sorted_serialize(&expected)?, sorted_serialize(&value)?);
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
    assign_and_path_match,
    assign_move,
    base64,
    binary_float,
    binary_int,
    binary_string,
    binary_uint,
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
    empty_record_pattern,
    empty_subslice,
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
    record_comprehension_imut,
    record_comprehension,
    record,
    recordpattern_eq,
    regex,
    simple_match,
    state_null,
    string_concat,
    string_interpolation,
    subslice_repeated,
    subslice,
    unary,
    // preprocessor
    pp_nest0,
    pp_nest1,
    pp_inline_nest1,
    pp_nest2,
    pp_nest3,
    pp_alias0,
    pp_alias1,
    pp_alias2,
    pp_alias3,
    // regression
    empty_array_pattern,
    // TODO
    // const_in_const_lookup,
    // INSERT
role_map,
    string_interpolation_nested,
    string_interpolation_escaped_hash,
    string_interpolation_escaped,
    string_interpolation_simple,
    string_interpolation_tailing,
    string_interpolation_regexp,
    binary_binary,
    binary_binary_string,
    binary_string_binary,
    bytes_tcp,
    bytes_create,
    string_interpolation_quotes,
    heredoc_interpolation_quotes,
    range,
    origin,
    array_pattern_element,
    array_pattern_ignore,
    array_pattern_short_circuit,
    string_quoted_curly,
    heredoc_quoted_curly,
    string_interpolation_import,
    string_interpolation_prefix,
    patch_in_place,
    tuple_pattern,
    pattern_cmp,
    pass_args,
    escape_in_extractor,
    const_of_const,
    fn_extractors,
    mod_access_const,
    module,
    fn_fib,
    fn_nest2_fib,
    fn_nest2_abs_fib,
    pp_fn_fib,
    heredoc_interpolation,
    heredoc_usefn_interpolation,
    heredoc_regression,
);
