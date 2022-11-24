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
use tremor_script::{highlighter::Dumb, module::Manager, Script, FN_REGISTRY};

macro_rules! test_cases {
    ($($file:ident),* ,) => {
        $(
            #[test]
            #[serial(script_error, timeout_ms = 120000)]
            fn $file() -> Result<()> {
                tremor_runtime::functions::load()?;
                let script_dir = concat!("tests/script_errors/", stringify!($file), "/").to_string();
                let script_file = concat!("tests/script_errors/", stringify!($file), "/script.tremor");
                let err_file = concat!("tests/script_errors/", stringify!($file), "/error.txt");

                println!("Loading script: {}", script_file);
                let mut file = file::open(script_file)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;

                println!("Loading error: {}", err_file);
                let mut file = file::open(err_file)?;
                let mut err = String::new();
                file.read_to_string(&mut err)?;
                let err = err.trim();

                Manager::clear_path()?;
                Manager::add_path(&script_dir)?;
                Manager::add_path(&"tremor-script/lib")?;
                let s = Script::parse(&contents, &*FN_REGISTRY.read()?);
                if let Err(e) = s {
                    let got = Dumb::error_to_string(&e)?;
                    println!("{}", got);
                    assert_eq!(err.trim(), got.trim());
                } else {
                    println!("Expected error, but got succeess :/");
                    assert!(false);
                }
                Ok(())
            }
        )*
    };
}

macro_rules! ignored_cases {
    ($($file:ident),* ,) => {
        $(
            #[test]
            #[serial(script_error, timeout_ms = 120000)]
            fn $file() -> Result<()> {
                tremor_runtime::functions::load()?;
                let script_dir = concat!("tests/script_errors/", stringify!($file), "/").to_string();
                let script_file = concat!("tests/script_errors/", stringify!($file), "/script.tremor");
                let err_file = concat!("tests/script_errors/", stringify!($file), "/error.txt");

                println!("Loading script: {}", script_file);
                let mut file = file::open(script_file)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;

                println!("Loading error: {}", err_file);
                let mut file = file::open(err_file)?;
                let mut err = String::new();
                file.read_to_string(&mut err)?;
                let _err = err.trim();

                Manager::clear_path()?;
                Manager::add_path(&script_dir)?;
                Manager::add_path(&"tremor-script/lib")?;
                let s = Script::parse(&contents, &*FN_REGISTRY.read()?);
                if let Err(e) = s {
                    let got = Dumb::error_to_string(&e)?;
                    println!("{}", got);
                    assert_eq!(err.trim(), got.trim());
                } else {
                    println!("Expected error, but got succeess :(");
                    assert!(false);
                }
                Ok(())
            }
        )*
    };
}

test_cases!(
    arith_bad_shift_1,
    arith_bad_shift_2,
    double_const,
    function_error_1,
    function_error_2,
    function_error_3,
    function_error_n,
    imut_bad_drop,
    imut_bad_emit,
    invalid_const_binary,
    invalid_const_unary,
    invalid_extractor,
    lexer_diamond,
    lexer_ngt,
    lexer_triple_colon,
    lexer_unterminated_extractor,
    lexer_unterminated_extractor2,
    lexer_unterminated_ident2,
    lexer_unterminated_string2,
    unknown_extractor,
    pp_unrecognized_token,
    pp_unrecognized_token2,
    pp_unrecognized_token3,
    pp_unrecognized_token4,
    pp_unrecognized_token5,
    pp_mod_not_found,
    unknown_function_in_function,
    pp_cyclic,
    pp_nest_cyclic,
    // INSERT
    sub_overflow,
    mul_overflow,
    add_overflow,
    tailing_gt,
    lexer_invalid_float_exp,
    invalid_escape_extractor,
    tailing_heredoc,
    invalid_utf8_3,
    lexer_invalid_pp,
    const_expr_id_into_arr,
    const_expr_range_on_non_array,
    const_expr_index_into_int,
    const_expr_index_out_of_bounds,
    const_expr_range_out_of_bounds,
    const_expr_range_bad_end,
    const_expr_range_bad_start,
    const_expr_reverse_range,
    const_expr_unknown_key,
    patch_non_str_key,
    bin_invalid_bits,
    bin_invalid_type,
    merge_ident,
    select_ident,
    function_already_defined,
    missing_function,
    string_interpolation_empty,
    fn_bad_recur,
    script_without_newline,
    lexer_invalid_hex2,
    lexer_invalid_int_invalid_char,
    lexer_invalid_hex,
    lexer_invalid_float,
    lexer_invalid_int,
    string_interpolation_eof,
    string_interpolation_invalid_utf8,
    string_interpolation_escape,
    string_interpolation_extractor,
    heredoc_tailing,
    heredoc_interpolation_multiline,
    heredoc_interpolation_invalid_unicode,
    heredoc_interpolation_escape,
    heredoc_interpolation_ident,
    heredoc_interpolation_extractor,
    unexpected_escape,
    unfinished_escape_eof,
    single_quote,
    lexer_string_interpolation3,
    lexer_string_interpolation2,
    lexer_string_interpolation,
    lexer_heredoc_interpolation,
    lexer_heredoc_interpolation2,
    lexer_heredoc_interpolation3,
    invalid_utf8_1,
    invalid_utf8_2,
    error_in_include,
);

ignored_cases!(
    pp_unknown_function_in_function,
    lexer_bad_float,
    lexer_bad_hex,
    lexer_bad_hex2,
    lexer_bad_hex3,
    lexer_unterminated_heredoc,
    lexer_unterminated_heredoc2,
    lexer_unterminated_ident,
    lexer_unterminated_string,
    lexer_bad_int,
);
