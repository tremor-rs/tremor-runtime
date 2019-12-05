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
use std::fs::File;
use std::io::prelude::*;
use tremor_pipeline::FN_REGISTRY;
use tremor_runtime;
use tremor_runtime::errors::*;
use tremor_script::highlighter::{Dumb, Highlighter};
use tremor_script::Script;

macro_rules! test_cases {
    ($($file:ident),* ,) => {
        $(
            #[test]
            fn $file() -> Result<()> {

                tremor_runtime::functions::load()?;
                let script_file = concat!("tests/script_errors/", stringify!($file), "/script.tremor");
                let err_file = concat!("tests/script_errors/", stringify!($file), "/error.txt");

                println!("Loading script: {}", script_file);
                let mut file = File::open(script_file)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;

                let mut file = File::open(err_file)?;
                let mut err = String::new();
                file.read_to_string(&mut err)?;
                let err = err.trim();
                let s = Script::parse(&contents, &*FN_REGISTRY.lock()?);
                if let Err(e) = s {
                    let mut h = Dumb::new();
                    Script::format_error_from_script(&contents, &mut h, &e)?;
                    h.finalize()?;
                    let got = h.to_string();
                    let got = got.trim();
                    println!("{}", got);
                    assert_eq!(err, got);
                } else {
                    println!("Expected error, but got succeess");
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
            fn $file() -> Result<()> {

                tremor_runtime::functions::load()?;
                let script_file = concat!("tests/script_errors/", stringify!($file), "/script.tremor");
                let err_file = concat!("tests/script_errors/", stringify!($file), "/error.txt");

                println!("Loading script: {}", script_file);
                let mut file = File::open(script_file)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;

                let mut file = File::open(err_file)?;
                let mut err = String::new();
                file.read_to_string(&mut err)?;
                let _err = err.trim();
                let s = Script::parse(&contents, &*FN_REGISTRY.lock()?);
                if let Err(e) = s {
                    let mut h = Dumb::new();
                    Script::format_error_from_script(&contents, &mut h, &e)?;
                    h.finalize()?;
                    let got = h.to_string();
                    let got = got.trim();
                    println!("{}", got);
                    //assert_eq!(err, got);
                } else {
                    println!("Expected error, but got succeess");
                    //assert!(false);
                }
                Ok(())
            }
        )*
    };
}

test_cases!(
    double_const,
    function_error_1,
    function_error_2,
    function_error_3,
    function_error_n,
    invalid_const_binary,
    invalid_const_unary,
    invalid_extractor,
    lexer_diamond,
    lexer_double_minus,
    lexer_ngt,
    lexer_triple_colon,
    lexer_unterminated_extractor,
    lexer_unterminated_extractor2,
    lexer_unterminated_ident2,
    lexer_unterminated_string2,
    unknown_extractor,
);

ignored_cases!(
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
