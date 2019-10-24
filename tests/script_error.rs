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
use std::fs::File;
use std::io::prelude::*;
use tremor_pipeline::FN_REGISTRY;
use tremor_runtime;
use tremor_runtime::errors::*;
use tremor_script::Script;

macro_rules! test_cases {
    ($($file:ident),* ,) => {
        $(
            #[test]
            fn $file() -> Result<()> {

                tremor_runtime::functions::load()?;
                let script_file = concat!("tests/script_errors/", stringify!($file), "/script.tremor");
                let err_file = concat!("tests/script_errors/", stringify!($file), "/error.txt");

                let mut file = File::open(script_file)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;

                let mut file = File::open(err_file)?;
                let mut err = String::new();
                file.read_to_string(&mut err)?;
                let err = err.trim();
                let s = Script::parse(&contents, &*FN_REGISTRY.lock()?);
                if let Err(e) = dbg!(s) {
                    assert_eq!(err, format!("{}", e));
                } else {
                    println!("Expected error, but got succeess");
                    assert!(false);
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
);
