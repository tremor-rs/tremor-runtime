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
use tremor_script::FN_REGISTRY;

use serial_test::serial;
use tremor_runtime::errors::*;
use tremor_script::highlighter::{Dumb, Highlighter};
use tremor_script::{module::Manager, Script};

macro_rules! test_cases {
    ($($file:ident),* ,) => {
        $(
            #[test]
            #[serial(script_warning)]
            fn $file() -> Result<()> {

                tremor_runtime::functions::load()?;
                let script_dir = concat!("tests/script_warnings/", stringify!($file), "/").to_string();
                let script_file = concat!("tests/script_warnings/", stringify!($file), "/script.tremor");
                let err_file = concat!("tests/script_warnings/", stringify!($file), "/warning.txt");
                Manager::clear_path()?;
                Manager::add_path(&"tremor-script/lib")?;
                Manager::add_path(&script_dir)?;

                println!("Loading script: {}", script_file);
                let mut file = file::open(script_file)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;

                println!("Loading warning: {}", err_file);
                let mut file = file::open(err_file)?;
                let mut err = String::new();
                file.read_to_string(&mut err)?;
                let err = err.trim();
                let s = Script::parse(&contents, &*FN_REGISTRY.read()?)?;
                let mut h = Dumb::new();
                s.format_warnings_with(&mut h)?;
                h.finalize()?;
                let got = h.to_string();
                let got = got.trim();
                println!("{}", got);
                assert_eq!(err, got);
                Ok(())
            }
        )*
    };
}

test_cases!(
    match_no_default,
    match_multiple_default,
    match_imut_no_default,
    match_imut_multiple_default,
    // INSERT
    record_perf,
    array_perf,
    default_case__,
    nanotime,
    lower_case_const,
    recordpattern_absence_and_extractor,
    recordpattern_presence_and_extractor,
);
