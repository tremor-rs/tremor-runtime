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
                let script_file = concat!("tests/script_wanings/", stringify!($file), "/script.tremor");
                let err_file = concat!("tests/script_wanings/", stringify!($file), "/warning.txt");

                println!("Loading script: {}", script_file);
                let mut file = File::open(script_file)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;

                let mut file = File::open(err_file)?;
                let mut err = String::new();
                file.read_to_string(&mut err)?;
                let err = err.trim();
                let s = Script::parse(&contents, &*FN_REGISTRY.lock()?)?;
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
    recordpattern_absence_and_extractor,
    recordpattern_presence_and_extractor,
);
