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
use regex::Regex;
use std::io::prelude::*;
use std::path::Path;
use tremor_common::file;
use tremor_pipeline;
use tremor_pipeline::query::Query;
use tremor_pipeline::FN_REGISTRY;
use tremor_runtime;
use tremor_runtime::errors::*;
use tremor_script::highlighter::{Dumb, Highlighter};
use tremor_script::path::ModulePath;

fn to_query(module_path: &ModulePath, file_name: &str, query: &str) -> Result<Query> {
    let aggr_reg = tremor_script::aggr_registry();
    let cus = vec![];
    Ok(Query::parse(
        module_path,
        query,
        file_name,
        cus,
        &*FN_REGISTRY.lock()?,
        &aggr_reg,
    )?)
}

macro_rules! test_cases {

    ($($file:ident),* ,) => {
        $(
            #[cfg(not(tarpaulin_include))]
            #[test]
            fn $file() -> Result<()> {

                tremor_runtime::functions::load()?;
                let query_dir = concat!("tests/query_warnings/", stringify!($file), "/").to_string();
                let query_file = concat!("tests/query_warnings/", stringify!($file), "/query.trickle");
                let warning_file = concat!("tests/query_warnings/", stringify!($file), "/warning.txt");
                let warning_re_file = concat!("tests/query_warnings/", stringify!($file), "/warning.re");
                let module_path = &ModulePath { mounts: vec![query_dir, "tremor-script/lib/".to_string()] };

                println!("Loading query: {}", query_file);
                let mut file = file::open(query_file)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;

                if Path::new(warning_re_file).exists() {
                    println!("Loading warning: {}", warning_re_file);
                    let mut file = file::open(warning_re_file)?;
                    let mut warning = String::new();
                    file.read_to_string(&mut warning)?;
                    let warning = warning.trim();
                    let re = Regex::new(warning)?;

                    let s = to_query(&module_path, warning_re_file, &contents);
                    if let Err(e) = s {
                        println!("{} ~ {}", warning, format!("{}", e));
                        assert!(re.is_match(&format!("{}", e)));
                    } else {
                        println!("Expected error, but got succeess");
                        assert!(false);
                    }
                } else {
                    println!("Loading error: {}", warning_file);
                    let mut file = file::open(warning_file)?;
                    let mut warning = String::new();
                    file.read_to_string(&mut warning)?;
                    let warning = warning.trim();

                    match to_query(&module_path, warning_file, &contents) {
                        Ok(query) => {
                            let mut h = Dumb::new();
                            query.0.format_warnings_with(&mut h)?;
                            h.finalize()?;
                            let got = h.to_string();
                            let got = got.trim();
                            println!("{}", got);
                            assert_eq!(warning, got);
                        }
                        Err(e) => {
                            assert!(false, "Expected successful parse to Query with warnings, got Error: {}", e);
                        }
                    };
                };
                Ok(())
            }
        )*
    };
}

test_cases!(
    emit_empty_windows,
    // INSERT
);
