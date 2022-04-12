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
use std::io::prelude::*;
use tremor_common::file;
use tremor_script::deploy::Deploy;
use tremor_script::errors::*;
use tremor_script::highlighter::{Dumb, Highlighter};
use tremor_script::ModuleManager;

fn parse<'script>(deploy: &str) -> tremor_script::Result<Deploy> {
    let aggr_reg = tremor_script::aggr_registry();
    let reg = tremor_script::registry::registry();
    Deploy::parse(deploy, &reg, &aggr_reg)
}

macro_rules! test_cases {

    ($($file:ident),* ,) => {
        mod deploy_errors {
            use super::*;
            use pretty_assertions::assert_eq;
        $(
            #[test]
            fn $file() -> Result<()> {

                let deploy_dir = concat!("tests/deploy_errors/", stringify!($file), "/").to_string();
                let deploy_file = concat!("tests/deploy_errors/", stringify!($file), "/deploy.troy");
                let err_file = concat!("tests/deploy_errors/", stringify!($file), "/error.txt");
                ModuleManager::add_path("tremor-script/lib")?;
                ModuleManager::add_path(deploy_dir)?;


                println!("Loading deployment: {}", deploy_file);
                let mut file = file::open(deploy_file)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;
                println!("{}", &contents);

                println!("Loading error: {}", err_file);
                let mut file = file::open(err_file)?;
                let mut err = String::new();
                file.read_to_string(&mut err)?;
                let err = err.trim();

                match parse(&contents) {
                    Err(e) => {
                        let mut h = Dumb::new();
                        h.format_error(&e)?;
                        h.finalize()?;
                        let got = h.to_string();
                        let got = got.trim();
                        println!("{}", got);
                        println!("got wrong error: {:?}", e);

                        assert_eq!(err, got);
                    }
                    _ =>{
                        println!("Expected error, but got succeess");
                        assert!(false);
                    }
                };
                Ok(())
            }
        )*
        }
    };
}

test_cases!(
    module_not_found,
    pipeline_bad_query,
    connector_no_kind,
    connector_bad_kind,
    // connector_bad_with, look at this once we have connectors included
    flow_not_found,
);
