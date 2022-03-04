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
use std::{io::prelude::*, sync::Mutex};
use tremor_common::file;
use tremor_script::{deploy::Deploy, errors::*, highlighter::Dumb, module::Manager};

fn parse<'script>(deploy: &str) -> tremor_script::Result<Deploy> {
    let aggr_reg = tremor_script::aggr_registry();
    let reg = tremor_script::registry::registry();
    Deploy::parse(deploy, &reg, &aggr_reg)
}
// Since we need to run one test at a time as we do test against module path
// we have to lineralize them :(
lazy_static::lazy_static! {
    static ref UNIQUE: Mutex<()> = Mutex::new(());
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
                let l = UNIQUE.lock();
                Manager::clear_path()?;
                Manager::add_path(&deploy_dir)?;
                Manager::add_path(&"tremor-script/lib")?;


                println!("Loading deployment: {}", deploy_file);
                let mut file = file::open(deploy_file)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;
                println!("{}", &contents);

                println!("Loading error: {}", err_file);
                let mut file = file::open(err_file)?;
                let mut err = String::new();
                file.read_to_string(&mut err)?;

                match parse(&contents) {
                    Err(e) => {
                        let got = Dumb::error_to_string(&e)?;
                        print!("{}", got);
                        println!("got wrong error: {:?}", e);

                        assert_eq!(err.trim(), got.trim());
                    }
                    _ =>{
                        println!("Expected error, but got succeess");
                        assert!(false);
                    }
                };
                drop(l);
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
