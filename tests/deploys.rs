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
use tremor_script::Manager;

fn parse<'script>(deploy: &str) -> tremor_script::Result<tremor_script::deploy::Deploy> {
    let aggr_reg = tremor_script::aggr_registry();
    let reg = tremor_script::registry::registry();
    Deploy::parse(deploy, &reg, &aggr_reg)
}

macro_rules! test_cases {

    ($($file:ident),* ,) => {
        mod deploys {
            use super::*;

            $(
                #[test]
                fn $file() -> Result<()> {
                    let deploy_dir = concat!("tests/deploys/", stringify!($file), "/").to_string();
                    let deploy_file = concat!("tests/deploys/", stringify!($file), "/deploy.troy");
                    ModuleManager::add_path("tremor-script/lib")?;
                    ModuleManager::add_path(deploy_dir)?;

                    println!("Loading deployment file: {}", deploy_file);
                    let mut file = file::open(deploy_file)?;
                    let mut contents = String::new();
                    file.read_to_string(&mut contents)?;

                    match parse(&contents) {
                        Ok(_) => (),
                        otherwise => {
                            println!("Expected valid deployment file, compile phase, but got an unexpected error: {:?}", otherwise);
                            assert!(false);
                        }
                    }

                    Ok(())
                }
            )*
        }
    };
}

test_cases!(
    pipeline_identity,
    pipeline_args,
    pipeline_with,
    pipeline_overalls, // TODO: Work through args and config
);
