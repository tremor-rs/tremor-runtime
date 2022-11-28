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
use async_std::prelude::FutureExt;
use serial_test::serial;
use std::io::prelude::*;
use std::time::Duration;
use tremor_common::file;
use tremor_runtime::{
    errors::*,
    system::{World, WorldConfig},
};
use tremor_script::{deploy::Deploy, module::Manager};

fn parse(deploy: &str) -> tremor_script::Result<tremor_script::deploy::Deploy> {
    let aggr_reg = tremor_script::aggr_registry();
    let reg = tremor_script::registry::registry();
    Deploy::parse(deploy, &reg, &aggr_reg)
}

async fn deploy_test_config(contents: String) -> Result<()> {
    match parse(&contents) {
        Ok(deployable) => {
            let config = WorldConfig {
                debug_connectors: true,
                ..WorldConfig::default()
            };
            let (world, h) = World::start(config).await?;
            for flow in deployable.iter_flows() {
                world.start_flow(flow).await?;
            }
            // this isn't good
            h.timeout(Duration::from_secs(10)).await??;
        }
        otherwise => {
            panic!(
                "Expected valid deployment file, compile phase, but got an unexpected error: {:?}",
                otherwise
            );
        }
    }

    Ok(())
}

async fn main_config(file: &str) -> Result<()> {
    // serial_test::set_max_wait(Duration::from_secs(600));

    let deploy_dir = &format!("tests/flows/{file}/");
    let deploy_file = &format!("tests/flows/{file}/flow.troy");
    Manager::clear_path()?;
    Manager::add_path(&"tremor-script/lib")?;
    Manager::add_path(deploy_dir)?;

    println!("Loading deployment file: {deploy_file}");
    let mut file = file::open(deploy_file)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    deploy_test_config(contents).await
}

macro_rules! test_cases {

    ($($file:ident),* ,) => {
        mod flows {
            use super::*;
            $(
                #[async_std::test]
                #[serial(flow, timeout_ms = 6000000)]
                async fn $file() -> Result<()> {
					main_config(stringify!($file)).await
                }
            )*
        }
    };
}

test_cases!(
    pipeline_identity,
    pipeline_args,
    pipeline_with,
    // INSERT
    consts_as_default_args,
    args_in_create,
    chained_pipelines,
);
