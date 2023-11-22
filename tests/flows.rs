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
use serial_test::serial;
use std::io::prelude::*;
use std::time::Duration;
use tremor_common::{alias, file};
use tremor_runtime::{
    errors::*,
    system::{Runtime, ShutdownMode, WorldConfig},
};
use tremor_script::{deploy::Deploy, module::Manager};

fn parse(deploy: &str) -> tremor_script::Result<tremor_script::deploy::Deploy> {
    let aggr_reg = tremor_script::aggr_registry();
    let reg = tremor_script::registry::registry();
    Deploy::parse(&deploy, &reg, &aggr_reg)
}

macro_rules! test_cases {

    ($($file:ident),* ,) => {
        mod flows {
            use super::*;
            $(
                #[tokio::test(flavor = "multi_thread")]
                #[serial(flow)]
                async fn $file() -> Result<()> {

                    let deploy_dir = concat!("tests/flows/", stringify!($file), "/").to_string();
                    let deploy_file = concat!("tests/flows/", stringify!($file), "/flow.troy");
                    Manager::clear_path()?;
                    Manager::add_path(&"tremor-script/lib")?;
                    Manager::add_path(&deploy_dir)?;

                    println!("Loading deployment file: {}", deploy_file);
                    let mut file = file::open(deploy_file)?;
                    let mut contents = String::new();
                    file.read_to_string(&mut contents)?;
                    match parse(&contents) {
                        Ok(deployable) => {
                            let config = WorldConfig{
                                debug_connectors: true,
                            };
                            let (runtime, h) = Runtime::start(config).await?;
                            let app_id = alias::App::default();
                            for flow in deployable.iter_flows() {
                                let flow_alias = alias::Flow::new(app_id.clone(), flow.instance_alias.clone());
                                runtime.deploy_flow(app_id.clone(), flow, tremor_runtime::system::flow::DeploymentType::AllNodes).await?;
                                runtime.start_flow(flow_alias).await?;
                            }
                            runtime.stop(ShutdownMode::Forceful).await?;
                            // this isn't good
                            tokio::time::timeout(Duration::from_secs(10), h).await???;
                        },
                        otherwise => {
                            println!("Expected valid deployment file, compile phase, but got an unexpected error: {otherwise:?}");
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
    // INSERT
    codec_config,
    consts_as_default_args,
    args_in_create,
    chained_pipelines,
);
