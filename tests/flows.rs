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
use std::time::Duration;
use tremor_common::asy::file;
use tremor_runtime::{errors::*, system::Runtime};
use tremor_script::module::Manager;
use tremor_value::Value;

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
                    let config_file = concat!("tests/flows/", stringify!($file), "/config.json");

                    Manager::clear_path()?;
                    Manager::add_path(&"tremor-script/lib")?;
                    Manager::add_path(&deploy_dir)?;

                    println!("Loading deployment file: {deploy_file}");

                    let mut archive = Vec::new();
                    tremor_archive::package(
                        &mut archive,
                        deploy_file,
                        Some(stringify!($file).to_string()),
                        Some("main".to_string()),
                    ).await?;

                    let app = tremor_archive::get_app(archive.as_slice()).await?;


                    assert_eq!(stringify!($file), app.name.0);

                    println!("Loading config file: {config_file}");
                    let flow_config = file::read(config_file).await.ok().and_then(|mut c| simd_json::from_slice::<Value>(&mut c).ok().map(Value::into_static));

                    let (world, h) = Runtime::builder().default_include_connectors().build().await?;

                    tremor_runtime::load_archive(&world, archive.as_slice(), None, flow_config).await?;

                    tokio::time::timeout(Duration::from_secs(10), h).await???;

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
