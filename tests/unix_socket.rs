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
use tremor_runtime::config::Binding;
use tremor_runtime::system;
use tremor_runtime::url::TremorUrl;

use hashbrown::HashMap;
use pretty_assertions::assert_eq;
use std::io::prelude::*;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::Path;

use tremor_runtime::errors::*;

use simd_json::json;
use tremor_runtime::repository::BindingArtefact;

const SOCKET_PATH: &'static str = "/tmp/test-unix-socket-onramp.sock";

#[async_std::test]
pub async fn unix_socket_default_permissions() -> Result<()> {
    let (world, _handle) = system::World::start(50).await?;
    let onramp_url = TremorUrl::from_onramp_id("test").expect("");
    let onramp_config = json!({
        "id": "test",
        "type": "unix-socket",
        "codec": "json",
        "preprocessors": [ "lines" ],
        "config": {
            "path": SOCKET_PATH
        }
    });
    let onramp: tremor_runtime::config::OnRamp =
        serde_yaml::from_value(serde_yaml::to_value(onramp_config).expect("")).expect("");

    world
        .repo
        .publish_onramp(&onramp_url, false, onramp)
        .await
        .unwrap();

    Ok(())
}

#[async_std::test]
pub async fn unix_socket() -> Result<()> {
    let (world, _handle) = system::World::start(50).await?;
    let onramp_url = TremorUrl::from_onramp_id("test").expect("");
    let onramp_config = json!({
        "id": "test",
        "type": "unix-socket",
        "codec": "json",
        "preprocessors": [ "lines" ],
        "config": {
            "path": SOCKET_PATH,
            "permissions": "=777"
        }
    });
    let onramp: tremor_runtime::config::OnRamp =
        serde_yaml::from_value(serde_yaml::to_value(onramp_config).expect("")).expect("");

    world
        .repo
        .publish_onramp(&onramp_url, false, onramp)
        .await?;

    let offramp_url = TremorUrl::from_offramp_id("test").expect("");
    let output_file = "/tmp/unix-socket-out.log";

    if Path::new(output_file).exists() {
        std::fs::remove_file(output_file).unwrap();
    }

    let offramp_config = json!({
        "id": "test",
        "type": "file",
        "codec": "json",
        "config": {
            "file": output_file
        }
    });
    let offramp: tremor_runtime::config::OffRamp =
        serde_yaml::from_value(serde_yaml::to_value(offramp_config).expect("")).expect("");

    world
        .repo
        .publish_offramp(&offramp_url, false, offramp)
        .await?;

    let id = TremorUrl::parse(&format!("/pipeline/{}", "test"))?;
    let module_path = &tremor_script::path::ModulePath { mounts: Vec::new() };
    let aggr_reg = tremor_script::aggr_registry();
    let artefact = tremor_pipeline::query::Query::parse(
        &module_path,
        "select event from in into out;",
        "<test>",
        Vec::new(),
        &*tremor_pipeline::FN_REGISTRY.lock()?,
        &aggr_reg,
    )?;
    world.repo.publish_pipeline(&id, false, artefact).await?;

    let binding: Binding = serde_yaml::from_str(
        r#"
id: test
links:
  '/onramp/test/{instance}/out': [ '/pipeline/test/{instance}/in' ]
  '/pipeline/test/{instance}/out': [ '/offramp/test/{instance}/in' ]
"#,
    )?;

    world
        .repo
        .publish_binding(
            &TremorUrl::parse(&format!("/binding/{}", "test"))?,
            false,
            BindingArtefact {
                binding,
                mapping: None,
            },
        )
        .await?;

    let mapping: HashMap<TremorUrl, HashMap<String, String>> = serde_yaml::from_str(
        r#"
/binding/test/01:
  instance: "01"
"#,
    )?;

    let id = TremorUrl::parse(&format!("/binding/{}/01", "test"))?;
    world.link_binding(&id, mapping[&id].clone()).await?;

    std::thread::sleep(std::time::Duration::from_millis(1000));

    assert_eq!(
        0o777,
        std::fs::metadata(SOCKET_PATH).unwrap().permissions().mode() & 0o777
    );

    let mut stream = UnixStream::connect(SOCKET_PATH).unwrap();
    writeln!(stream, "{}", "{\"a\" : 0}").unwrap();
    writeln!(stream, "{}", "{\"b\" : 1}").unwrap();
    writeln!(stream, "{}", "{\"c\" : 2}").unwrap();

    let mut stream2 = UnixStream::connect(SOCKET_PATH).unwrap();
    writeln!(stream2, "{}", "{\"d\" : 3}").unwrap();
    writeln!(stream2, "{}", "{\"e\" : 4}").unwrap();

    std::thread::sleep(std::time::Duration::from_millis(1000));

    world.stop().await?;

    let actual_output = std::fs::read_to_string(output_file).unwrap();
    let mut actual_output: Vec<_> = actual_output.trim().split("\n").collect();
    actual_output.sort();
    let expected_output = vec![
        r#"{"a":0}"#,
        r#"{"b":1}"#,
        r#"{"c":2}"#,
        r#"{"d":3}"#,
        r#"{"e":4}"#,
    ];
    assert_eq!(expected_output, actual_output);

    Ok(())
}
