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

use std::env;
use tremor_grpc_build::generate;

fn get_git_branch() -> std::io::Result<String> {
    use std::process::Command;

    let branch = Command::new("git")
        .arg("rev-parse")
        .arg("--abbrev-ref")
        .arg("HEAD")
        .output();
    branch.map(|branch_output| {
        let branch_string = String::from_utf8_lossy(&branch_output.stdout);
        branch_string
            .lines()
            .next()
            .unwrap_or("unknown_branch")
            .to_string()
    })
}

fn get_git_commit() -> std::io::Result<String> {
    use std::process::Command;

    let commit = Command::new("git")
        .arg("rev-parse")
        .arg("--verify")
        .arg("HEAD")
        .output();

    commit.map(|commit| {
        let commit_string = String::from_utf8_lossy(&commit.stdout);
        commit_string
            .lines()
            .next()
            .unwrap_or("unknown_hash")
            .to_string()
    })
}

#[cfg(feature = "grpc")]
fn generate_grpc_impls_code() {
    let out_dir = env::var_os("OUT_DIR").unwrap();
    let proto = env::var_os("PROTO_FILES");
    let include = env::var_os("INCLUDE");
    if let Some((proto, include)) = proto.zip(include) {
        let protos = proto.to_str().unwrap().split(',').collect::<Vec<&str>>();
        let includes = include.to_str().unwrap().split(',').collect::<Vec<&str>>();

        let mut config = prost_build::Config::new();
        config.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");
        tonic_build::configure()
            .compile_with_config(config, &protos, &includes)
            .unwrap();
        generate(&protos, &includes, out_dir, false, true).unwrap();
    }
}

fn main() {
    println!("cargo:rerun-if-changed=.git");
    if let Ok(branch) = get_git_branch() {
        println!("cargo:rustc-env=VERSION_BRANCH={}", branch);
    }
    if let Ok(commit) = get_git_commit() {
        println!("cargo:rustc-env=VERSION_HASH={}", commit);
    }
    #[cfg(feature = "grpc")]
    generate_grpc_impls_code()
}
