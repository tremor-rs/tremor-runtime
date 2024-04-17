// Copyright 2024, The Tremor Team
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

#![deny(warnings)]
#![deny(missing_docs)]
#![recursion_limit = "1024"]
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic,
    clippy::mod_module_files
)]
//! Helpers for testing tremor connectors

use log::warn;

/// Free port finder
pub mod free_port;

/// Setup for TLS
/// # Panics
/// If the TLS cert/key could not be created
pub fn setup_for_tls() {
    use std::process::Command;
    use std::process::Stdio;
    use std::sync::Once;

    static TLS_SETUP: Once = Once::new();

    // create TLS cert and key only once at the beginning of the test execution to avoid
    // multiple threads stepping on each others toes
    TLS_SETUP.call_once(|| {
        warn!("Refreshing TLS Cert/Key...");
        let mut cmd = Command::new("./tests/refresh_tls_cert.sh")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            // ALLOW: This is for tests only, but we should extract it
            .expect("Unable to spawn ./tests/refresh_tls_cert.sh");
        // ALLOW: This is for tests only, but we should extract it
        let out = cmd.wait().expect("Failed to refresh certs/keys");
        match out.code() {
            Some(0) => {
                println!("Done refreshing TLS Cert/Key.");
                warn!("Done refreshing TLS Cert/Key.");
            }
            // ALLOW: This is for tests only, but we should extract it
            _ => panic!("Error creating tls certificate and key"),
        }
    });
}
