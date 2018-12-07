// Copyright 2018, Wayfair GmbH
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

#[cfg(feature = "kafka")]
use rdkafka::util::get_rdkafka_version;
use std::io::Write;

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// println_stderr and run_command_or_fail are copied from rdkafka-sys
macro_rules! println_stderr(
    ($($arg:tt)*) => { {
        let r = writeln!(&mut ::std::io::stderr(), $($arg)*);
        r.expect("failed printing to stderr");
    } }
);

pub fn print() {
    println_stderr!("tremor version: {}", VERSION);
    #[cfg(feature = "kafka")]
    {
        let (version_n, version_s) = get_rdkafka_version();
        println_stderr!("rd_kafka version: 0x{:08x}, {}", version_n, version_s);
    }
}
