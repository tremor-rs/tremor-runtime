// Copyright 2018-2020, Wayfair GmbH
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

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn print() {
    eprintln!("tremor version: {}", VERSION);
    #[cfg(feature = "kafka")]
    {
        let (version_n, version_s) = get_rdkafka_version();
        eprintln!("rd_kafka version: 0x{:08x}, {}", version_n, version_s);
    }
}

pub fn log() {
    info!("tremor version: {}", VERSION);
    #[cfg(feature = "kafka")]
    {
        let (version_n, version_s) = get_rdkafka_version();
        info!("rd_kafka version: 0x{:08x}, {}", version_n, version_s);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn for_coverage_only() {
        print();
        log();
    }
}
