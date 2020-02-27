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

use crate::errors::*;
use simd_json::BorrowedValue;
use tremor_script::LineValue;
pub(crate) mod influx;
pub(crate) mod json;
pub(crate) mod msgpack;
pub(crate) mod null;
pub(crate) mod statsd;
pub(crate) mod string;
pub(crate) mod yaml;

/// The codec trait, to encode and decode data
pub trait Codec: Send + Sync {
    /// Decode a binary, into an Value
    fn decode(&mut self, data: Vec<u8>, ingest_ns: u64) -> Result<Option<LineValue>>;
    /// Encodes a Value into a binary
    fn encode(&self, data: &BorrowedValue) -> Result<Vec<u8>>;
}

/// Codec lookup function
#[cfg_attr(tarpaulin, skip)]
pub fn lookup(name: &str) -> Result<Box<dyn Codec>> {
    match name {
        "json" => Ok(Box::new(json::JSON {})),
        "msgpack" => Ok(Box::new(msgpack::MsgPack {})),
        "influx" => Ok(Box::new(influx::Influx {})),
        "binflux" => Ok(Box::new(influx::BInflux {})),
        "null" => Ok(Box::new(null::Null {})),
        "string" => Ok(Box::new(string::String {})),
        "statsd" => Ok(Box::new(statsd::StatsD {})),
        "yaml" => Ok(Box::new(yaml::YAML {})),
        _ => Err(format!("Codec '{}' not found.", name).into()),
    }
}
