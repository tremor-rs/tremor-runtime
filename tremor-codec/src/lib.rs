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

//! Tremor codecs for decoding and encoding data from wire format to tremor's internal representation

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

pub mod errors;
pub use crate::errors::Error;
use crate::errors::{Kind as ErrorKind, Result};
use std::fmt::{Debug, Display};
use tremor_value::Value;
mod codec {
    pub(crate) mod avro;
    pub(crate) mod binary;
    pub(crate) mod binflux;
    pub(crate) mod confluent_schema_registry;
    pub(crate) mod csv;
    pub(crate) mod dogstatsd;
    pub(crate) mod influx;
    /// JSON codec
    pub mod json;
    pub(crate) mod msgpack;
    pub(crate) mod null;
    pub(crate) mod statsd;
    pub(crate) mod string;
    pub(crate) mod syslog;
    /// Tremor to Tremor codec
    pub(crate) mod tremor;
    pub(crate) mod yaml;
}

pub use codec::*;

mod prelude {
    pub use super::Codec;
    pub use crate::errors::*;
    pub use simd_json::prelude::*;
    pub use tremor_value::{literal, Object, Value};
}

/// Configuration, commonly used for codecs
pub type Config = tremor_config::NameWithConfig;

#[async_trait::async_trait]
/// The codec trait, to encode and decode data
pub trait Codec: Send + Sync {
    /// The canonical name for this codec
    fn name(&self) -> &str;

    /// supported mime types
    /// as <base>/<subtype>
    ///
    /// e.g. application/json
    ///
    /// The returned mime types should be unique to this codec

    fn mime_types(&self) -> Vec<&'static str> {
        vec![]
    }

    /// Decode a binary, into an Value
    /// If `None` is returned, no data could be encoded, but we don't exactly triggered an error condition.
    ///
    /// # Errors
    ///  * if we can't decode the data
    async fn decode<'input>(
        &mut self,
        data: &'input mut [u8],
        ingest_ns: u64,
        meta: Value<'input>,
    ) -> Result<Option<(Value<'input>, Value<'input>)>>;
    /// Encodes a Value into a binary
    ///
    /// # Errors
    ///  * If the encoding fails
    async fn encode(&mut self, data: &Value, meta: &Value) -> Result<Vec<u8>>;

    /// special clone method for getting clone functionality
    /// into a this trait referenced as trait object
    /// otherwise we cannot use this type inside structs that need to be `Clone`.
    /// See: `crate::codec::rest::BoxedCodec`
    fn boxed_clone(&self) -> Box<dyn Codec>;
}

impl Display for dyn Codec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Codec: {}", self.name())
    }
}

impl Debug for dyn Codec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Codec: {}", self.name())
    }
}

/// resolve a codec from either a codec name of a full-blown config
///
/// # Errors
///  * if the codec doesn't exist
pub fn resolve(config: &Config) -> Result<Box<dyn Codec>> {
    match config.name.as_str() {
        "avro" => avro::Avro::from_config(config.config.as_ref()),
        "confluent-schema-registry" => {
            confluent_schema_registry::Csr::from_config(config.config.as_ref())
        }
        "binary" => Ok(Box::new(binary::Binary {})),
        "binflux" => Ok(Box::<binflux::BInflux>::default()),
        "csv" => Ok(Box::new(csv::Csv {})),
        "dogstatsd" => Ok(Box::<dogstatsd::DogStatsD>::default()),
        "json" => json::from_config(config.config.as_ref()),
        "msgpack" => Ok(Box::new(msgpack::MsgPack {})),
        "influx" => Ok(Box::new(influx::Influx {})),
        "null" => Ok(Box::new(null::Null {})),
        "statsd" => Ok(Box::<statsd::StatsD>::default()),
        "string" => Ok(Box::new(string::String {})),
        "syslog" => Ok(Box::new(syslog::Syslog::utcnow())),
        "tremor" => Ok(Box::<tremor::Tremor>::default()),
        "yaml" => Ok(Box::new(yaml::Yaml {})),
        s => Err(ErrorKind::CodecNotFound(s.into()).into()),
    }
}

#[cfg(test)]
mod test {
    use tremor_config::NameWithConfig;
    use tremor_value::literal;

    #[test]
    fn lookup() {
        assert!(super::resolve(&"binflux".into()).is_ok());
        assert!(super::resolve(&"dogstatsd".into()).is_ok());
        assert!(super::resolve(&"influx".into()).is_ok());
        assert!(super::resolve(&"json".into()).is_ok());
        assert!(super::resolve(&NameWithConfig {
            name: "json".into(),
            config: None
        })
        .is_ok());
        assert!(super::resolve(&NameWithConfig {
            name: "json".into(),
            config: Some(literal!({"mode": "sorted"}))
        })
        .is_ok());
        assert!(super::resolve(&NameWithConfig {
            name: "json".into(),
            config: Some(literal!({"mode": "unsorted"}))
        })
        .is_ok());

        assert!(super::resolve(&NameWithConfig {
            name: "json".into(),
            config: Some(literal!({"mode": "badger"}))
        })
        .is_err());
        assert!(super::resolve(&"json".into()).is_ok());
        assert!(super::resolve(&"msgpack".into()).is_ok());
        assert!(super::resolve(&"null".into()).is_ok());
        assert!(super::resolve(&"statsd".into()).is_ok());
        assert!(super::resolve(&"string".into()).is_ok());
        assert!(super::resolve(&"syslog".into()).is_ok());
        assert!(super::resolve(&"tremor".into()).is_ok());
        assert!(super::resolve(&"yaml".into()).is_ok());
        assert!(super::resolve(&"snot".into()).is_err(),);
    }
}
