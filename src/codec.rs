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

use crate::{
    config,
    errors::{Kind as ErrorKind, Result},
};
use std::fmt::{Debug, Display};
use tremor_script::Value;
pub(crate) mod binary;
pub(crate) mod binflux;
pub(crate) mod csv;
pub(crate) mod dogstatsd;
pub(crate) mod influx;
pub(crate) mod json;
pub(crate) mod msgpack;
pub(crate) mod null;
pub(crate) mod statsd;
pub(crate) mod string;
pub(crate) mod syslog;
pub(crate) mod yaml;

mod prelude {
    pub use super::Codec;
    pub use crate::errors::*;
    pub use tremor_script::prelude::*;
    pub use tremor_value::{Object, Value};
}

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
    fn decode<'input>(
        &mut self,
        data: &'input mut [u8],
        ingest_ns: u64,
    ) -> Result<Option<Value<'input>>>;
    /// Encodes a Value into a binary
    ///
    /// # Errors
    ///  * If the encoding fails
    fn encode(&self, data: &Value) -> Result<Vec<u8>>;

    /// Encodes into an existing buffer
    ///
    /// # Errors
    ///  * when we can't write encode to the given vector

    fn encode_into(&self, data: &Value, dst: &mut Vec<u8>) -> Result<()> {
        let mut res = self.encode(data)?;
        std::mem::swap(&mut res, dst);
        Ok(())
    }

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
pub fn resolve(config: &config::Codec) -> Result<Box<dyn Codec>> {
    match config.name.as_str() {
        "json" => Ok(Box::new(json::Json::<json::Unsorted>::default())),
        "json-sorted" => Ok(Box::new(json::Json::<json::Sorted>::default())),
        "msgpack" => Ok(Box::new(msgpack::MsgPack {})),
        "influx" => Ok(Box::new(influx::Influx {})),
        "binflux" => Ok(Box::new(binflux::BInflux {})),
        "null" => Ok(Box::new(null::Null {})),
        "string" => Ok(Box::new(string::String {})),
        "statsd" => Ok(Box::new(statsd::StatsD {})),
        "dogstatsd" => Ok(Box::new(dogstatsd::DogStatsD {})),
        "yaml" => Ok(Box::new(yaml::Yaml {})),
        "binary" => Ok(Box::new(binary::Binary {})),
        "syslog" => Ok(Box::new(syslog::Syslog::utcnow())),
        "csv" => Ok(Box::new(csv::Csv {})),
        s => Err(ErrorKind::CodecNotFound(s.into()).into()),
    }
}

#[cfg(test)]
mod test {

    #[test]
    fn lookup() {
        assert!(super::resolve(&"json".into()).is_ok());
        assert!(super::resolve(&"json-sorted".into()).is_ok());
        assert!(super::resolve(&"msgpack".into()).is_ok());
        assert!(super::resolve(&"influx".into()).is_ok());
        assert!(super::resolve(&"binflux".into()).is_ok());
        assert!(super::resolve(&"null".into()).is_ok());
        assert!(super::resolve(&"string".into()).is_ok());
        assert!(super::resolve(&"statsd".into()).is_ok());
        assert!(super::resolve(&"dogstatsd".into()).is_ok());
        assert!(super::resolve(&"yaml".into()).is_ok());
        assert!(super::resolve(&"syslog".into()).is_ok());
        assert_eq!(
            super::resolve(&"snot".into()).err().unwrap().to_string(),
            "Codec \"snot\" not found."
        )
    }
}
