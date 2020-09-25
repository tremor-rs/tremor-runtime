// Copyright 2020, The Tremor Team
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

use crate::errors::Result;
use simd_json::BorrowedValue;
use tremor_script::Value;
pub(crate) mod binflux;
pub(crate) mod influx;
pub(crate) mod json;
pub(crate) mod msgpack;
pub(crate) mod null;
pub(crate) mod statsd;
pub(crate) mod string;
pub(crate) mod yaml;

mod prelude {
    pub use super::Codec;
    pub use crate::errors::*;
    pub use simd_json::prelude::*;
    pub use tremor_script::prelude::*;
}

/// The codec trait, to encode and decode data
pub trait Codec: Send + Sync {
    /// The canonical name for this codec
    fn name(&self) -> std::string::String;

    /// supported mime types
    /// as <base>/<subtype>
    ///
    /// e.g. application/json
    ///
    /// The returned mime types should be unique to this codec
    fn mime_types(&self) -> Vec<&str> {
        vec![]
    }

    /// Decode a binary, into an Value
    /// If `None` is returned, no data could be encoded, but we don't exactly triggered an error condition.
    ///
    /// # Errors
    ///  * if we can't decode the data
    fn decode<'input>(
        &self,
        data: &'input mut [u8],
        ingest_ns: u64,
    ) -> Result<Option<Value<'input>>>;
    /// Encodes a Value into a binary
    ///
    /// # Errors
    ///  * If the encoding fails
    fn encode(&self, data: &BorrowedValue) -> Result<Vec<u8>>;
    /// Encodes into an existing buffer
    ///
    /// # Errors
    ///  * when we can't write encode to the given vector
    fn encode_into(&self, data: &BorrowedValue, dst: &mut Vec<u8>) -> Result<()> {
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

/// Codec lookup function
///
/// # Errors
///  * if the codec doesn't exist
pub fn lookup(name: &str) -> Result<Box<dyn Codec>> {
    match name {
        "json" => Ok(Box::new(json::JSON {})),
        "msgpack" => Ok(Box::new(msgpack::MsgPack {})),
        "influx" => Ok(Box::new(influx::Influx {})),
        "binflux" => Ok(Box::new(binflux::BInflux {})),
        "null" => Ok(Box::new(null::Null {})),
        "string" => Ok(Box::new(string::String {})),
        "statsd" => Ok(Box::new(statsd::StatsD {})),
        "yaml" => Ok(Box::new(yaml::YAML {})),
        _ => Err(format!("Codec '{}' not found.", name).into()),
    }
}

/// Map from Mime types to codecs for all builtin codecs mappable to Mime types
/// these are all safe mappings
/// if you have a specific codec to be used for a more unspecific mime type
/// like statsd for text/plain
/// these must be specified in a source specific codec_map
pub fn builtin_codec_map() -> halfbrown::HashMap<String, Box<dyn Codec>> {
    let mut codecs: halfbrown::HashMap<String, Box<dyn Codec>> =
        halfbrown::HashMap::with_capacity(7);
    codecs.insert_nocheck("application/json".to_string(), Box::new(json::JSON {}));
    codecs.insert_nocheck("application/yaml".to_string(), Box::new(yaml::YAML {}));
    codecs.insert_nocheck("text/plain".to_string(), Box::new(string::String {}));
    codecs.insert_nocheck("text/html".to_string(), Box::new(string::String {}));
    codecs.insert_nocheck(
        "application/msgpack".to_string(),
        Box::new(msgpack::MsgPack {}),
    );
    codecs.insert_nocheck(
        "application/x-msgpack".to_string(),
        Box::new(msgpack::MsgPack {}),
    );
    codecs.insert_nocheck(
        "application/vnd.msgpack".to_string(),
        Box::new(msgpack::MsgPack {}),
    );
    // TODO: add more codecs
    codecs
}

#[must_use]
/// lookup a codec by mime type
///
/// # Errors
/// if no codec could be found for the given mime type
pub fn by_mime_type(mime: &str) -> Result<Box<dyn Codec>> {
    match mime {
        "application/json" => Ok(Box::new(json::JSON {})),
        "application/yaml" => Ok(Box::new(yaml::YAML {})),
        "text/plain" => Ok(Box::new(string::String {})),
        "text/html" => Ok(Box::new(string::String {})),
        "application/msgpack" | "application/x-msgpack" | "application/vnd.msgpack" => {
            Ok(Box::new(msgpack::MsgPack {}))
        }
        _ => Err(format!("No codec found for mime type '{}'", mime).into()),
    }
}
