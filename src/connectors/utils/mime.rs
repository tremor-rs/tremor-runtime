// Copyright 2022, The Tremor Team
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

use crate::codec::{self, binary, json, msgpack, string, yaml, Codec};
use crate::config::NameWithConfig;
use crate::errors::Result;
use halfbrown::HashMap;
use serde::{
    de::{MapAccess, Visitor},
    ser::SerializeMap,
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::fmt;

const MIME_TYPES: [&str; 8] = [
    "application/json",
    "application/yaml",
    "text/plain",
    "text/html",
    "application/msgpack",
    "application/x-msgpack",
    "application/vnd.msgpack",
    "application/octet-stream",
];

/// Map from Mime types to codecs for all builtin codecs mappable to Mime types
/// these are all safe mappings
/// if you have a specific codec to be used for a more unspecific mime type
/// like statsd for text/plain
/// these must be specified in a source specific `codec_map`
#[must_use]
pub fn builtin_codec_map() -> halfbrown::HashMap<String, Box<dyn Codec>> {
    MIME_TYPES
        .iter()
        .filter_map(|t| Some(((*t).to_string(), by_mime_type(t).ok()?)))
        .collect()
}

/// lookup a codec by mime type
///
/// # Errors
/// if no codec could be found for the given mime type
pub fn by_mime_type(mime: &str) -> Result<Box<dyn Codec>> {
    match mime {
        "application/json" => Ok(Box::new(json::Json::<json::Unsorted>::default())),
        "application/yaml" => Ok(Box::new(yaml::Yaml {})),
        "text/plain" | "text/html" => Ok(Box::new(string::String {})),
        "application/msgpack" | "application/x-msgpack" | "application/vnd.msgpack" => {
            Ok(Box::new(msgpack::MsgPack {}))
        }
        "application/octet-stream" => Ok(Box::new(binary::Binary {})),
        _ => Err(format!("No codec found for mime type '{}'", mime).into()),
    }
}

#[derive(Debug)]
pub(crate) struct MimeCodecMap {
    pub(crate) map: HashMap<String, Box<dyn Codec>>,
}

impl Clone for MimeCodecMap {
    fn clone(&self) -> Self {
        let mut h = HashMap::new();
        for (k, v) in &self.map {
            if let Some(v) = h.insert(k.clone(), v.boxed_clone()) {
                // NOTE as duplicates shouldn't occur, we ignore
                trace!(
                    "Unespected duplicate mime type codec mapping {} / {}",
                    k,
                    v.name()
                );
            }
        }
        Self { map: h }
    }
}

impl MimeCodecMap {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn with_builtin() -> Self {
        Self::default() // We simply wrap default
    }

    pub fn with_capacity(size: usize) -> Self {
        Self {
            map: HashMap::with_capacity(size),
        }
    }

    pub fn default() -> Self {
        Self {
            map: builtin_codec_map(),
        }
    }
}

impl Serialize for MimeCodecMap {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.map.len()))?;
        for (k, v) in &self.map {
            map.serialize_entry(k, &v.name())?;
        }
        map.end()
    }
}

impl<'de> Visitor<'de> for MimeCodecMap {
    type Value = MimeCodecMap;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("MIME Codec map")
    }

    fn visit_map<M>(self, mut access: M) -> std::result::Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut map = MimeCodecMap::with_capacity(access.size_hint().unwrap_or(0));

        while let Some((key, value)) = access.next_entry::<String, String>()? {
            let value: String = value.to_string();
            let codec = codec::resolve(&NameWithConfig::from(&value));
            match codec {
                Ok(codec) => map.map.insert(key, codec),
                Err(_) => {
                    return Err(serde::de::Error::custom(format_args!(
                        "invalid codec {} specified",
                        value
                    )));
                }
            };
        }

        Ok(map)
    }
}

impl<'de> Deserialize<'de> for MimeCodecMap {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(MimeCodecMap::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tremor_script::literal;
    use tremor_value::structurize;

    #[test]
    fn builtin_codec_map() {
        let map = super::builtin_codec_map();

        for t in super::MIME_TYPES.iter() {
            assert!(map.contains_key(*t));
        }
    }
    #[test]
    fn by_mime_type() {
        for t in super::MIME_TYPES.iter() {
            assert!(super::by_mime_type(t).is_ok());
        }
        assert_eq!(
            super::by_mime_type("application/badger")
                .err()
                .unwrap()
                .to_string(),
            "No codec found for mime type 'application/badger'"
        );
    }

    #[test]
    fn codec_map_serde() -> Result<()> {
        let empty = literal!({});
        let mime_codec_map: MimeCodecMap = structurize(empty)?;
        assert_eq!(0, mime_codec_map.map.len());

        let absent = literal!(null);
        let mime_codec_map: std::result::Result<MimeCodecMap, _> = structurize(absent);
        assert!(mime_codec_map.is_err());

        let bad_codec_name = literal!({
            "application/json": "snot",
        });
        let mime_codec_map: std::result::Result<MimeCodecMap, _> = structurize(bad_codec_name);
        assert!(mime_codec_map.is_err());

        let valid = literal!({
            "application/json": "json",
        });
        let mime_codec_map: MimeCodecMap = structurize(valid)?;
        let codec = mime_codec_map.map.get("application/json");
        assert!(codec.is_some());
        let codec = codec.unwrap();
        assert_eq!("json", codec.name());

        // TODO consider errors/warnings for unknown mime string codes

        Ok(())
    }
}
