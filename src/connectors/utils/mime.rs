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

use crate::errors::Result;
use bimap::BiMap;
use halfbrown::HashMap;
use serde::{Deserialize, Serialize};

const MIME_TYPES: [(&str, &str); 9] = [
    ("application/json", "json"),
    ("application/yaml", "yaml"),
    ("text/csv", "csv"),
    ("text/plain", "string"),
    ("text/html", "string"),
    ("application/msgpack", "msgpack"),
    ("application/x-msgpack", "msgpack"),
    ("application/vnd.msgpack", "msgpack"),
    ("application/octet-stream", "binary"),
];

/// Map from mime-type / content-type to codec name
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct MimeCodecMap {
    map: BiMap<String, String>,
}

impl MimeCodecMap {
    fn new() -> Self {
        Self {
            map: MIME_TYPES
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<BiMap<_, _>>(),
        }
    }

    pub fn with_overwrites(custom_codecs: &HashMap<String, String>) -> Result<Self> {
        let mut base = Self::new();
        for (mime, codec_name) in custom_codecs {
            base.map.insert(mime.clone(), codec_name.clone());
        }
        Ok(base)
    }

    /// get codec name from given Content-Type essence (e.g. "application/json")
    pub fn get_codec_name(&self, content_type: &str) -> Option<&String> {
        self.map.get_by_left(content_type)
    }

    /// get mime type from given codec name
    pub fn get_mime_type(&self, codec_name: &str) -> Option<&String> {
        self.map.get_by_right(codec_name)
    }
}

impl Default for MimeCodecMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tremor_script::literal;
    use tremor_value::structurize;

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
        let codec = mime_codec_map.get_codec_name("application/json");
        assert_eq!("json", codec.unwrap());

        // TODO consider errors/warnings for unknown mime string codes

        Ok(())
    }
}
