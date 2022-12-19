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

use halfbrown::HashMap;

use crate::config::NameWithConfig;

const MIME_TYPES: [(&str, &str); 10] = [
    ("application/json", "json"),
    ("application/yaml", "yaml"),
    ("text/csv", "csv"),
    ("text/plain", "string"),
    ("text/html", "string"),
    ("text/syslog", "syslog"),
    ("application/msgpack", "msgpack"),
    ("application/x-msgpack", "msgpack"),
    ("application/vnd.msgpack", "msgpack"),
    ("application/octet-stream", "binary"),
];

/// additional mapping from codec to mime-types
const CODEC_TO_MIME_TYPES: [(&str, &str); 11] = [
    ("json", "application/json"),
    ("csv", "text/csv"),
    ("string", "text/plain"),
    ("msgpack", "application/msgpack"),
    ("yaml", "application/yaml"),
    ("binary", "application/octet-stream"),
    ("syslog", "text/plain"),
    ("influx", "text/plain"),
    ("binflux", "application/octet-stream"),
    ("statsd", "text/plain"),
    ("dogstatsd", "text/plain"),
];

/// Map from mime-type / content-type to codec name
#[derive(Debug, Clone)]
pub(crate) struct MimeCodecMap {
    by_mime: HashMap<String, NameWithConfig>,
    by_codec: HashMap<String, String>,
}

impl MimeCodecMap {
    pub(crate) fn new() -> Self {
        let by_mime = MIME_TYPES
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.into()))
            .collect::<HashMap<String, NameWithConfig>>();
        let by_codec = CODEC_TO_MIME_TYPES
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<HashMap<String, String>>();
        Self { by_mime, by_codec }
    }

    /// constructs this map while overriding the mapping from mime-type to codec by `custom_codecs`
    pub(crate) fn from_custom(by_mime: HashMap<String, NameWithConfig>) -> Self {
        // We use the existing codec -> mimetype mapping as default to ensure that each
        // codec at least maps to a mime type
        let mut by_codec = CODEC_TO_MIME_TYPES
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<HashMap<String, String>>();

        // After that we overwrite / extend the mapping with the definition given
        // by the user
        for (mime, codec_name) in &by_mime {
            if mime != "*/*" {
                by_codec.insert(codec_name.name.clone(), mime.clone());
            }
        }

        Self { by_mime, by_codec }
    }

    /// get codec name from given Content-Type essence (e.g. "application/json")
    /// if the specific codec isn't found provide the default code3c from `*/*`
    pub fn get_codec_name(&self, content_type: &str) -> Option<&NameWithConfig> {
        self.by_mime
            .get(content_type)
            .or_else(|| self.by_mime.get("*/*"))
    }

    /// get mime type from given codec name
    ///
    /// More expensive lookup than getting the codec name by mime type
    pub fn get_mime_type(&self, codec_name: &str) -> Option<&String> {
        self.by_codec.get(codec_name)
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
    #[test]
    fn get_mime_type() {
        let map = MimeCodecMap::default();
        let csv = Some("text/csv".to_string());
        assert_eq!(csv.as_ref(), map.get_mime_type("csv"));
    }
}
