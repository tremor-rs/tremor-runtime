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

//! Appends the separation character at the end of each event's byte stream, the default character is '\n' (newline). It can be overwritten with the `separator` option.
//!
//! | Option      | Description                                            | Required | Default Value |
//! |-------------|--------------------------------------------------------|----------|---------------|
//! | `separator` | The separator to append after each event's byte stream | no       | `\n`          |

use super::Postprocessor;
use crate::preprocessor::separate::{default_separator, DEFAULT_SEPARATOR};
use serde::Deserialize;
use tremor_config::{Impl as ConfigImpl, Map as ConfigMap};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_separator")]
    separator: String,
}

impl tremor_config::Impl for Config {}

pub(crate) struct Separate {
    separator: u8,
}

impl Default for Separate {
    fn default() -> Self {
        Self {
            separator: DEFAULT_SEPARATOR,
        }
    }
}
/// Seperator error
#[derive(Debug, thiserror::Error)]

pub enum Error {
    /// Invalid Seperator
    #[error("Invalid 'separator': \"{0}\", must be 1 byte.")]
    InvalidSeparator(String),
    /// Config Error
    #[error("Invalid Configuration for separate postprocessor: {0}")]
    InvalidConfiguration(#[from] tremor_value::Error),
}

impl Separate {
    pub(super) fn from_config(config: &ConfigMap) -> Result<Self, super::Error> {
        let separator = if let Some(raw_config) = config {
            let config = Config::new(raw_config)
                .map_err(|e| super::Error::InvalidConfig("seperate", e.into()))?;
            if config.separator.bytes().len() != 1 {
                return Err(super::Error::InvalidConfig(
                    "seperate",
                    Error::InvalidSeparator(config.separator).into(),
                ));
            }
            config.separator.as_bytes()[0]
        } else {
            DEFAULT_SEPARATOR
        };
        Ok(Self { separator })
    }
}

impl Postprocessor for Separate {
    fn name(&self) -> &str {
        "join"
    }

    fn process(
        &mut self,
        _ingres_ns: u64,
        _egress_ns: u64,
        data: &[u8],
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        // padding capacity with 1 to account for the new line char we will be pushing
        let mut framed: Vec<u8> = Vec::with_capacity(data.len() + 1);
        framed.extend_from_slice(data);
        framed.push(self.separator);
        Ok(vec![framed])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tremor_value::literal;

    #[test]
    fn separate_postprocessor() -> anyhow::Result<()> {
        let config = Some(literal!({
            "separator": "|"
        }));
        let mut separate = Separate::from_config(&config)?;
        let data: [u8; 0] = [];
        assert_eq!(separate.process(0, 0, &data).ok(), Some(vec![vec![b'|']]));
        assert_eq!(
            separate.process(0, 0, b"foob").ok(),
            Some(vec![vec![b'f', b'o', b'o', b'b', b'|']]),
        );
        assert!(separate.finish(None)?.is_empty());
        Ok(())
    }

    #[test]
    fn separate_postprocessor_default() -> anyhow::Result<()> {
        let mut separate = Separate::from_config(&None)?;
        let data: [u8; 0] = [];
        assert_eq!(separate.process(0, 0, &data).ok(), Some(vec![vec![b'\n']]));
        assert_eq!(
            separate.process(0, 0, b"foob").ok(),
            Some(vec![vec![b'f', b'o', b'o', b'b', b'\n']]),
        );
        assert!(separate.finish(None)?.is_empty());
        Ok(())
    }

    #[test]
    fn from_config_invalid_separator() {
        let config = Some(literal!({
            "separator": "abc"
        }));
        let res = Separate::from_config(&config)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();

        assert_eq!(
            "seperate Invalid config: Invalid 'separator': \"abc\", must be 1 byte.",
            res
        );
    }
}
