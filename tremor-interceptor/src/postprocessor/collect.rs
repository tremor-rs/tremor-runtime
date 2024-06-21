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

//! Combines output into a single result before passing them on to the next postprocessor or sink.
//! The processor will **not** split messages if they exceed the `max-bytes` limit instead pass them through as is.
//!
//! | Option      | Description                                                                         | Required | Default Value |
//! |-------------|-------------------------------------------------------------------------------------|----------|---------------|
//! | `max-bytes` | The maximum number of bytes collected at a time - 0 means only on finalize we flush | no       | 0             |

use super::Postprocessor as Trait;
use anyhow::Ok;
use serde::Deserialize;
use tremor_config::{Impl as ConfigImpl, Map as ConfigMap};

#[derive(Clone, Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    max_bytes: usize,
}

impl tremor_config::Impl for Config {}

#[derive(Debug, Default)]
pub(crate) struct Postprocessor {
    max_bytes: usize,
    buffer: Vec<u8>,
}

impl Postprocessor {
    pub(super) fn from_config(config: &ConfigMap) -> Result<Self, anyhow::Error> {
        let config = config
            .as_ref()
            .map_or_else(|| Ok(Config::default()), |config| Ok(Config::new(config)?))?;
        Ok(Self {
            max_bytes: config.max_bytes,
            buffer: Vec::new(),
        })
    }
}

impl Trait for Postprocessor {
    fn is_streaming(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "collect"
    }

    fn process(
        &mut self,
        _ingres_ns: u64,
        _egress_ns: u64,
        data: &[u8],
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        if self.max_bytes != 0 && self.buffer.len() + data.len() > self.max_bytes {
            // If data itself is larger than max_bytes, we ignore this as we do not know the semantics of the data
            // and randomly splitting it would be incorrect in the scope of a combine operation
            let mut buf = data.to_vec();
            std::mem::swap(&mut buf, &mut self.buffer);
            Ok(vec![buf])
        } else {
            self.buffer.extend_from_slice(data);
            Ok(vec![])
        }
    }
    fn finish(&mut self, data: Option<&[u8]>) -> anyhow::Result<Vec<Vec<u8>>> {
        let mut buf = Vec::new();
        std::mem::swap(&mut buf, &mut self.buffer);

        if let Some(data) = data {
            if self.max_bytes != 0 && buf.len() + data.len() > self.max_bytes {
                Ok(vec![buf, data.to_vec()])
            } else {
                buf.extend_from_slice(data);
                Ok(vec![buf])
            }
        } else {
            Ok(vec![buf])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tremor_value::literal;
    #[test]
    fn name() {
        let post = Postprocessor::default();
        assert_eq!(post.name(), "collect");
    }
    #[test]
    fn collect_all() -> anyhow::Result<()> {
        let config = Some(literal!({}));
        let mut separate = Postprocessor::from_config(&config)?;
        assert_eq!(separate.process(0, 0, b"").ok(), Some(vec![]));
        assert_eq!(separate.process(0, 0, b"snot").ok(), Some(vec![]));
        assert_eq!(separate.process(0, 0, b"badger").ok(), Some(vec![]));
        assert_eq!(
            separate.finish(Some(b"wobble")).ok(),
            Some(vec![b"snotbadgerwobble".to_vec()])
        );

        Ok(())
    }
    #[test]
    fn no_final() -> anyhow::Result<()> {
        let config = Some(literal!({}));
        let mut separate = Postprocessor::from_config(&config)?;
        assert_eq!(separate.process(0, 0, b"").ok(), Some(vec![]));
        assert_eq!(separate.process(0, 0, b"snot").ok(), Some(vec![]));
        assert_eq!(separate.process(0, 0, b"badger").ok(), Some(vec![]));
        assert_eq!(
            separate.finish(None).ok(),
            Some(vec![b"snotbadger".to_vec()])
        );

        Ok(())
    }
    #[test]
    fn with_max() -> anyhow::Result<()> {
        let config = Some(literal!({"max_bytes": 5}));
        let mut separate = Postprocessor::from_config(&config)?;
        assert_eq!(separate.process(0, 0, b"").ok(), Some(vec![]));
        assert_eq!(separate.process(0, 0, b"snot").ok(), Some(vec![]));
        assert_eq!(
            separate.process(0, 0, b"badger").ok(),
            Some(vec![b"snot".to_vec()])
        );
        assert_eq!(
            separate.finish(Some(b"wobble")).ok(),
            Some(vec![b"badger".to_vec(), b"wobble".to_vec()])
        );

        Ok(())
    }
}
