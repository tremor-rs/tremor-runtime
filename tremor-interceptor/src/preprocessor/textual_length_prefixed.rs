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

//! Extracts the message based on prefixed message length given in ascii digits which is followed by a space as used in [RFC 5425](https://tools.ietf.org/html/rfc5425#section-4.3) for TLS/TCP transport for syslog

use super::prelude::*;
use crate::errors::Result;
use bytes::{Buf, BytesMut};

#[derive(Clone, Default, Debug)]
pub(crate) struct TextualLengthPrefixed {
    len: Option<usize>,
    buffer: BytesMut,
}
impl Preprocessor for TextualLengthPrefixed {
    fn name(&self) -> &str {
        "textual-length-prefixed"
    }

    fn process(
        &mut self,
        _ingest_ns: &mut u64,
        data: &[u8],
        meta: Value<'static>,
    ) -> Result<Vec<(Vec<u8>, Value<'static>)>> {
        self.buffer.extend(data);

        let mut res = Vec::new();
        loop {
            if let Some(l) = self.len {
                if self.buffer.len() >= l {
                    let mut part = self.buffer.split_off(l);
                    std::mem::swap(&mut part, &mut self.buffer);
                    res.push((part.to_vec(), meta.clone()));
                    self.len = None;
                } else {
                    break;
                }
            }

            // find the whitespace
            if let Some((i, _c)) = self
                .buffer
                .iter()
                .enumerate()
                .find(|(_i, c)| c.is_ascii_whitespace())
            {
                let mut buf = self.buffer.split_off(i);
                std::mem::swap(&mut buf, &mut self.buffer);
                // parse the textual length
                let l = std::str::from_utf8(&buf)?;
                self.len = Some(l.parse::<u32>()? as usize);
                self.buffer.advance(1); // advance beyond the whitespace delimiter
            } else {
                // no whitespace found
                break;
            }
        }
        Ok(res)
    }
}
