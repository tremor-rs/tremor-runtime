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

//! Prefixes the data with the length of the event data in bytes as an unsigned 64 bit big-endian integer.

use super::Stateless;
use std::io::Write;

#[derive(Clone, Default)]
pub(crate) struct TextualLengthPrefixed {}
impl Stateless for TextualLengthPrefixed {
    fn name(&self) -> &str {
        "textual-length-prefixed"
    }

    fn process(&self, data: &[u8]) -> anyhow::Result<Vec<Vec<u8>>> {
        let size = data.len();
        let mut digits: Vec<u8> = size.to_string().into_bytes();
        let mut res = Vec::with_capacity(digits.len() + 1 + size);
        res.append(&mut digits);
        res.push(32);
        res.write_all(data)?;
        Ok(vec![res])
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn name() {
        let post = TextualLengthPrefixed {};
        assert_eq!(post.name(), "textual-length-prefixed");
    }
    #[test]
    fn textual_length_prefix_postp() -> anyhow::Result<()> {
        let post = TextualLengthPrefixed {};
        let data = vec![1_u8, 2, 3];
        let encoded = post.process(&data)?.pop().unwrap_or_default();
        assert_eq!("3 \u{1}\u{2}\u{3}", std::str::from_utf8(&encoded)?);
        Ok(())
    }
}
