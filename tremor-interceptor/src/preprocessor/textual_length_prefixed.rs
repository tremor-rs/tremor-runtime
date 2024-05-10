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
    ) -> anyhow::Result<Vec<(Vec<u8>, Value<'static>)>> {
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

#[cfg(test)]
mod test {
    #![allow(clippy::ignored_unit_patterns)]
    use crate::postprocessor::{self, Postprocessor};

    use super::*;
    use proptest::prelude::*;

    fn textual_prefix(len: usize) -> String {
        format!("{len} {}", String::from_utf8_lossy(&vec![b'O'; len]))
    }

    // generate multiple chopped length-prefixed strings
    fn multiple_textual_lengths(max_elements: usize) -> BoxedStrategy<(Vec<usize>, Vec<String>)> {
        proptest::collection::vec(".+", 1..max_elements) // generator for Vec<String> of arbitrary strings, maximum length of vector: `max_elements`
            .prop_map(|ss| {
                let s: (Vec<usize>, Vec<String>) = ss
                    .into_iter()
                    .map(|s| (s.len(), format!("{} {s}", s.len()))) // for each string, extract the length, and create a textual length prefix
                    .unzip();
                s
            })
            .prop_map(|tuple| (tuple.0, tuple.1.join(""))) // generator for a tuple of 1. the sizes of the length prefixed strings, 2. the concatenated length prefixed strings as one giant string
            .prop_map(|tuple| {
                // here we chop the big string into up to 4 bits
                let mut chopped = Vec::with_capacity(4);
                let mut giant_string: String = tuple.1.clone();
                while !giant_string.is_empty() && chopped.len() < 4 {
                    // verify we are at a char boundary
                    let mut indices = giant_string.char_indices();
                    let num_chars = giant_string.chars().count();
                    if let Some((index, _)) = indices.nth(num_chars / 2) {
                        let mut splitted = giant_string.split_off(index);
                        std::mem::swap(&mut splitted, &mut giant_string);
                        chopped.push(splitted);
                    } else {
                        break;
                    }
                }
                chopped.push(giant_string);
                (tuple.0, chopped)
            })
            .boxed()
    }

    #[test]
    fn name() {
        let pre = TextualLengthPrefixed::default();
        assert_eq!("textual-length-prefixed", pre.name());
    }

    proptest! {
        #[test]
        fn textual_length_prefix_prop((lengths, datas) in multiple_textual_lengths(5)) {
            let mut pre_p = TextualLengthPrefixed::default();
            let mut in_ns = 0_u64;
            let res: Vec<_> = datas.into_iter().flat_map(|data| {
                pre_p.process(&mut in_ns, data.as_bytes(), Value::object()).unwrap_or_default()
            }).collect();
            assert_eq!(lengths.len(), res.len());
            for (processed, expected_len) in res.iter().zip(lengths) {
                assert_eq!(expected_len, processed.0.len());
            }
        }

        #[test]
        fn textual_length_pre_post(length in 1..100_usize) {
            let data = vec![1_u8; length];
            let mut pre_p = TextualLengthPrefixed::default();
            let mut post_p = postprocessor::textual_length_prefixed::TextualLengthPrefixed::default();
            let encoded = post_p.process(0, 0, &data).unwrap_or_default().pop().unwrap_or_default();
            let mut in_ns = 0_u64;
            let mut res = pre_p.process(&mut in_ns, &encoded, Value::object()).unwrap_or_default();
            assert_eq!(1, res.len());
            let payload = res.pop().unwrap_or_default().0;
            assert_eq!(length, payload.len());
        }
    }

    #[test]
    fn textual_prefix_length_loop() {
        let datas = vec![
                "24 \'?\u{d617e}ѨR\u{202e}\u{f8f7c}\u{ede29}\u{ac784}36 ?{¥?MȺ\r\u{bac41}9\u{5bbbb}\r\u{1c46c}\u{4ba79}¥\u{7f}*?:\u{0}$i",
                "60 %\u{a825a}\u{a4269}\u{39e0c}\u{b3e21}<ì\u{f6c20}ѨÛ`HW\u{9523f}V",
                "\u{3}\u{605fe}%Fq\u{89b5e}\u{93780}Q3",
                "¥?\u{feff}9",
                " \'�2\u{4269b}",
            ];
        let lengths: Vec<usize> = vec![24, 36, 60, 9];
        let mut pre_p = TextualLengthPrefixed::default();
        let mut in_ns = 0_u64;
        let res: Vec<_> = datas
            .into_iter()
            .flat_map(|data| {
                pre_p
                    .process(&mut in_ns, data.as_bytes(), Value::object())
                    .unwrap_or_default()
            })
            .collect();
        assert_eq!(lengths.len(), res.len());
        for (processed, expected_len) in res.iter().zip(lengths) {
            assert_eq!(expected_len, processed.0.len());
        }
    }

    #[test]
    fn textual_length_prefix() {
        let mut pre_p = TextualLengthPrefixed::default();
        let data = textual_prefix(42);
        let mut in_ns = 0_u64;
        let mut res = pre_p
            .process(&mut in_ns, data.as_bytes(), Value::object())
            .unwrap_or_default();
        assert_eq!(1, res.len());
        let payload = res.pop().unwrap_or_default().0;
        assert_eq!(42, payload.len());
    }

    #[test]
    fn empty_textual_prefix() {
        let data = ("").as_bytes();
        let mut pre_p = TextualLengthPrefixed::default();
        let mut post_p = postprocessor::textual_length_prefixed::TextualLengthPrefixed::default();
        let mut in_ns = 0_u64;
        let res = pre_p
            .process(&mut in_ns, data, Value::object())
            .unwrap_or_default();
        assert_eq!(0, res.len());

        let data_empty = vec![];
        let encoded = post_p
            .process(42, 23, &data_empty)
            .unwrap_or_default()
            .pop()
            .unwrap_or_default();
        assert_eq!("0 ", String::from_utf8_lossy(&encoded));
        let mut res2 = pre_p
            .process(&mut in_ns, &encoded, Value::object())
            .unwrap_or_default();
        assert_eq!(1, res2.len());
        let payload = res2.pop().unwrap_or_default().0;
        assert_eq!(0, payload.len());
    }
}
