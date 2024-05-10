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

//! Separates a continuous stream of data based on length prefixing. The length for each package in a stream is based on the first 64 bit decoded as an unsigned big endian integer.
use super::prelude::*;
use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, BytesMut};

#[derive(Clone, Default, Debug)]
pub(crate) struct LengthPrefixed {
    len: Option<usize>,
    buffer: BytesMut,
}
impl Preprocessor for LengthPrefixed {
    fn name(&self) -> &str {
        "length-prefixed"
    }

    #[allow(clippy::cast_possible_truncation)]
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
            if self.buffer.len() > 8 {
                self.len = Some(BigEndian::read_u64(&self.buffer) as usize);
                self.buffer.advance(8);
            } else {
                break;
            }
        }
        Ok(res)
    }
}

#[cfg(test)]
mod test {
    use tremor_common::alias::Connector;

    use crate::{
        postprocessor::{self, Postprocessor},
        preprocessor::{finish, preprocess},
    };

    #[test]
    fn name() {
        let pre = super::LengthPrefixed::default();
        assert_eq!(pre.name(), "length-prefixed");
    }

    use super::*;
    #[test]
    fn length_prefix() -> anyhow::Result<()> {
        let mut it = 0;

        let pre_p = LengthPrefixed::default();
        let mut post_p = postprocessor::length_prefixed::LengthPrefixed::default();

        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let wire = post_p.process(0, 0, &data)?;
        let (start, end) = wire[0].split_at(7);
        let alias = Connector::new("test", "test");
        let mut pps: Vec<Box<dyn Preprocessor>> = vec![Box::new(pre_p)];
        let recv = preprocess(
            pps.as_mut_slice(),
            &mut it,
            start.to_vec(),
            Value::object(),
            &alias,
        )?;
        assert!(recv.is_empty());
        let recv = preprocess(
            pps.as_mut_slice(),
            &mut it,
            end.to_vec(),
            Value::object(),
            &alias,
        )?;
        assert_eq!(recv[0].0, data);

        // incomplete data
        let processed = preprocess(
            pps.as_mut_slice(),
            &mut it,
            start.to_vec(),
            Value::object(),
            &alias,
        )?;
        assert!(processed.is_empty());
        // not emitted upon finish
        let finished = finish(pps.as_mut_slice(), &alias)?;
        assert!(finished.is_empty());

        Ok(())
    }
}
