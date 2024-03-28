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

use crate::errors::{err_gcs, Result};
use crate::utils::object_storage::{BufferPart, ObjectStorageBuffer};

/// This structure is similar to a Vec<u8>, but with some special methods.
/// `write` will add data (any size of data is accepted)
/// `read_current_block` will return `block_size` of data, or None if there's not enough
/// `mark_done_until` will mark the data until the given index as read and advance the internal cursor (and throw away what's unneeded)
/// `final_block` returns all the data that has not been marked as done
#[derive(Debug)]
pub(crate) struct ChunkedBuffer {
    data: Vec<u8>,
    block_size: usize,
    buffer_start: usize,
}

impl ChunkedBuffer {
    pub(crate) fn start(&self) -> usize {
        self.buffer_start
    }
}

impl ObjectStorageBuffer for ChunkedBuffer {
    fn new(size: usize) -> Self {
        Self {
            data: Vec::with_capacity(size * 2),
            block_size: size,
            buffer_start: 0,
        }
    }

    fn write(&mut self, mut data: Vec<u8>) {
        self.data.append(&mut data);
    }

    fn read_current_block(&mut self) -> Option<BufferPart> {
        self.data.get(..self.block_size).map(|raw_data| BufferPart {
            data: raw_data.to_vec(),
            start: self.start(),
        })
    }

    fn mark_done_until(&mut self, position: usize) -> Result<()> {
        if position < self.buffer_start {
            return Err(err_gcs(format!(
                "Buffer was marked as done at index {position} which is not in memory anymore"
            )));
        }

        let bytes_to_remove = position - self.buffer_start;
        self.data = Vec::from(
            self.data
                .get(bytes_to_remove..)
                .ok_or_else(|| err_gcs("Not enough data in the buffer"))?,
        );
        self.buffer_start += bytes_to_remove;

        Ok(())
    }

    fn reset(&mut self) -> BufferPart {
        // extract current state
        let data = self.data.clone(); // only clone up to len, not up to capacity
        let start = self.buffer_start;
        // reset
        self.data.clear();
        self.buffer_start = 0;
        BufferPart { data, start }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn chunked_buffer_can_add_data() {
        let mut buffer = ChunkedBuffer::new(10);
        buffer.write((1..=10).collect::<Vec<u8>>());

        assert_eq!(0, buffer.start());

        assert_eq!(
            Some(BufferPart {
                data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                start: 0,
            }),
            buffer.read_current_block()
        );
    }

    #[test]
    pub fn chunked_buffer_will_not_return_a_block_which_is_not_full() {
        let mut buffer = ChunkedBuffer::new(10);
        buffer.write((1..=5).collect::<Vec<u8>>());

        assert!(buffer.read_current_block().is_none());
    }

    #[test]
    pub fn chunked_buffer_marking_as_done_removes_data() -> Result<()> {
        let mut buffer = ChunkedBuffer::new(10);
        buffer.write((1..=15).collect::<Vec<u8>>());

        buffer.mark_done_until(5)?;

        assert_eq!(
            Some(BufferPart {
                data: (6..=15).collect::<Vec<u8>>(),
                start: 5,
            }),
            buffer.read_current_block()
        );
        Ok(())
    }

    #[test]
    pub fn chunked_buffer_returns_all_the_data_in_the_final_block() -> Result<()> {
        let mut buffer = ChunkedBuffer::new(10);
        buffer.write((1..=16).collect::<Vec<u8>>());

        buffer.mark_done_until(5)?;
        assert_eq!(
            BufferPart {
                data: (6..=16).collect::<Vec<u8>>(),
                start: 5,
            },
            buffer.reset()
        );
        Ok(())
    }
}
