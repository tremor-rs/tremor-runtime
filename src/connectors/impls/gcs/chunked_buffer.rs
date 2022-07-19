use crate::connectors::prelude::Result;
use crate::errors::ErrorKind;

#[derive(Debug, PartialEq, Eq)]
pub struct BufferPart {
    pub data: Vec<u8>,
    pub start: usize,
    pub length: usize,
}

pub struct ChunkedBuffer {
    data: Vec<u8>,
    block_size: usize,
    buffer_start: usize,
}

impl ChunkedBuffer {
    pub fn new(size: usize) -> Self {
        Self {
            data: Vec::with_capacity(size * 2),
            block_size: size,
            buffer_start: 0,
        }
    }

    pub fn mark_done_until(&mut self, position: usize) -> Result<()> {
        if position < self.buffer_start {
            return Err("Buffer was marked as done at index which is not in memory anymore".into());
        }

        let bytes_to_remove = position - self.buffer_start;
        self.data = Vec::from(self.data.get(bytes_to_remove..).ok_or(
            ErrorKind::GoogleCloudStorageError("Not enough data in the buffer"),
        )?);
        self.buffer_start += bytes_to_remove;

        Ok(())
    }

    pub fn read_current_block(&self) -> Option<BufferPart> {
        self.data.get(..self.block_size).map(|raw_data| BufferPart {
            data: raw_data.to_vec(),
            start: self.start(),
            length: self.len(),
        })
    }

    pub fn write(&mut self, data: &[u8]) {
        self.data.extend_from_slice(data);
    }

    pub fn start(&self) -> usize {
        self.buffer_start
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    // FIXME: Consume self instead?
    pub fn final_block(&self) -> BufferPart {
        BufferPart {
            data: self.data.clone(),
            start: self.start(),
            length: self.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn chunked_buffer_can_add_data() {
        let mut buffer = ChunkedBuffer::new(10);
        buffer.write(&(1..=10).collect::<Vec<u8>>());

        assert_eq!(0, buffer.start());
        assert_eq!(10, buffer.len());
        assert_eq!(
            BufferPart {
                data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                start: 0,
                length: 10
            },
            buffer.read_current_block().unwrap()
        );
    }

    #[test]
    pub fn chunked_buffer_will_not_return_a_block_which_is_not_full() {
        let mut buffer = ChunkedBuffer::new(10);
        buffer.write(&(1..=5).collect::<Vec<u8>>());

        assert!(buffer.read_current_block().is_none());
    }

    #[test]
    pub fn chunked_buffer_marking_as_done_removes_data() {
        let mut buffer = ChunkedBuffer::new(10);
        buffer.write(&(1..=15).collect::<Vec<u8>>());

        buffer.mark_done_until(5).unwrap();

        assert_eq!(
            BufferPart {
                data: (6..=15).collect::<Vec<u8>>(),
                start: 5,
                length: 10
            },
            buffer.read_current_block().unwrap()
        );
    }

    #[test]
    pub fn chunked_buffer_returns_all_the_data_in_the_final_block() {
        let mut buffer = ChunkedBuffer::new(10);
        buffer.write(&(1..=16).collect::<Vec<u8>>());

        buffer.mark_done_until(5).unwrap();
        assert_eq!(
            BufferPart {
                data: (6..=16).collect::<Vec<u8>>(),
                start: 5,
                length: 11
            },
            buffer.final_block()
        );
    }
}
