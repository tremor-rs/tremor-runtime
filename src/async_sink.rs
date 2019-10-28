// Copyright 2018-2019, Wayfair GmbH
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

use crate::errors::*;
use crossbeam_channel::Receiver;
use std::collections::VecDeque;
use std::error;
use std::fmt;
use std::result;

#[derive(Debug)]
pub struct AsyncSink<T> {
    queue: VecDeque<Receiver<Result<T>>>,
    capacity: usize,
    size: usize,
}

#[derive(Debug, PartialEq)]
pub enum SinkEnqueueError {
    AtCapacity,
}
impl error::Error for SinkEnqueueError {}

#[cfg_attr(tarpaulin, skip)]
impl fmt::Display for SinkEnqueueError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

#[derive(Debug, PartialEq)]
pub enum SinkDequeueError {
    Empty,
    NotReady,
}
impl error::Error for SinkDequeueError {}

#[cfg_attr(tarpaulin, skip)]
impl fmt::Display for SinkDequeueError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

/// A queue of async tasks defined by an receiver that returns once the task
/// completes.
impl<T> AsyncSink<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: VecDeque::with_capacity(capacity),
            capacity,
            size: 0,
        }
    }
    pub fn enqueue(&mut self, value: Receiver<Result<T>>) -> result::Result<(), SinkEnqueueError> {
        if self.size >= self.capacity {
            Err(SinkEnqueueError::AtCapacity)
        } else {
            self.size += 1;
            self.queue.push_back(value);
            Ok(())
        }
    }
    pub fn dequeue(&mut self) -> result::Result<Result<T>, SinkDequeueError> {
        match self.queue.pop_front() {
            None => Err(SinkDequeueError::Empty),
            Some(rx) => match rx.try_recv() {
                Err(_) => {
                    self.queue.push_front(rx);
                    Err(SinkDequeueError::NotReady)
                }
                Ok(result) => {
                    self.size -= 1;
                    Ok(result)
                }
            },
        }
    }
    pub fn empty(&mut self) -> Result<()> {
        while let Some(rx) = self.queue.pop_front() {
            rx.recv()??;
        }
        Ok(())
    }
    pub fn has_capacity(&self) -> bool {
        self.size < self.capacity
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crossbeam_channel::bounded;

    #[test]
    fn dequeue_empty() {
        let mut q: AsyncSink<u8> = AsyncSink::new(2);
        assert_eq!(q.dequeue(), Err(SinkDequeueError::Empty));
    }

    #[test]
    fn full_cycle() {
        let mut q: AsyncSink<u8> = AsyncSink::new(2);
        let (tx1, rx) = bounded(1);
        assert!(q.enqueue(rx).is_ok());
        let (tx2, rx) = bounded(1);
        assert!(q.enqueue(rx).is_ok());
        let (_tx3, rx) = bounded(1);
        assert_eq!(q.enqueue(rx).err(), Some(SinkEnqueueError::AtCapacity));
        assert_eq!(q.dequeue(), Err(SinkDequeueError::NotReady));
        assert!(tx1.send(Ok(1)).is_ok());
        assert_eq!(q.dequeue(), Ok(Ok(1)));
        let (tx4, rx) = bounded(1);
        assert!(q.enqueue(rx).is_ok());
        let (_tx5, rx) = bounded(1);
        assert_eq!(q.enqueue(rx).err(), Some(SinkEnqueueError::AtCapacity));
        assert_eq!(q.dequeue(), Err(SinkDequeueError::NotReady));
        assert!(tx2.send(Ok(2)).is_ok());
        assert_eq!(q.dequeue(), Ok(Ok(2)));
        assert!(tx4.send(Ok(4)).is_ok());
        assert_eq!(q.dequeue(), Ok(Ok(4)));
        assert_eq!(q.dequeue(), Err(SinkDequeueError::Empty));
    }
}
