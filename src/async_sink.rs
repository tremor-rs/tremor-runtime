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
use std::collections::VecDeque;
use std::error;
use std::fmt;
use std::result;
use std::sync::mpsc::Receiver;

#[derive(Debug)]
pub struct AsyncSink<T> {
    queue: VecDeque<Receiver<Result<T>>>,
    capacity: usize,
    size: usize,
}

#[derive(Debug)]
pub enum SinkEnqueueError {
    AtCapacity,
}
impl error::Error for SinkEnqueueError {}
impl fmt::Display for SinkEnqueueError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

#[derive(Debug)]
pub enum SinkDequeueError {
    Empty,
    NotReady,
}
impl error::Error for SinkDequeueError {}
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
    pub fn empty(&mut self) {
        while let Some(rx) = self.queue.pop_front() {
            let _ = rx.recv();
        }
    }
    pub fn has_capacity(&self) -> bool {
        self.size < self.capacity
    }
}
