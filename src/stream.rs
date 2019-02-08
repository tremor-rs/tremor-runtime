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

use crossbeam_channel::{bounded, Receiver, Sender};
use std::collections::VecDeque;
use std::io::prelude::*;
use std::io::{self, Error, ErrorKind, SeekFrom};

//
// TODO FIXME
//
// The stream abstraction is currently sufficient for batch/flush in
// offramps but lack of punctuations in pipeline event processing means
// it does not work with receivers that depend on the Seek trait
//

#[derive(Debug, Clone)]
pub struct StreamMultiWriter<T>
where
    T: Send,
{
    tx: Sender<StreamMessage<T>>,
}

#[derive(Debug)]
pub struct StreamWriter<T>
where
    T: Send,
{
    tx: Sender<StreamMessage<T>>,
}

#[derive(Debug)]
pub struct StreamReader<T>
where
    T: Send,
{
    rx: Receiver<StreamMessage<T>>,
    eof: bool,
    buf: VecDeque<T>,
    current_offset: i64,
}

#[derive(Debug, Clone)]
pub struct StreamMultiReader<T>
where
    T: Send,
{
    rx: Receiver<StreamMessage<T>>,
    eof: bool,
    buf: VecDeque<T>,
    current_offset: i64,
}

enum StreamMessage<T>
where
    T: Send,
{
    Data(Vec<T>),
    EOF,
}

impl<T> StreamMultiWriter<T>
where
    T: Send,
{
    pub fn write(&self, data: Vec<T>) -> std::io::Result<()> {
        // FIXME TODO when SendError impls Error trait ( current nightly ) match can be removed
        match self.tx.send(StreamMessage::Data(data)) {
            Ok(()) => Ok(()),
            Err(e) => Err(Error::new(ErrorKind::Other, e.to_string())),
        }
    }

    pub fn close(&self) {
        let _ = self.tx.send(StreamMessage::EOF);
    }
}

impl<T> StreamWriter<T>
where
    T: Send,
{
    pub fn write(&self, data: Vec<T>) -> std::io::Result<()> {
        // FIXME TODO when SendError impls Error trait ( current nightly ) match can be removed
        match self.tx.send(StreamMessage::Data(data)) {
            Ok(()) => Ok(()),
            Err(e) => Err(Error::new(ErrorKind::Other, e.to_string())),
        }
    }

    pub fn close(&self) {
        let _ = self.tx.send(StreamMessage::EOF);
    }
}

impl<T> StreamMultiWriter<T>
where
    T: Clone + Send,
{
    fn stream_write(&mut self, data: &[T]) -> std::io::Result<usize> {
        // FIXME TODO when SendError impls Error trait ( current nightly ) match can be removed
        match self.tx.send(StreamMessage::Data(data.to_vec())) {
            Ok(_) => Ok(data.len()),
            Err(e) => Err(Error::new(ErrorKind::Other, e.to_string())),
        }
    }
}
impl Write for StreamMultiWriter<u8> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.stream_write(data)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // nothing to do
        Ok(())
    }
}

impl<T> StreamWriter<T>
where
    T: Clone + Send,
{
    fn stream_write(&mut self, data: &[T]) -> std::io::Result<usize> {
        // FIXME TODO when SendError impls Error trait ( current nightly ) match can be removed
        match self.tx.send(StreamMessage::Data(data.to_vec())) {
            Ok(_) => Ok(data.len()),
            Err(e) => Err(Error::new(ErrorKind::Other, e.to_string())),
        }
    }
}

impl Write for StreamWriter<u8> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.stream_write(data)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // nothing to do
        Ok(())
    }
}

pub fn mpsc_stream<T>(size: usize, capacity: usize) -> (StreamMultiWriter<T>, StreamReader<T>)
where
    T: Send,
{
    let (tx, rx) = bounded(size);
    let writer = StreamMultiWriter { tx };

    let reader = StreamReader {
        rx,
        eof: false,
        current_offset: 0,
        buf: VecDeque::with_capacity(capacity),
    };
    (writer, reader)
}

pub fn spsc_stream<T>(size: usize, capacity: usize) -> (StreamWriter<T>, StreamReader<T>)
where
    T: Send,
{
    let (tx, rx) = bounded(size);
    let writer = StreamWriter { tx };

    let reader = StreamReader {
        rx,
        eof: false,
        current_offset: 0,
        buf: VecDeque::with_capacity(capacity),
    };
    (writer, reader)
}

pub fn mpmc_stream<T>(size: usize, capacity: usize) -> (StreamMultiWriter<T>, StreamMultiReader<T>)
where
    T: Send,
{
    let (tx, rx) = bounded(size);
    let writer = StreamMultiWriter { tx };

    let reader = StreamMultiReader {
        rx,
        eof: false,
        current_offset: 0,
        buf: VecDeque::with_capacity(capacity),
    };
    (writer, reader)
}

pub fn spmc_stream<T>(size: usize, capacity: usize) -> (StreamWriter<T>, StreamMultiReader<T>)
where
    T: Send,
{
    let (tx, rx) = bounded(size);
    let writer = StreamWriter { tx };

    let reader = StreamMultiReader {
        rx,
        eof: false,
        current_offset: 0,
        buf: VecDeque::with_capacity(capacity),
    };
    (writer, reader)
}

impl<T> Seek for StreamReader<T>
where
    T: Send,
{
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        debug!("seek: {:?}", pos);
        self.current_offset = match pos {
            SeekFrom::Current(x) => self.current_offset + x, //TODO: Skip x
            SeekFrom::Start(x) => x as i64,                  //TODO re-calculate
            SeekFrom::End(x) => {
                let offset = (self.buf.len() as i64 - x) as i64;
                if offset > 0 { offset} else { 0 }
            },
        };
        Ok(self.current_offset as u64)
    }
}

pub trait ReadableStream<T> {
    fn stream_read(&mut self, buf: &mut [T]) -> io::Result<usize>;
}

impl Read for ReadableStream<u8> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream_read(buf)
    }
}

impl<T> ReadableStream<T> for StreamReader<T>
where
    T: Send,
{
    fn stream_read(&mut self, buf: &mut [T]) -> io::Result<usize> {
        debug!("read: {:?}", buf.len());
        if self.eof {
            debug!("read eof");
            return Ok(0);
        };
        for (i, item) in buf.iter_mut().enumerate() {
            match self.buf.pop_front() {
                Some(x) => {
                    self.current_offset += 1;
                    *item = x
                }
                None => match self.rx.recv() {
                    Ok(StreamMessage::EOF) => {
                        self.eof = true;
                        debug!("read eof 1");
                        return Ok(i);
                    }
                    Ok(StreamMessage::Data(data)) => {
                        self.buf.append(&mut VecDeque::from(data));
                        match self.buf.pop_front() {
                            Some(x) => {
                                self.current_offset += 1;
                                *item = x
                            }
                            None => unreachable!(),
                        };
                    }
                    _ => panic!("illegal state"),
                },
            }
        }
        Ok(buf.len())
    }
}

impl Read for StreamReader<u8> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream_read(buf)
    }
}

impl<T> Seek for StreamMultiReader<T>
where
    T: Send,
{
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        debug!("seek: {:?}", pos);
        self.current_offset = match pos {
            SeekFrom::Current(x) => self.current_offset + x, //TODO: Skip x
            SeekFrom::Start(x) => x as i64,                  //TODO re-calculate
            SeekFrom::End(_) => unimplemented!(),
        };
        Ok(self.current_offset as u64)
    }
}

impl<T> StreamMultiReader<T>
where
    T: Send,
{
    fn stream_read(&mut self, buf: &mut [T]) -> io::Result<usize> {
        debug!("read: {:?}", buf.len());
        if self.eof {
            debug!("read eof");
            return Ok(0);
        };
        //TODO: Darach will kill me again
        for (i, item) in buf.iter_mut().enumerate() {
            match self.buf.pop_front() {
                Some(x) => {
                    self.current_offset += 1;
                    *item = x
                }
                None => match self.rx.recv() {
                    Ok(StreamMessage::EOF) => {
                        self.eof = true;
                        debug!("read eof 1");
                        return Ok(i);
                    }
                    Ok(StreamMessage::Data(data)) => {
                        self.buf.append(&mut VecDeque::from(data));
                        match self.buf.pop_front() {
                            Some(x) => {
                                self.current_offset += 1;
                                *item = x
                            }
                            None => unreachable!(),
                        };
                    }
                    _ => panic!("illegal state"),
                },
            }
        }
        Ok(buf.len())
    }
}

impl Read for StreamMultiReader<u8> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream_read(buf)
    }
}

#[cfg(test)]
mod test {
    use super::{mpsc_stream, spsc_stream};
    use std::io::prelude::*;
    use std::str;
    use std::io::SeekFrom;

    #[test]
    fn test_basic_spsc_stream() {
        let (w, mut r) = spsc_stream(10, 10);
        let _ = w.write("hello world".as_bytes().to_vec());
        let mut s = String::new();
        w.close();
        let _ = r.read_to_string(&mut s);
        assert_eq!(s, "hello world");
    }

    #[test]
    fn test_basic_mpsc_stream() {
        let (w0, r) = mpsc_stream(10, 10);
        let w1 = w0.clone();
        let _ = w0.write("beep".as_bytes().to_vec());
        let _ = w1.write("boop".as_bytes().to_vec());
        let mut b = Vec::new();
        b.resize(12, 0);
        w0.close();
        w1.close();
        let _ = r.take(8).read(&mut b);
        assert_eq!(
            str::from_utf8(&b)
                .unwrap()
                .to_string()
                .trim_matches(char::from(0)),
            "beepboop"
        );
    }

    #[test]
    fn test_basic_mpsc_stream_string() {
        let (w0, mut r) = mpsc_stream(10, 10);
        let w1 = w0.clone();
        let _ = w0.write("beep".as_bytes().to_vec());
        let _ = w1.write("boop".as_bytes().to_vec());
        let mut s = String::new();
        w0.close();
        w1.close();
        let _ = r.read_to_string(&mut s);
        assert_eq!(s, "beepboop");
    }

    #[test]
    fn test_basic_spsc_stream_seek() {
        let (w, mut r) = spsc_stream(10, 10);
        let _ = w.write("hello world".as_bytes().to_vec());
        let mut s = String::new();
        w.close();
        // TODO FIXME Should return size of underlying buf
        assert_eq!(0, r.seek(SeekFrom::End(0)).unwrap());
        assert_eq!(0, r.seek(SeekFrom::End(9)).unwrap());
        let _ = r.read_to_string(&mut s);
        assert_eq!(s, "hello world");
    }
}
