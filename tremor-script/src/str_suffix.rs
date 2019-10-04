// Modifications Copyright 2018-2019, Wayfair GmbH
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

// Original source origin and copyright with original authors:
// Gluon Project - https://github.com/gluon-lang/gluon/blob/master/parser/src/str_suffix.rs

#![allow(unused)]

use std::{mem, str};

/// Str-like type where the first 0-2 bytes may point into a UTF-8 characters but all bytes
/// following those are guaranteed to represent a valid UTF-8 string (`str`). Relying on this
/// property we can iterate over the `StrSuffix` byte-by-byte as we would on a `[u8]` without
/// needing an expensive validation when going back to a `str` as checking any part of a
/// `StrSuffix` for UTF-8-ness only requires a char boundary check (same as a slicing a `str`).
#[repr(transparent)]
pub struct StrSuffix([u8]);

impl StrSuffix {
    pub fn new(s: &str) -> &Self {
        unsafe { mem::transmute(s.as_bytes()) }
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn first(&self) -> Option<u8> {
        self.0.first().cloned()
    }

    pub fn split_first(&self) -> Option<(u8, &Self)> {
        if self.is_empty() {
            None
        } else {
            Some((self.0[0], self.suffix(1)))
        }
    }

    pub fn try_as_str(&self) -> Option<&str> {
        self.get(0)
    }

    fn get(&self, index: usize) -> Option<&str> {
        if self.is_char_boundary(index) {
            Some(unsafe { str::from_utf8_unchecked(&self.0) })
        } else {
            None
        }
    }

    #[inline(always)]
    fn is_char_boundary_byte(b: u8) -> bool {
        // This is bit magic equivalent to: b < 128 || b >= 192
        (b as i8) >= -0x40
    }

    fn is_char_boundary(&self, index: usize) -> bool {
        // From std::str::is_char_boundary
        if index == 0 || index == self.len() {
            return true;
        }
        match self.as_bytes().get(index) {
            None => false,
            Some(&b) => Self::is_char_boundary_byte(b),
        }
    }

    fn bytes_prefix(&self) -> &[u8] {
        for i in 0..(self.len().min(3)) {
            if Self::is_char_boundary_byte(self.0[i]) {
                return &self.0[..i];
            }
        }
        &self.0[..0]
    }

    fn suffix(&self, index: usize) -> &Self {
        // Any suffix of a StrSuffix is a valid StrSuffix
        unsafe { mem::transmute(&self.0[index..]) }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn iter(&self) -> Iter {
        Iter(self)
    }
}

pub struct Iter<'a>(&'a StrSuffix);

impl<'a> Iterator for Iter<'a> {
    type Item = u8;

    fn next(&mut self) -> Option<u8> {
        if let Some((b, rest)) = self.0.split_first() {
            self.0 = rest;
            Some(b)
        } else {
            None
        }
    }
}

impl<'a> Iter<'a> {
    pub fn as_str_suffix(&self) -> &'a StrSuffix {
        self.0
    }
}
