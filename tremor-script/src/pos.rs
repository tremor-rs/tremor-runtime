// Modifications Copyright 2020-2021, The Tremor Team
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

// Copyright of original code is with original authors. Source cited below:
// [libsyntax_pos]: https://github.com/rust-lang/rust/blob/master/src/libsyntax_pos/lib.rs

use crate::arena;

use super::lexer::Token;
pub use codespan::{
    ByteIndex as Byte, ByteOffset, ColumnIndex as Column, ColumnOffset, LineIndex as Line,
    LineOffset,
};
/// A location in a source file
#[derive(
    Copy, Clone, Default, Eq, PartialEq, Debug, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
pub struct Location {
    /// The Line
    pub(crate) line: usize,
    /// The Column
    pub(crate) column: usize,
    /// Absolute location in bytes starting from 0
    pub(crate) absolute: usize,
    /// Absolute location in bytes starting from 0
    pub(crate) aid: arena::Index,
}

impl std::ops::Sub for Location {
    type Output = Location;
    fn sub(self, rhs: Location) -> Self::Output {
        debug_assert_eq!(self.aid, rhs.aid);
        Location {
            line: self.line.saturating_sub(rhs.line),
            column: self.column.saturating_sub(rhs.column),
            absolute: self.absolute.saturating_sub(rhs.absolute),
            aid: self.aid,
        }
    }
}

impl std::ops::Add<char> for Location {
    type Output = Location;
    fn add(mut self, c: char) -> Self::Output {
        self.shift(c);
        self
    }
}

pub(crate) fn span(start: Location, end: Location) -> Span {
    Span::new(start, end)
}

/// A Spanned element, position plus element
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Spanned<'tkn> {
    /// The span
    pub span: Span,
    /// The token
    pub value: Token<'tkn>,
}

/// A span in a file between two locations
#[derive(
    Copy, Clone, Default, Eq, PartialEq, Debug, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
pub struct Span {
    pub(crate) start: Location,
    pub(crate) end: Location,
}

impl Span {
    /// New span
    pub fn new(start: Location, end: Location) -> Self {
        Self { start, end }
    }
    pub(crate) fn expand_lines(mut self, lines: usize) -> Self {
        self.start = self.start.move_up_lines(lines);
        self.end = self.end.move_down_lines(lines);
        self
    }
    pub(crate) fn aid(self) -> arena::Index {
        self.start.aid
    }
    /// start of the span
    pub fn start(self) -> Location {
        self.start
    }
    /// end of the span
    pub fn end(self) -> Location {
        self.end
    }
}

impl From<(Location, Location)> for Span {
    fn from((start, end): (Location, Location)) -> Self {
        Self { start, end }
    }
}

impl Location {
    /// Creates a new location
    #[must_use]
    pub fn new(line: usize, column: usize, absolute: usize, aid: arena::Index) -> Self {
        Self {
            line,
            column,
            absolute,
            aid,
        }
    }

    /// resets the column to zero
    #[must_use]
    pub fn start_of_line(mut self) -> Self {
        self.column = 0;
        self
    }
    /// absolute position in the source as bytes
    #[must_use]
    pub fn absolute(self) -> usize {
        self.absolute
    }

    /// line of the location
    #[must_use]
    pub fn line(self) -> usize {
        self.line
    }

    /// column  (character not byte) in the current line
    #[must_use]
    pub fn column(self) -> usize {
        self.column
    }

    pub(crate) fn move_down_lines(&self, lines: usize) -> Self {
        let mut new = *self;
        new.line += lines;
        new
    }

    pub(crate) fn move_up_lines(&self, lines: usize) -> Self {
        let mut new = *self;
        new.line = self.line.saturating_sub(lines);
        new
    }

    pub(crate) fn shift(&mut self, ch: char) {
        if ch == '\n' {
            self.line += 1;
            self.column = 1;
        } else {
            self.column += 1;
        }
        self.absolute += ch.len_utf8();
    }

    pub(crate) fn shift_str(&mut self, s: &str) {
        for c in s.chars() {
            self.shift(c);
        }
    }

    pub(crate) fn extend_left(&mut self, ch: char) {
        if self.column != 0 {
            self.column -= 1;
            self.absolute -= ch.len_utf8();
        }
    }
}
