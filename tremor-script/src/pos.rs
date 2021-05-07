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

pub use codespan::{
    ByteIndex as BytePos, ByteOffset, ColumnIndex as Column, ColumnOffset, LineIndex as Line,
    LineOffset,
};
/// A location in a source file
#[derive(
    Copy, Clone, Default, Eq, PartialEq, Debug, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
pub struct Location {
    /// The compilation unit id
    pub unit_id: usize, // mapping of id -> file ( str )
    /// The Line
    line: usize,
    /// The Column
    column: usize,
    /// Absolute location in bytes starting from 0
    absolute: usize,
}

impl std::ops::Sub for Location {
    type Output = Location;
    fn sub(self, rhs: Location) -> Self::Output {
        Location {
            unit_id: self.unit_id,
            line: self.line.saturating_sub(rhs.line),
            column: self.column.saturating_sub(rhs.column),
            absolute: self.absolute.saturating_sub(rhs.absolute),
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

/// A Span with start and end location
#[derive(Copy, Clone, Default, Eq, PartialEq, Debug, Hash, Ord, PartialOrd)]
pub struct Span {
    /// The starting location
    pub start: Location,
    /// The end location
    pub end: Location,
    /// start location without pp adjustment
    pub pp_start: Location,
    /// end location without pp adjustment
    pub pp_end: Location,
}

impl Span {
    pub(crate) fn new(
        start: Location,
        end: Location,
        pp_start: Location,
        pp_end: Location,
    ) -> Self {
        Self {
            start,
            end,
            pp_start,
            pp_end,
        }
    }
    pub(crate) fn start(&self) -> Location {
        self.start
    }
    pub(crate) fn end(&self) -> Location {
        self.end
    }
}

pub(crate) fn span(start: Location, end: Location, pp_start: Location, pp_end: Location) -> Span {
    Span::new(start, end, pp_start, pp_end)
}

/// A Spanned element, position plus element
#[derive(Copy, Clone, Debug, Eq, PartialEq, Default)]
pub struct Spanned<T> {
    /// The span
    pub span: Span,
    /// The token
    pub value: T,
}

/// A range in a file between two locations
#[derive(
    Copy, Clone, Default, Eq, PartialEq, Debug, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
pub struct Range(pub Location, pub Location);
impl Range {
    pub(crate) fn expand_lines(&self, lines: usize) -> Self {
        let mut new = *self;
        new.0 = new.0.move_up_lines(lines);
        new.1 = new.1.move_down_lines(lines);
        new
    }
    /// The compilation unit associated with this range
    #[must_use]
    pub fn cu(self) -> usize {
        self.0.unit_id
    }
}

impl From<(Location, Location)> for Range {
    fn from(locs: (Location, Location)) -> Self {
        Self(locs.0, locs.1)
    }
}

impl Location {
    /// Creates a new location
    #[must_use]
    pub fn new(line: usize, column: usize, absolute: usize, unit_id: usize) -> Self {
        Self {
            unit_id,
            line,
            column,
            absolute,
        }
    }

    /// resets the column to zero
    #[must_use]
    pub fn start_of_line(&self) -> Self {
        let mut new = *self;
        new.column = 0;
        new
    }
    /// absolute position in the source as bytes
    #[must_use]
    pub fn absolute(&self) -> usize {
        self.absolute
    }

    /// line of the location
    #[must_use]
    pub fn line(&self) -> usize {
        self.line
    }

    /// column  (character not byte) in the current line
    #[must_use]
    pub fn column(&self) -> usize {
        self.column
    }

    pub(crate) fn set_cu(&mut self, cu: usize) {
        self.unit_id = cu;
    }

    /// Location for line directives
    #[must_use]
    pub fn for_line_directive() -> Self {
        Self {
            line: 0,
            column: 0,
            absolute: 0,
            unit_id: 0,
        }
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
