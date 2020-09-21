// Modifications Copyright 2020, The Tremor Team
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
    pub line: usize,
    /// The Column
    pub column: usize,
    /// Absolute location in bytes starting from 0
    pub absolute: usize,
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

/// A Span with start and end location
#[derive(Copy, Clone, Default, Eq, PartialEq, Debug, Hash, Ord, PartialOrd)]
pub struct Span {
    /// The starting location
    pub start: Location,
    /// The end location
    pub end: Location,
}

impl Span {
    pub(crate) fn new(start: Location, end: Location) -> Self {
        Self { start, end }
    }
    pub(crate) fn start(&self) -> Location {
        self.start
    }
    pub(crate) fn end(&self) -> Location {
        self.end
    }
}

pub(crate) fn span(start: Location, end: Location) -> Span {
    Span::new(start, end)
}

/// A Spanned element, position plus element
#[derive(Copy, Clone, Debug, Eq, PartialEq, Default)]
pub struct Spanned<T> {
    /// The span
    pub span: Span,
    /// The token
    pub value: T,
}
/// A rage in a file between two locations
#[derive(
    Copy, Clone, Default, Eq, PartialEq, Debug, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
pub struct Range(pub(crate) Location, pub(crate) Location);
impl Range {
    pub(crate) fn expand_lines(&self, lines: usize) -> Self {
        let mut new = *self;
        new.0 = new.0.move_up_lines(lines);
        new.1 = new.1.move_down_lines(lines);
        new
    }
    /// The compilation unit associated with this range
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
    pub fn new(line: usize, column: usize, absolute: usize) -> Self {
        Self {
            line,
            column,
            absolute,
            unit_id: 0,
        }
    }

    pub(crate) fn set_cu(&mut self, cu: usize) {
        self.unit_id = cu;
    }

    /// Location for line directives
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
        self.absolute += 1;
    }
}
