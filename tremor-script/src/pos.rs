// Modifications Copyright 2018-2020, Wayfair GmbH
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

//use crate::ast::{BaseExpr, Expr};
pub use codespan::{
    ByteIndex as BytePos, ByteOffset, ColumnIndex as Column, ColumnOffset, LineIndex as Line,
    LineOffset,
};
// A location in a source file
#[derive(
    Copy, Clone, Default, Eq, PartialEq, Debug, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
pub struct Location {
    pub line: usize,
    pub column: usize,
    pub absolute: usize,
}

#[derive(Copy, Clone, Default, Eq, PartialEq, Debug, Hash, Ord, PartialOrd)]
pub struct Span {
    pub start: Location,
    pub end: Location,
}

impl Span {
    pub fn new(start: Location, end: Location) -> Self {
        Self { start, end }
    }
    pub fn start(&self) -> Location {
        self.start
    }
    pub fn end(&self) -> Location {
        self.end
    }
}

pub fn span(start: Location, end: Location) -> Span {
    Span::new(start, end)
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Default)]
pub struct Spanned<T> {
    pub span: Span,
    pub value: T,
}
#[derive(
    Copy, Clone, Default, Eq, PartialEq, Debug, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
pub struct Range(pub Location, pub Location);
impl Range {
    pub fn expand_lines(&self, lines: usize) -> Self {
        let mut new = *self;
        new.0 = new.0.move_up_lines(lines);
        new.1 = new.1.move_down_lines(lines);
        new
    }
}

/*
impl<'script> From<Expr<'script>> for Range {
    fn from(expr: Expr<'script>) -> Self {
        expr.extent(&FIXME)
    }
}
*/

impl From<(Location, Location)> for Range {
    fn from(locs: (Location, Location)) -> Self {
        Self(locs.0, locs.1)
    }
}
pub fn spanned2<T>(start: Location, end: Location, value: T) -> Spanned<T> {
    Spanned {
        span: span(start, end),
        value,
    }
}

impl Location {
    pub fn new(line: usize, column: usize, absolute: usize) -> Self {
        Self {
            line,
            column,
            absolute,
        }
    }

    pub fn move_down_lines(&self, lines: usize) -> Self {
        let mut new = *self;
        new.line += lines;
        new
    }

    pub fn move_up_lines(&self, lines: usize) -> Self {
        let mut new = *self;
        new.line = self.line.saturating_sub(lines);
        new
    }

    pub fn shift(&mut self, ch: char) {
        if ch == '\n' {
            self.line += 1;
            self.column = 1;
        } else {
            self.column += 1;
        }
        self.absolute += 1;
    }
}
