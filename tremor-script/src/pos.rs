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

// Copyright of original code is with original authors. Source cited below:
// [libsyntax_pos]: https://github.com/rust-lang/rust/blob/master/src/libsyntax_pos/lib.rs

use crate::ast::{BaseExpr, Expr};
use crate::registry::Context;
use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{Serialize, SerializeStruct, Serializer};
use std::fmt;

pub use codespan::{
    ByteIndex as BytePos, ByteOffset, ColumnIndex as Column, ColumnOffset, LineIndex as Line,
    LineOffset, Span,
};

// A location in a source file
#[derive(Copy, Clone, Default, Eq, PartialEq, Debug, Hash, Ord, PartialOrd)]
pub struct Location {
    pub line: Line,
    pub column: Column,
    pub absolute: BytePos,
}

#[derive(
    Copy, Clone, Default, Eq, PartialEq, Debug, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
pub struct Range(pub Location, pub Location);

impl Range {
    pub fn expand_lines(&self, lines: u32) -> Self {
        let mut new = *self;
        new.0 = new.0.move_up_lines(lines);
        new.1 = new.1.move_down_lines(lines);
        new
    }
}

impl<'script, Ctx: Context + Serialize + Clone> From<Expr<'script, Ctx>> for Range {
    fn from(expr: Expr<'script, Ctx>) -> Range {
        expr.extent()
    }
}

impl From<(Location, Location)> for Range {
    fn from(locs: (Location, Location)) -> Range {
        Range(locs.0, locs.1)
    }
}

impl Serialize for Location {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state_struct = serializer.serialize_struct("Location", 3)?;
        state_struct.serialize_field("line", &self.line.to_usize())?;
        state_struct.serialize_field("column", &self.column.to_usize())?;
        state_struct.serialize_field("absolute", &self.absolute.to_usize())?;
        state_struct.end()
    }
}

impl<'de> Deserialize<'de> for Location {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field {
            Line,
            Column,
            Absolute,
        };

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;
                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("`line`, `column` or `absolute`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "line" => Ok(Field::Line),
                            "column" => Ok(Field::Column),
                            "absolute" => Ok(Field::Absolute),
                            _ => Err(de::Error::unknown_field(value, LOCATION_FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct LocationVisitor;

        impl<'de> Visitor<'de> for LocationVisitor {
            type Value = Location;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Duration")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<Location, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let line = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let column = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let absolute = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                Ok(Location::new(line, column, absolute))
            }

            fn visit_map<V>(self, mut map: V) -> Result<Location, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut line = None;
                let mut column = None;
                let mut absolute = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Line => {
                            if line.is_some() {
                                return Err(de::Error::duplicate_field("line"));
                            }
                            line = Some(map.next_value()?);
                        }
                        Field::Column => {
                            if column.is_some() {
                                return Err(de::Error::duplicate_field("column"));
                            }
                            column = Some(map.next_value()?);
                        }
                        Field::Absolute => {
                            if absolute.is_some() {
                                return Err(de::Error::duplicate_field("absolute"));
                            }
                            absolute = Some(map.next_value()?);
                        }
                    }
                }
                let line = line.ok_or_else(|| de::Error::missing_field("line"))?;
                let column = column.ok_or_else(|| de::Error::missing_field("column"))?;
                let absolute = absolute.ok_or_else(|| de::Error::missing_field("absolute"))?;
                Ok(Location::new(line, column, absolute))
            }
        }
        const LOCATION_FIELDS: &[&str] = &["line", "column", "absolute"];
        deserializer.deserialize_struct("Location", LOCATION_FIELDS, LocationVisitor)
    }
}

impl Location {
    pub fn new(line: u32, column: u32, absolute: u32) -> Self {
        Self {
            line: Line(line),
            column: Column(column),
            absolute: BytePos(absolute),
        }
    }

    pub fn move_down_lines(&self, lines: u32) -> Location {
        let mut new = *self;
        new.line.0 += lines;
        new
    }

    pub fn move_up_lines(&self, lines: u32) -> Location {
        let mut new = *self;
        new.line.0 = self.line.0.checked_sub(lines).unwrap_or(0);
        new
    }

    pub fn shift(&mut self, ch: char) {
        if ch == '\n' {
            self.line += LineOffset(1);
            self.column = Column(1);
        } else {
            self.column += ColumnOffset(1);
        }
        self.absolute += ByteOffset(1);
    }
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Line: {}, Column: {}",
            self.line.number(),
            self.column.number()
        )
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Default)]
pub struct Spanned<T, Pos> {
    pub span: Span<Pos>,
    pub value: T,
}

impl<T, Pos> Spanned<T, Pos> {
    pub fn map<U, F>(self, mut f: F) -> Spanned<U, Pos>
    where
        F: FnMut(T) -> U,
    {
        Spanned {
            span: self.span,
            value: f(self.value),
        }
    }
}

impl<T: fmt::Display, Pos: fmt::Display + Copy> fmt::Display for Spanned<T, Pos> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: {}", self.span.start(), self.value)
    }
}

pub fn span<Pos>(start: Pos, end: Pos) -> Span<Pos>
where
    Pos: Ord,
{
    Span::new(start, end)
}

pub fn spanned<T, Pos>(span: Span<Pos>, value: T) -> Spanned<T, Pos>
where
    Pos: Ord,
{
    Spanned { span, value }
}

pub fn spanned2<T, Pos>(start: Pos, end: Pos, value: T) -> Spanned<T, Pos>
where
    Pos: Ord,
{
    Spanned {
        span: span(start, end),
        value,
    }
}

pub trait HasSpan {
    fn span(&self) -> Span<BytePos>;
}
