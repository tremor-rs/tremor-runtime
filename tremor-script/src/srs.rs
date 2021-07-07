// Copyright 2020-2021, The Tremor Team
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

use crate::{ast, SRS};
use std::{mem, pin::Pin, sync::Arc};

///! Thisn file includes our self referential structs

/// A script and it's attached source.
///
/// Implemention alalougous to `EventPayload`
///
/// It is essential to never access the parts of the struct outside of it's
/// implementation! This will void all warenties and likely lead to errors.
///
/// They **must** remain private. All interactions with them have to be guarded
/// by the implementation logic to ensure they remain sane.
///

pub struct Script {
    /// The vector of raw input values
    raw: Vec<Arc<Pin<Vec<u8>>>>,
    structured: ast::Script<'static>,
}

impl std::fmt::Debug for Script {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.structured.fmt(f)
    }
}

unsafe impl SRS for Script {
    type Structured = ast::Script<'static>;

    unsafe fn into_parts(self) -> (Vec<Arc<Pin<Vec<u8>>>>, Self::Structured) {
        (self.raw, self.structured)
    }

    fn raw(&self) -> &[Arc<Pin<Vec<u8>>>] {
        &self.raw
    }

    fn suffix(&self) -> &Self::Structured {
        &self.structured
    }
}

impl Script {
    /// Creates a new Payload with a given byte vector and
    /// a function to turn it into a value and metadata set.
    ///
    /// The return can reference the the data it gets passed
    /// in the function.
    ///
    /// Internally the lifetime will be bound to the raw part
    /// of the struct.
    ///
    /// # Errors
    /// errors if the conversion function fails
    pub fn try_new<E, F>(mut raw: String, f: F) -> std::result::Result<Self, E>
    where
        F: for<'head> FnOnce(&'head mut String) -> std::result::Result<ast::Script<'head>, E>,
    {
        let structured = f(&mut raw)?;
        // This is where the magic happens
        // ALLOW: this is sound since we implement a self referential struct
        let structured: ast::Script<'static> = unsafe { mem::transmute(structured) };
        // This is possibl as String::into_bytes just returns the `vec` of the string
        let raw = Pin::new(raw.into_bytes());
        let raw = vec![Arc::new(raw)];
        Ok(Self { raw, structured })
    }
}

/*
====================================

*/

/// A query and it's attached source.
///
/// Implemention alalougous to `EventPayload`
///
/// It is essential to never access the parts of the struct outside of it's
/// implementation! This will void all warenties and likely lead to errors.
///
/// They **must** remain private. All interactions with them have to be guarded
/// by the implementation logic to ensure they remain sane.
///

#[derive(Clone)]
pub struct Query {
    /// The vector of raw input values
    raw: Vec<Arc<Pin<Vec<u8>>>>,
    structured: ast::Query<'static>,
}

impl std::fmt::Debug for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.structured.fmt(f)
    }
}

unsafe impl SRS for Query {
    type Structured = ast::Query<'static>;

    unsafe fn into_parts(self) -> (Vec<Arc<Pin<Vec<u8>>>>, Self::Structured) {
        (self.raw, self.structured)
    }

    fn raw(&self) -> &[Arc<Pin<Vec<u8>>>] {
        &self.raw
    }

    fn suffix(&self) -> &Self::Structured {
        &self.structured
    }
}

impl Query {
    /// Creates a new Query with a given String and
    /// a function to turn it into a value and metadata set.
    ///
    /// The return can reference the the data it gets passed
    /// in the function.
    ///
    /// Internally the lifetime will be bound to the raw part
    /// of the struct.
    ///
    /// # Errors
    /// errors if the conversion function fails
    pub fn try_new<E, F>(mut raw: String, f: F) -> std::result::Result<Self, E>
    where
        F: for<'head> FnOnce(&'head mut String) -> std::result::Result<ast::Query<'head>, E>,
    {
        let structured = f(&mut raw)?;
        // This is where the magic happens
        // ALLOW: this is sound since we implement a self referential struct
        let structured: ast::Query<'static> = unsafe { mem::transmute(structured) };
        // This is possibl as String::into_bytes just returns the `vec` of the string
        let raw = Pin::new(raw.into_bytes());
        let raw = vec![Arc::new(raw)];
        Ok(Self { raw, structured })
    }

    /// Extracts SRS statements
    ///
    /// This clones all statements
    pub fn extract_stmts(&self) -> Vec<Stmt> {
        // This is valid since we clone `raw` into each
        // self referential struct, so we keep the data each
        // SRS points to inside the SRS
        self.structured
            .stmts
            .iter()
            .cloned()
            .map(|structured| Stmt {
                // THIS IS VERY IMPORTANT (a load bearing clone)
                raw: self.raw.clone(),
                structured,
            })
            .collect()
    }
}

/*
====================================
*/

/// A statement and it's attached source.
///
/// Implemention alalougous to `EventPayload`
///
/// It is essential to never access the parts of the struct outside of it's
/// implementation! This will void all warenties and likely lead to errors.
///
/// They **must** remain private. All interactions with them have to be guarded
/// by the implementation logic to ensure they remain sane.
///
#[derive(Clone)]
pub struct Stmt {
    /// The vector of raw input values
    raw: Vec<Arc<Pin<Vec<u8>>>>,
    structured: ast::Stmt<'static>,
}

unsafe impl SRS for Stmt {
    type Structured = ast::Stmt<'static>;

    unsafe fn into_parts(self) -> (Vec<Arc<Pin<Vec<u8>>>>, Self::Structured) {
        (self.raw, self.structured)
    }

    fn raw(&self) -> &[Arc<Pin<Vec<u8>>>] {
        &self.raw
    }

    fn suffix(&self) -> &Self::Structured {
        &self.structured
    }
}

impl std::fmt::Debug for Stmt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.structured.fmt(f)
    }
}

impl PartialEq for Stmt {
    fn eq(&self, other: &Self) -> bool {
        self.structured == other.structured
    }
}

impl PartialOrd for Stmt {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None // NOTE Here be dragons
    }
}

impl Eq for Stmt {}

impl Stmt {
    /// Creates a new statement from another SRS
    pub fn try_new_from_srs<E, F, O: SRS>(other: &O, f: F) -> std::result::Result<Self, E>
    where
        F: for<'head> FnOnce(&'head O::Structured) -> std::result::Result<ast::Stmt<'head>, E>,
    {
        let raw = other.raw().to_vec();
        let structured = f(other.suffix())?;
        // This is where the magic happens
        // ALLOW: this is sound since we implement a self referential struct
        let structured: ast::Stmt<'static> = unsafe { mem::transmute(structured) };

        Ok(Self { raw, structured })
    }
}
