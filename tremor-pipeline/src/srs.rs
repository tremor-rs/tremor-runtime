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

///! Thisn file includes our self referential structs
use std::sync::Arc;
use std::{mem, pin::Pin};
use tremor_script::ast::{ScriptDecl, SelectStmt};
use tremor_script::SRS;

#[derive(Clone)]
pub(crate) struct Script {
    raw: Vec<Arc<Pin<Vec<u8>>>>,
    script: ScriptDecl<'static>,
}

unsafe impl SRS for Script {
    type Structured = ScriptDecl<'static>;

    unsafe fn into_parts(self) -> (Vec<Arc<Pin<Vec<u8>>>>, Self::Structured) {
        (self.raw, self.script)
    }

    fn raw(&self) -> &[Arc<Pin<Vec<u8>>>] {
        &self.raw
    }

    fn suffix(&self) -> &Self::Structured {
        &self.script
    }
}

impl std::fmt::Debug for Script {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.script.fmt(f)
    }
}

impl Script {
    pub(crate) fn try_new_from_srs<E, F, O: SRS>(other: &O, f: F) -> std::result::Result<Self, E>
    where
        F: for<'head> FnOnce(&'head O::Structured) -> std::result::Result<ScriptDecl<'head>, E>,
    {
        let raw = other.raw().to_vec();
        let script = f(other.suffix())?;
        // This is where the magic happens
        // ALLOW: this is sound since we implement a self referential struct
        let script: ScriptDecl<'static> = unsafe { mem::transmute(script) };

        Ok(Self { raw, script })
    }

    pub(crate) fn apply<R, F, Other: SRS>(&mut self, other: &Other, join_f: F) -> R
    where
        R: ,
        F: Fn(&mut ScriptDecl<'static>, &Other::Structured) -> R,
    {
        // We append first in the case that some data already moved into self.structured by the time
        // that the join_f fails
        self.raw.extend_from_slice(other.raw());
        join_f(&mut self.script, other.suffix())
    }
}

/*
=========================================================================
*/

#[derive(Clone)]
pub(crate) struct Select {
    raw: Vec<Arc<Pin<Vec<u8>>>>,
    select: SelectStmt<'static>,
}

unsafe impl SRS for Select {
    type Structured = SelectStmt<'static>;

    unsafe fn into_parts(self) -> (Vec<Arc<Pin<Vec<u8>>>>, Self::Structured) {
        (self.raw, self.select)
    }

    fn raw(&self) -> &[Arc<Pin<Vec<u8>>>] {
        &self.raw
    }

    fn suffix(&self) -> &Self::Structured {
        &self.select
    }
}

impl std::fmt::Debug for Select {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.select.fmt(f)
    }
}

impl Select {
    /// Named after the original rental struct for easy rewriting.
    ///
    /// Borrows the borrowed (liftimed) part of the self referential struct
    /// and calls the provided function with a reference to it
    pub fn rent<F, R>(&self, f: F) -> R
    where
        F: for<'iref, 'head> FnOnce(&'iref SelectStmt<'head>) -> R,
        R: ,
    {
        f(&self.select)
    }

    /// Named after the original rental struct for easy rewriting.
    ///
    /// Borrows the borrowed (liftimed) part of the self referential struct
    /// mutably and calls the provided function with a mutatable reference to it
    pub fn rent_mut<F, R>(&mut self, f: F) -> R
    where
        F: for<'iref, 'head> FnOnce(&'iref mut SelectStmt<'head>) -> R,
        R: ,
    {
        f(&mut self.select)
    }

    pub(crate) fn try_new_from_srs<E, F, O: SRS>(other: &O, f: F) -> std::result::Result<Self, E>
    where
        F: for<'head> FnOnce(&'head O::Structured) -> std::result::Result<SelectStmt<'head>, E>,
    {
        let raw = other.raw().to_vec();
        let script = f(other.suffix())?;
        // This is where the magic happens
        // ALLOW: this is sound since we implement a self referential struct
        let script: SelectStmt<'static> = unsafe { mem::transmute(script) };

        Ok(Self {
            raw,
            select: script,
        })
    }
}
