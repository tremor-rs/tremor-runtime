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

use crate::errors::Result;
use std::{io, pin::Pin, sync::RwLock};
lazy_static::lazy_static! {
    static ref ARENA: RwLock<Arena> = RwLock::new(Arena::default());
}

/// Memory arena for source to get static lifeimtes

#[derive(Debug)]
struct ArenaEntry {
    src: Pin<String>,
}

/// The arena for all our scripts
#[derive(Debug, Default)]
pub struct Arena {
    sources: Vec<ArenaEntry>,
}

/// Index into the Arena
#[derive(
    Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Debug,
)]
pub struct Index(usize);

impl std::fmt::Display for Index {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0)
    }
}

impl Index {
    /// invalid index, assuming we never gonna have that many data chunks loaded
    pub const INVALID: Self = Self(usize::MAX);
}

impl From<usize> for Index {
    fn from(v: usize) -> Self {
        Index(v)
    }
}

impl From<u32> for Index {
    fn from(v: u32) -> Self {
        Index(v as usize)
    }
}

///
/// Append only arena
impl Arena {
    fn insert_<S>(&mut self, src: &S) -> Index
    where
        S: ToString + ?Sized,
    {
        let id = self.sources.len();
        self.sources.push(ArenaEntry {
            src: Pin::new(src.to_string()),
        });
        Index(id)
    }
    fn get_(&self, id: Index) -> Option<&str> {
        self.sources.get(id.0).map(|e| {
            let s: &str = &e.src;
            s
        })
    }

    unsafe fn get_static(&self, id: Index) -> Option<&'static str> {
        self.get_(id)
            // ALLOW: The reason we can do that is because the Arena is additive only, we never remove from it
            .map(|s| unsafe { std::mem::transmute::<&str, &'static str>(s) })
    }

    /// Fetches the source as a static string
    /// # Errors
    /// if the source can't be found
    pub fn get(id: Index) -> Result<Option<&'static str>> {
        Ok(unsafe { ARENA.read()?.get_static(id) })
    }

    /// Same as get but returns an io error
    /// # Errors
    /// if the source can't be found
    pub fn io_get(aid: Index) -> io::Result<&'static str> {
        Arena::get(aid)?.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "source not found"))
    }

    /// Inserts source code
    /// # Errors
    /// really never
    pub fn insert<S>(src: &S) -> Result<(Index, &'static str)>
    where
        S: ToString + ?Sized,
    {
        let mut a = ARENA.write()?;
        let id = a.insert_(src);
        let s = unsafe { a.get_static(id).ok_or("this can't happen")? };
        Ok((id, s))
    }
}
