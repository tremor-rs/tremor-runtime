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
use std::{io, mem, pin::Pin, sync::RwLock};
lazy_static::lazy_static! {
    static ref ARENA: RwLock<Arena> = {
        #[cfg(feature = "arena-delete")]
        eprintln!("[ARENA] The memory Arena is compiled with deletions enabled, this should only ever happen in the tremor-language server!");
        RwLock::new(Arena::default())
    };
}

/// Memory arena for source to get static lifeimtes

#[derive(Debug)]
struct ArenaEntry {
    src: Option<Pin<String>>,
    version: u64,
    rc: u64,
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
pub struct Index {
    idx: usize,
    version: u64,
}

impl std::fmt::Display for Index {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.idx, self.version)
    }
}

impl Index {
    /// invalid index, assuming we never gonna have that many data chunks loaded
    pub const INVALID: Self = Self {
        idx: usize::MAX,
        version: u64::MAX,
    };
}

impl From<usize> for Index {
    fn from(idx: usize) -> Self {
        Index { idx, version: 0 }
    }
}

impl From<u32> for Index {
    fn from(idx: u32) -> Self {
        Index {
            idx: idx as usize,
            version: 0,
        }
    }
}

///
/// Append only arena
impl Arena {
    #[cfg(feature = "arena-delete")]
    unsafe fn delete_index_this_is_really_unsafe_dont_use_it_(&mut self, idx: Index) -> Result<()> {
        if let Some(e) = self.sources.get_mut(idx.idx) {
            if e.version == idx.version && e.rc == 0 {
                e.version += 1;
                e.src = None;
                eprintln!("[ARENA] Freed arena index {idx}");
                Ok(())
            } else if e.version == idx.version && e.rc > 0 {
                e.rc -= 1;
                eprintln!("[ARENA] RC reduction of arena index {idx} to {}", e.rc);
                Ok(())
            } else {
                Err("Invalid version to delete".into())
            }
        } else {
            Err("Index already deleted".into())
        }
    }

    #[allow(clippy::let_and_return)]
    fn insert_<S>(&mut self, src: &S) -> Index
    where
        S: ToString + ?Sized + std::ops::Deref<Target = str>,
    {
        if let Some((idx, e)) = self.sources.iter_mut().enumerate().find(|e| {
            e.1.src
                .as_ref()
                .map(|s| {
                    let s: &str = s;
                    let src: &str = src;
                    s == src
                })
                .unwrap_or_default()
        }) {
            e.rc += 1;
            let idx = Index {
                idx,
                version: e.version,
            };
            #[cfg(feature = "arena-delete")]
            eprintln!("[ARENA] RC re-use arena index {idx}: {}", e.rc);
            idx
        } else if let Some((idx, e)) = self
            .sources
            .iter_mut()
            .enumerate()
            .find(|e| e.1.src.is_none())
        {
            e.src = Some(Pin::new(src.to_string()));
            e.rc = 0;
            let idx = Index {
                idx,
                version: e.version,
            };
            #[cfg(feature = "arena-delete")]
            eprintln!("[ARENA] Reclaimed arena index {idx}");
            idx
        } else {
            let idx = self.sources.len();
            self.sources.push(ArenaEntry {
                src: Some(Pin::new(src.to_string())),
                version: 0,
                rc: 0,
            });
            let idx = Index { idx, version: 0 };
            #[cfg(feature = "arena-delete")]
            eprintln!("[ARENA] Added arena index {idx}");
            idx
        }
    }

    fn get_(&self, id: Index) -> Option<&str> {
        self.sources.get(id.idx).and_then(|e| {
            if e.version == id.version {
                e.src.as_deref()
            } else {
                None
            }
        })
    }

    unsafe fn get_static(&self, id: Index) -> Option<&'static str> {
        // ALLOW: The reason we can do that is because the Arena is additive only, we never remove from it
        self.get_(id).map(|s| mem::transmute::<_, &'static str>(s))
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
        S: ToString + ?Sized + std::ops::Deref<Target = str>,
    {
        let mut a = ARENA.write()?;
        let id = a.insert_(src);
        let s = unsafe { a.get_static(id).ok_or("this can't happen")? };
        Ok((id, s))
    }

    /// # Safety
    /// Removes a idex from the arena, freeing the memory and marking it valid for reause
    /// this function generally should not ever be used. It is a special case for the language
    /// server where we know that we really only parse the script to check for errors and
    /// warnings.
    /// That's also why it's behind a feature falg
    /// # Errors
    /// Errors when it can't lock the arena or the delete is invalid
    /// # Safety
    /// This function is unsafe because it can lead to memory unsafety if the index is used while being deleted
    #[cfg(feature = "arena-delete")]
    pub unsafe fn delete_index_this_is_really_unsafe_dont_use_it(id: Index) -> Result<()> {
        let mut a = ARENA.write()?;
        a.delete_index_this_is_really_unsafe_dont_use_it_(id)
    }
}
