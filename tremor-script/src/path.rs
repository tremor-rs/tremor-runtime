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

use crate::ast::NodeId;
use serde::Deserialize;
use std::path::{Path, PathBuf};

/// default `TREMOR_PATH`
///
/// * `/usr/share/tremor/lib` - in packages this directory contains the stdlib
/// * `/usr/local/share/tremor`         - place for custom user libraries and modules, takes precedence over stdlib
const DEFAULT: &str = "/usr/local/share/tremor:/usr/share/tremor/lib";
/// Structure representing module library paths
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize)]
pub struct ModulePath {
    /// A set of mount points on the module library path
    pub mounts: Vec<String>,
}

impl Default for ModulePath {
    fn default() -> Self {
        ModulePath::load()
    }
}

impl ModulePath {
    /// removes all module path
    pub fn clear(&mut self) {
        self.mounts.clear();
    }

    /// Adds to the module path
    pub fn add<S: ToString>(&mut self, path: &S) {
        let p = path.to_string();
        if !self.mounts.contains(&p) {
            self.mounts.push(p);
        }
    }

    /// Does a particular module exist relative to the module path in force
    #[must_use]
    pub fn resolve_id(&self, id: &NodeId) -> Option<PathBuf> {
        let mut p = PathBuf::new();
        for e in &id.module {
            p.push(e);
        }

        let mut p_tremor = p.clone();
        p_tremor.push(format!("{}.tremor", id.id));
        self.resolve(p_tremor)
            .or_else(|| {
                let mut p_trickle = p.clone();
                p_trickle.push(format!("{}.trickle", id.id));
                self.resolve(p_trickle)
            })
            .or_else(|| {
                let mut p_troy = p.clone();
                p_troy.push(format!("{}.troy", id.id));
                self.resolve(p_troy)
            })
    }

    /// Does a particular module exist relative to the module path in force
    pub fn resolve<S: AsRef<Path>>(&self, rel_file: S) -> Option<PathBuf> {
        for mount in &self.mounts {
            let mut target = PathBuf::new();
            target.push(mount);
            target.push(&rel_file);
            if let Ok(meta) = std::fs::metadata(&target) {
                if meta.is_file() {
                    return Some(target); // NOTE The first match on the path is returned so overriding is neither possible nor supported
                }
            }
        }
        None
    }

    /// Load module path
    #[must_use]
    pub fn load() -> Self {
        load_(&std::env::var("TREMOR_PATH").unwrap_or_else(|_| String::from(DEFAULT)))
    }
}

/// Load module path
#[must_use]
pub fn load() -> ModulePath {
    ModulePath::load()
}

fn load_(tremor_path: &str) -> ModulePath {
    let mounts: Vec<String> = tremor_path
        .split(':')
        .filter_map(|target| {
            let target = target.trim();
            if let Ok(meta) = std::fs::metadata(target) {
                let path = Path::new(&target);
                let path = path.to_path_buf();
                let target = if path.is_relative() {
                    let path = tremor_common::file::canonicalize(&path);
                    if let Ok(path) = path {
                        let path_str = path.to_string_lossy().to_string();
                        path_str
                    } else {
                        target.to_string()
                    }
                } else {
                    target.to_string()
                };
                if meta.is_dir() {
                    Some(target.replace("//", "/"))
                } else {
                    None
                }
            } else {
                None // Ignore configured elements that are not directories
            }
        })
        .filter(|s| !s.is_empty())
        .collect();
    ModulePath { mounts }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::errors::{Error, Result};
    #[test]
    fn test_default_module_path() {
        let empty: Vec<String> = vec![];
        assert_eq!(empty, load_("").mounts);
    }

    #[test]
    fn test_add() {
        let mounts = vec![];
        let mut paths = ModulePath { mounts };
        paths.add(&"/foo/bar/baz");
        paths.add(&"snot/badger");
        assert_eq!(
            vec![String::from("/foo/bar/baz"), String::from("snot/badger")],
            paths.mounts
        );
    }

    #[test]
    fn test_load_relative() -> Result<()> {
        let d = std::env::current_dir()?;
        let dir = std::fs::read_dir(&d)?;
        let mut path = None;
        for entry in dir {
            let entry = entry?;
            if entry.path().is_dir() {
                path = Some(entry.path());
                break;
            }
        }
        let tremor_path = match path.ok_or("bad path")?.strip_prefix(d.clone()) {
            Ok(p) => format!("{}", p.display()),
            Err(e) => return Err(Error::from(e.to_string())),
        };

        let mp = load_(&tremor_path);
        let expected = format!("{}", d.join(tremor_path).display());
        assert_eq!(vec![expected], mp.mounts);
        Ok(())
    }

    #[test]
    fn test_module_path_env_override() {
        use std::path::PathBuf;
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("tests/modules");
        let tremor_path = format!("{}", d.display());
        let empty: Vec<String> = vec![];

        let mp = load_(&tremor_path);
        assert_ne!(empty, mp.mounts);
        assert_eq!(1, mp.mounts.len());
        assert_eq!(format!("{}", d.display()), mp.mounts[0]);

        assert!(mp.resolve("there.tremor").is_some());
        assert!(mp.resolve("not_there.tremor").is_none());

        assert!(mp.resolve("nest/there.tremor").is_some());
        assert!(mp.resolve("nest/not_there.tremor").is_none());

        assert!(mp.resolve("nest/nest/there.tremor").is_some());
        assert!(mp.resolve("nest/nest/not_there.tremor").is_none());
    }

    #[test]
    fn test_module_path_env_override_bad_segments() {
        use std::path::PathBuf;
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("tests/modules");
        let tremor_path = format!("{}:snot:badger:/horse", d.display());
        let empty: Vec<String> = vec![];

        let mp = load_(&tremor_path);
        assert_ne!(empty, mp.mounts);
        assert_eq!(1, mp.mounts.len());
        assert_eq!(format!("{}", d.display()), mp.mounts[0]);
    }
}
