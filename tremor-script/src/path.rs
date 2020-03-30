// Copyright 2018-2020, Wayfair GmbH
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

use serde::Deserialize;

/// Structure representing module library paths
#[derive(Clone, Debug, Deserialize)]
pub struct ModulePath {
    /// A set of mount points on the module library path
    pub mounts: Vec<String>,
}

impl ModulePath {
    /// Does a particular module exist relative to the module path in force
    pub fn resolve(&self, rel_file: &str) -> Option<String> {
        for mount in &self.mounts {
            let target = format!("{}/{}", &mount, &rel_file);
            if let Ok(meta) = std::fs::metadata(&target) {
                if meta.is_file() {
                    return Some(target); // NOTE The first match on the path is returned so overriding is neither possible nor supported
                }
            }
        }
        None
    }

    /// Convert a relative file path to a module reference
    pub fn to_module(rel_file: &str) -> String {
        rel_file
            .to_string()
            .replace(".tremor$", "")
            .replace("/", "::")
    }
}

/// Load module path
pub fn load_module_path() -> ModulePath {
    load_module_path_(std::env::var("TREMOR_PATH").ok())
}

fn load_module_path_(tremor_path: Option<String>) -> ModulePath {
    if let Some(value) = tremor_path {
        let mounts: Vec<String> = value
            .split(":")
            .filter(|target| {
                if let Ok(meta) = std::fs::metadata(target) {
                    meta.is_dir()
                } else {
                    false // Ignore configured elements that are not directories
                }
            })
            .map(|x| x.replace("//", "/").to_string())
            .collect();
        ModulePath { mounts }
    } else {
        ModulePath { mounts: vec![] }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_module_path() {
        let empty: Vec<String> = vec![];
        assert_eq!(empty, load_module_path_(None).mounts)
    }

    #[test]
    fn test_module_path_env_override() {
        use std::path::PathBuf;
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("tests/modules");
        let tremor_path = format!("{}", d.display());
        let empty: Vec<String> = vec![];

        let mp = load_module_path_(Some(tremor_path));
        assert_ne!(empty, mp.mounts);
        assert_eq!(1, mp.mounts.len());
        assert_eq!(format!("{}", d.display()).to_string(), mp.mounts[0]);

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

        let mp = load_module_path_(Some(tremor_path));
        assert_ne!(empty, mp.mounts);
        assert_eq!(1, mp.mounts.len());
        assert_eq!(format!("{}", d.display()).to_string(), mp.mounts[0]);
    }
}
