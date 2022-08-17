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

// This file is OS dependant so testing the errors is near impossible to re-create on a reproducible
// basis
// #![cfg_attr(coverage, no_coverage)]

use std::{ffi::OsStr, fs::File, path::Path, path::PathBuf};

use crate::errors::Error;

/// A wrapper around `File::open` that will give a better error (including the filename)
///
/// # Errors
///   * if the file couldn't be opened
pub fn open<S>(path: &S) -> Result<File, Error>
where
    S: AsRef<Path> + ?Sized,
{
    File::open(path).map_err(|e| {
        let p: &Path = path.as_ref();
        Error::FileOpen(e, p.to_string_lossy().to_string())
    })
}

/// A wrapper around `File::create` that will give a better error (including the filename)
///
/// # Errors
///   * if the file couldn't be created
pub fn create<S>(path: &S) -> Result<File, Error>
where
    S: AsRef<Path> + ?Sized,
{
    File::create(path).map_err(|e| {
        let p: &Path = path.as_ref();
        Error::FileCreate(e, p.to_string_lossy().to_string())
    })
}

/// A wrapper around `File::create` that will give a better error (including the filename)
///
/// # Errors
///   * if the file couldn't be created
pub fn canonicalize<S>(path: &S) -> Result<PathBuf, Error>
where
    S: AsRef<Path> + ?Sized,
{
    std::fs::canonicalize(path).map_err(|e| {
        let p: &Path = path.as_ref();
        Error::FileCanonicalize(e, p.to_string_lossy().to_string())
    })
}

/// A wrapper around `File::create` that will give a better error (including the filename)
///
/// # Errors
///   * if the file couldn't be created
pub fn set_current_dir<S>(path: &S) -> Result<(), Error>
where
    S: AsRef<Path>,
{
    std::env::set_current_dir(path).map_err(|e| {
        let p: &Path = path.as_ref();
        Error::Cwd(e, p.to_string_lossy().to_string())
    })
}
/// Gets the extesion for a filename
pub fn extension(path: &str) -> Option<&str> {
    Path::new(path).extension().and_then(OsStr::to_str)
}

#[cfg(test)]
mod test {
    use crate::errors::Error;

    #[test]
    fn set_current_dir() -> Result<(), Error> {
        let path = std::env::current_dir().unwrap();
        super::set_current_dir(&path)?;
        let path = path.join("really.does.not.exist");
        let p = path.to_string_lossy().to_string();
        let r = super::set_current_dir(&path);
        assert!(r.is_err());
        let err = r.err().unwrap();
        assert!(matches!(err, Error::Cwd(_, bad) if bad == p));

        Ok(())
    }

    #[test]
    fn canonicalize() -> Result<(), Error> {
        let path = std::env::current_dir().unwrap();
        let path = path.join("Cargo.toml");
        let path = super::canonicalize(&path)?;
        assert!(path.exists());
        let path = path.join("really.does.not.exist");
        let p = path.to_string_lossy().to_string();
        let path = super::canonicalize(&path);
        assert!(path.is_err());
        let err = path.err().unwrap();
        assert!(matches!(err, Error::FileCanonicalize(_, bad) if bad == p));
        Ok(())
    }

    #[test]
    fn create() -> Result<(), Error> {
        let path = std::env::current_dir().unwrap();
        let path = path.join(".a.file.that.will.get.deleted");
        super::create(&path)?;
        let path = path.join("this.does.not.work");
        let p = path.to_string_lossy().to_string();
        let r = super::create(&path);
        assert!(r.is_err());
        let err = r.err().unwrap();
        assert!(matches!(err, Error::FileCreate(_, bad) if bad == p));
        Ok(())
    }

    #[test]
    fn open() -> Result<(), Error> {
        let path = std::env::current_dir().unwrap();
        let path = path.join("Cargo.toml");
        super::open(&path)?;
        let path = path.join("this.does.not.work");
        let p = path.to_string_lossy().to_string();
        let r = super::open(&path);
        assert!(r.is_err());
        let err = r.err().unwrap();
        assert!(matches!(err, Error::FileOpen(_, bad) if bad == p));
        Ok(())
    }
}
