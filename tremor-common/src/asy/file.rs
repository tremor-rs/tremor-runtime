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

use async_std::{
    fs::{File, OpenOptions},
    path::Path,
    path::PathBuf,
};

use crate::errors::Error;

/// A wrapper around `File::open` that will give a better error (including the filename)
///
/// # Errors
///   * if the file couldn't be opened
pub async fn open<S>(path: &S) -> Result<File, Error>
where
    S: AsRef<Path> + ?Sized,
{
    File::open(path).await.map_err(|e| {
        let p: &Path = path.as_ref();
        Error::FileOpen(e, p.to_string_lossy().to_string())
    })
}

/// A wrapper around `OpenOptions::open` that will give better errors (including the filename)
///
/// # Errors
///   * if the file could not be opened
pub async fn open_with<S>(path: &S, options: &mut OpenOptions) -> Result<File, Error>
where
    S: AsRef<Path> + ?Sized,
{
    options.open(path).await.map_err(|e| {
        let p: &Path = path.as_ref();
        Error::FileOpen(e, p.to_string_lossy().to_string())
    })
}

/// A wrapper around `File::create` that will give a better error (including the filename)
///
/// # Errors
///   * if the file couldn't be created
pub async fn create<S>(path: &S) -> Result<File, Error>
where
    S: AsRef<Path> + ?Sized,
{
    File::create(path).await.map_err(|e| {
        let p: &Path = path.as_ref();
        Error::FileCreate(e, p.to_string_lossy().to_string())
    })
}

/// A wrapper around `fs::canonicalize` that will give a better error (including the filename)
///
/// # Errors
///   * if the path does not exist
pub async fn canonicalize<S>(path: &S) -> Result<PathBuf, Error>
where
    S: AsRef<Path> + ?Sized,
{
    async_std::fs::canonicalize(path).await.map_err(|e| {
        let p: &Path = path.as_ref();
        Error::FileCanonicalize(e, p.to_string_lossy().to_string())
    })
}

pub use crate::file::extension;

#[cfg(test)]
mod test {
    use crate::errors::Error;

    #[async_std::test]
    async fn canonicalize() -> Result<(), Error> {
        let path = std::env::current_dir().unwrap();
        let path = path.join("Cargo.toml");
        let path = super::canonicalize(&path).await?;
        assert!(path.exists().await);
        let path = path.join("really.does.not.exist");
        let p = path.to_string_lossy().to_string();
        let path = super::canonicalize(&path).await;
        assert!(path.is_err());
        let err = path.err().unwrap();
        assert!(matches!(err, Error::FileCanonicalize(_, bad) if bad == p));
        Ok(())
    }

    #[async_std::test]
    async fn create() -> Result<(), Error> {
        let path = std::env::current_dir().unwrap();
        let path = path.join(".a.file.that.will.get.deleted");
        super::create(&path).await?;
        let path = path.join("this.does.not.work");
        let p = path.to_string_lossy().to_string();
        let r = super::create(&path).await;
        assert!(r.is_err());
        let err = r.err().unwrap();
        assert!(matches!(err, Error::FileCreate(_, bad) if bad == p));
        Ok(())
    }

    #[async_std::test]
    async fn open() -> Result<(), Error> {
        let path = std::env::current_dir().unwrap();
        let path = path.join("Cargo.toml");
        super::open(&path).await?;
        let path = path.join("this.does.not.work");
        let p = path.to_string_lossy().to_string();
        let r = super::open(&path).await;
        assert!(r.is_err());
        let err = r.err().unwrap();
        assert!(matches!(err, Error::FileOpen(_, bad) if bad == p));
        Ok(())
    }

    #[async_std::test]
    async fn open_with() -> Result<(), Error> {
        let path = std::env::current_dir().unwrap();
        let path = path.join("Cargo.toml");
        let mut opt = super::OpenOptions::new();
        opt.read(true);
        super::open_with(&path, &mut opt).await?;
        let path = path.join("this.does.not.work");
        let p = path.to_string_lossy().to_string();
        let r = super::open_with(&path, &mut opt).await;
        assert!(r.is_err());
        let err = r.err().unwrap();
        assert!(matches!(err, Error::FileOpen(_, bad) if bad == p));
        Ok(())
    }
}
