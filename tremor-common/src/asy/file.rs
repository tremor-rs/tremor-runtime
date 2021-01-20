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
#![cfg(not(tarpaulin_include))]

use async_std::{fs::File, path::Path, path::PathBuf};

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

/// A wrapper around `File::create` that will give a better error (including the filename)
///
/// # Errors
///   * if the file couldn't be created
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
