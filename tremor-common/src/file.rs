// Copyright 2020, The Tremor Team
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
use std::{fs::File, path::Path};

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
