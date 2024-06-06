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
use halfbrown::HashMap;
use serde::Deserialize;
use std::fs;
use std::path::Path;
use std::{ffi::OsStr, fmt};
use tremor_common::file as cfile;
use tremor_script::{
    arena::Arena,
    highlighter::{Highlighter, Term as TermHighlighter},
    lexer,
};
use tremor_value::Value;

// Wrapper around fs::read_to_string to provide better erros
// TODO create a tremor_common variant of fs::read_to_string
pub(crate) fn slurp_string<P: AsRef<Path>>(file: P) -> Result<String> {
    let data = fs::read_to_string(file)?;
    Ok(data)
}

#[derive(Deserialize, Debug, Serialize)]
pub(crate) struct TargetConfig {
    pub(crate) instances: HashMap<String, Vec<String>>, // TODO TremorUrl
}

pub(crate) type PathVisitor = dyn Fn(Option<&Path>, &Path) -> Result<()>;

pub(crate) fn visit_path_str(path: &str, visitor: &PathVisitor) -> Result<()> {
    let path = Path::new(path);
    visit_path(path, path, visitor)
}

pub(crate) fn visit_path(base: &Path, path: &Path, visitor: &PathVisitor) -> Result<()> {
    if path.is_file() {
        visitor(None, path)?;
    } else if path.is_dir() {
        // We process files first, followed by directories to impose
        // an order of visitation that follows the nested heirarchy from
        // outermost to innermost as the native ordering typically does
        // not follow a heirarchic visitation order
        //

        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            let rel_path = path.strip_prefix(base);
            match rel_path {
                Ok(rel_path) => {
                    if path.is_file() {
                        visitor(Some(rel_path), path.as_path())?;
                    }
                }
                Err(e) => error!(
                    "could not create relative path for `{}`: {}",
                    path.to_string_lossy(),
                    e
                ),
            }
        }

        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                visit_path(base, path.as_path(), visitor)?;
            }
        }
    }

    Ok(())
}

#[derive(Clone, PartialEq, Debug, Eq)]
pub enum SourceKind {
    /// A tremor source file
    Tremor,
    /// A trickle source file
    Trickle,
    /// A troy source file
    Troy,
    /// A json file
    Json,
    /// A tremor archive
    Archive,
    /// An unsuported file
    Unsupported(Option<String>),
}
impl fmt::Display for SourceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            SourceKind::Tremor => write!(f, "tremor"),
            SourceKind::Trickle => write!(f, "trickle"),
            SourceKind::Troy => write!(f, "troy"),
            SourceKind::Json => write!(f, "json"),
            SourceKind::Archive => write!(f, "tar"),
            SourceKind::Unsupported(None) => write!(f, "<NONE>"),
            SourceKind::Unsupported(Some(ext)) => write!(f, "{ext}"),
        }
    }
}

pub(crate) fn get_source_kind(path: &str) -> SourceKind {
    match cfile::extension(path) {
        Some("json") => SourceKind::Json,
        Some("tremor") => SourceKind::Tremor,
        Some("trickle") => SourceKind::Trickle,
        Some("troy") => SourceKind::Troy,
        Some("tar") => SourceKind::Archive,
        otherwise => SourceKind::Unsupported(otherwise.map(ToString::to_string)),
    }
}

pub(crate) fn highlight(is_pretty: bool, value: &Value) -> Result<()> {
    let result = format!(
        "{} ",
        if is_pretty {
            simd_json::to_string_pretty(&value)?
        } else {
            simd_json::to_string(&value)?
        }
    );
    let (aid, result) = Arena::insert(&result)?;
    let lexed_tokens: Vec<_> = lexer::Lexer::new(result, aid)
        .tokenize_until_err()
        .collect();

    let mut h = TermHighlighter::default();
    if let Err(e) = h.highlight(Some(result), &lexed_tokens, "", true, None) {
        return Err(e.into());
    };

    h.finalize()?;
    h.reset()?;
    Ok(())
}

pub(crate) fn basename(path: &str) -> String {
    Path::new(path)
        .file_name()
        .map(OsStr::to_string_lossy)
        .map_or_else(|| path.to_string(), String::from)
}
