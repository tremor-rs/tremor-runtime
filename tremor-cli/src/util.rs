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

use crate::errors::{Error, Result};
use halfbrown::HashMap;
use serde::Deserialize;
use simd_json::BorrowedValue as Value;
use std::ffi::OsStr;
use std::fs;
use std::io::prelude::*;
use std::io::BufReader;
use std::path::Path;
use tremor_common::file as cfile;
use tremor_script::highlighter::{Highlighter, Term as TermHighlighter};
use tremor_script::lexer;

pub(crate) enum FormatKind {
    Json,
    Yaml,
}

pub(crate) fn slurp_string(file: &str) -> Result<String> {
    let data = crate::open_file(file, None)?;
    let mut buffered_reader = BufReader::new(data);
    let mut data = String::new();
    buffered_reader.read_to_string(&mut data)?;
    Ok(data)
}

#[derive(Deserialize, Debug, Serialize)]
pub(crate) struct TargetConfig {
    pub(crate) instances: HashMap<String, Vec<String>>, // TODO FIXME TremorURL
}

pub(crate) struct TremorApp {
    pub(crate) format: FormatKind,
    pub(crate) config: TargetConfig,
}

impl TremorApp {
    fn default_url(&self) -> Result<url::Url> {
        self.config
            .instances
            .get("default")
            .and_then(|v| v.get(0))
            .ok_or_else(|| Error::from("No default api endpoint provided in ~/.tremor/config"))
            .and_then(|s| Ok(url::Url::parse(s)?))
    }

    pub(crate) fn endpoint(&self, endpoint: &str) -> Result<url::Url> {
        self.default_url().and_then(|mut url| {
            url.path_segments_mut()
                .map_err(|_| Error::from("Bad endpoint api"))?
                .push(endpoint);
            Ok(url)
        })
    }

    pub(crate) fn endpoint_id(&self, endpoint: &str, id: &str) -> Result<url::Url> {
        self.default_url().and_then(|mut url| {
            url.path_segments_mut()
                .map_err(|_| Error::from("Bad endpoint api"))?
                .push(endpoint)
                .push(id);
            Ok(url)
        })
    }

    pub(crate) fn endpoint_id_instance(
        &self,
        endpoint: &str,
        id: &str,
        instance: &str,
    ) -> Result<url::Url> {
        self.default_url().and_then(|mut url| {
            url.path_segments_mut()
                .map_err(|_| Error::from("Bad endpoint api"))?
                .push(endpoint)
                .push(id)
                .push(instance);
            Ok(url)
        })
    }
}

pub(crate) fn tremor_home_dir() -> Result<String> {
    dirs::home_dir()
        .and_then(|s| s.to_str().map(ToString::to_string))
        .ok_or_else(|| Error::from("Expected home_dir"))
        .map(|tremor_root| format!("{}/{}", tremor_root, ".tremor"))
}

pub(crate) fn save_config(config: &TargetConfig) -> Result<()> {
    let tremor_root = tremor_home_dir()?;
    let dot_config = format!("{}/config.yaml", tremor_root);
    let raw = serde_yaml::to_vec(&config)?;
    let mut file = cfile::create(&dot_config)?;
    Ok(file.write_all(&raw)?)
}

pub(crate) fn load_config() -> Result<TargetConfig> {
    let tremor_root = tremor_home_dir()?;
    let dot_config = format!("{}/config.yaml", tremor_root);
    let mut default = TargetConfig {
        instances: HashMap::new(),
    };
    default.instances.insert(
        "default".to_string(),
        vec!["http://localhost:9898".to_string()],
    );
    let meta = fs::metadata(&tremor_root);
    match meta {
        Ok(meta) => {
            if meta.is_dir() {
                let meta = fs::metadata(dot_config.clone());
                match meta {
                    Ok(meta) => {
                        if meta.is_file() {
                            let mut source = crate::open_file(&dot_config, None)?;
                            let mut raw = vec![];
                            source.read_to_end(&mut raw)?;
                            Ok(serde_yaml::from_slice(raw.as_slice())?)
                        } else {
                            Ok(default)
                        }
                    }
                    Err(_file) => {
                        save_config(&default)?;
                        load_config()
                    }
                }
            } else {
                Ok(default)
            }
        }
        Err(_dir) => {
            fs::create_dir(&tremor_root)?;
            load_config()
        }
    }
}

pub(crate) fn load(path_to_file: &str) -> Result<simd_json::OwnedValue> {
    let mut source = crate::open_file(path_to_file, None)?;
    let ext = cfile::extension(path_to_file)
        .ok_or_else(|| format!("Could not open fail path {}", path_to_file))?;
    let mut raw = vec![];
    source.read_to_end(&mut raw)?;

    if ext == "yaml" || ext == "yml" {
        Ok(serde_yaml::from_slice(raw.as_slice())?)
    } else if ext == "json" {
        Ok(simd_json::to_owned_value(raw.as_mut_slice())?)
    } else {
        Err(Error::from(format!("Unsupported format: {}", ext)))
    }
}

pub(crate) fn load_trickle(path_to_file: &str) -> Result<String> {
    let mut source = crate::open_file(path_to_file, None)?;
    let ext = cfile::extension(path_to_file)
        .ok_or_else(|| format!("Could not open fail path {}", path_to_file))?;
    let mut raw = String::new();
    source.read_to_string(&mut raw)?;

    if ext == "trickle" {
        Ok(raw)
    } else {
        Err(Error::from(format!("Unsupported format: {}", ext)))
    }
}

pub(crate) fn content_type(app: &TremorApp) -> &'static str {
    match app.format {
        FormatKind::Json => "application/json",
        FormatKind::Yaml => "application/yaml",
    }
}

pub(crate) fn accept(app: &TremorApp) -> &'static str {
    match app.format {
        FormatKind::Json => "application/json",
        FormatKind::Yaml => "application/yaml",
    }
}

pub(crate) fn ser(app: &TremorApp, json: &simd_json::OwnedValue) -> Result<String> {
    Ok(match app.format {
        FormatKind::Json => simd_json::to_string(&json)?,
        FormatKind::Yaml => serde_yaml::to_string(&json)?,
    })
}

pub(crate) type PathVisitor = dyn Fn(Option<&Path>, &Path) -> Result<()>;

pub(crate) fn visit_path_str(path: &str, visitor: &PathVisitor) -> Result<()> {
    let path = Path::new(path);
    visit_path(&path, &path, visitor)
}

pub(crate) fn relative_path(
    base: &Path,
    path: &Path,
) -> std::result::Result<String, std::path::StripPrefixError> {
    match path.strip_prefix(base) {
        Ok(path) => Ok(path.to_string_lossy().to_string()),
        Err(e) => Err(e),
    }
}

pub(crate) fn visit_path<'a>(base: &Path, path: &Path, visitor: &'a PathVisitor) -> Result<()> {
    if path.is_file() {
        visitor(None, &path)?
    } else if path.is_dir() {
        // We process files first, followed by directories to impose
        // an order of visitation that follows the nested heirarchy from
        // outermost to innermost as the native ordering typically does
        // not follow a heirarchic visitation order
        //

        for entry in std::fs::read_dir(&path)? {
            let entry = entry?;
            let path = entry.path();
            let rel_path = relative_path(base, &path);
            match rel_path {
                Ok(rel_path) => {
                    if path.is_file() {
                        visitor(Some(Path::new(&rel_path)), path.as_path())?;
                    }
                }
                Err(e) => error!(
                    "could not create relative path for `{}`: {}",
                    path.to_string_lossy(),
                    e
                ),
            }
        }

        for entry in std::fs::read_dir(&path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                visit_path(base, path.as_path(), visitor)?
            }
        }
    }

    Ok(())
}

#[derive(Copy, Clone)]
pub(crate) enum SourceKind {
    Pipeline,
    Tremor,
    Trickle,
    Json,
    Unsupported,
}

pub(crate) fn get_source_kind(path: &str) -> SourceKind {
    match cfile::extension(path) {
        Some("json") => SourceKind::Json,
        Some("tremor") => SourceKind::Tremor,
        Some("trickle") => SourceKind::Trickle,
        Some("yaml") => SourceKind::Pipeline,
        _otherwise => SourceKind::Unsupported,
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
    let lexed_tokens: Vec<_> = lexer::Tokenizer::new(&result)
        .filter_map(std::result::Result::ok)
        .collect();

    let mut h = TermHighlighter::new();
    if let Err(e) = h.highlight(Some(&result), &lexed_tokens) {
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
