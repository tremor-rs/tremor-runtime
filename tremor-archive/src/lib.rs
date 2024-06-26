// Copyright 2022, The Tremor Team
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

//! Tremor arcive module for packaging and unpacking tremor apps

#![deny(warnings)]
#![deny(missing_docs)]
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic,
    clippy::mod_module_files
)]

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use simd_json::OwnedValue;
use std::{
    collections::{BTreeSet, HashMap},
    path::PathBuf,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio_stream::StreamExt;
use tokio_tar::{Archive, Builder, Header};
use tremor_common::{alias, asy::file, base64};
use tremor_script::{
    arena::{self, Arena},
    ast::{
        warning::{Class, Warning},
        DeployStmt, Helper, NodeId,
    },
    deploy::Deploy,
    highlighter::{self, Highlighter},
    module::{Manager, PreCachedNodes, MODULES},
    prelude::Ranged,
    NodeMeta, FN_REGISTRY,
};

use log::{debug, error, info};

/// archive result
pub type Result<T> = std::result::Result<T, Error>;

/// Archive errors
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Failed to get parent dir
    #[error("Failed to get parent dir")]
    NoParentDir,
    /// Failed to get name
    #[error("Failed to get name")]
    NoName,
    /// `app.json` missing
    #[error("`app.json` missing")]
    SpecMissing,
    /// `main.troy` missing
    #[error("`main.troy` missing")]
    NoEntrypoint,
    /// Archive is empty
    #[error("Archive is empty")]
    Empty,
    /// No module name Specified
    #[error("No module name Specified")]
    NoModuleName,
    /// Failed to aquire read lock
    #[error("Failed to aquire read lock")]
    ReadLock,
    /// Tremor Common Error
    #[error("Tremor Common Error")]
    Common(#[from] tremor_common::Error),
    /// Tremor Script Error
    #[error("Tremor Script Error")]
    Script(#[from] tremor_script::errors::Error),
    /// IO Error
    #[error("IO Error")]
    Io(#[from] std::io::Error),
    /// Serde Error
    #[error("Serde Error")]
    Serde(#[from] serde_json::Error),
    /// Simd JSON Error
    #[error("Simd JSON Error")]
    SimdJson(#[from] simd_json::Error),
}

/// A tremor app flow with defaults and arguments
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AppFlow {
    /// arguments with possible default values
    /// arguments without default values are required
    pub args: HashMap<String, Option<OwnedValue>>,
}

/// A tremor app definition
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TremorAppDef {
    /// name of the app
    pub name: alias::App,
    /// entrypoint of the app, this flow must exist
    pub entrypoint: String,
    /// hash of all the included files
    /// starting with the main.troy and then all `use`d files in order
    pub sha256: String,
    /// flows in the app
    pub flows: HashMap<alias::Instance, AppFlow>,
}

impl TremorAppDef {
    /// returns the name of the app
    #[must_use]
    pub fn name(&self) -> &alias::App {
        &self.name
    }
    // #[cfg(test)]
    // pub(crate) fn dummy() -> Self {
    //     Self {
    //         name: alias::App("dummy".to_string()),
    //         sha256: "dummy".to_string(),
    //         flows: HashMap::new(),
    //     }
    // }
}

/// Packages a tremor application into a tarball, entry point is the `main.troy` file, target the tar.gz file
/// # Errors
/// if the tarball cannot be created
pub async fn package<W: AsyncWrite + Unpin + Send>(
    target: &mut W,
    file: &str,
    name: Option<String>,
    entrypoint: Option<String>,
) -> Result<()> {
    let f = PathBuf::from(file);
    let dir = f.parent().ok_or(Error::NoParentDir)?;
    let path = dir.to_string_lossy();
    info!("Adding {path} to path");
    Manager::add_path(&path)?;
    let name = name
        .or_else(|| {
            f.file_stem()
                .and_then(std::ffi::OsStr::to_str)
                .map(ToString::to_string)
        })
        .ok_or(Error::NoName)?;
    let entrypoint = entrypoint.unwrap_or_else(|| "main".to_string());
    info!("Building archive for {name} with entrypoint {entrypoint}");
    build_archive(&name, entrypoint, file, target).await
}

pub(crate) async fn build_archive<W: AsyncWrite + Unpin + Send>(
    name: &str,
    entrypoint: String,
    file: &str,
    w: &mut W,
) -> Result<()> {
    let src = file::read_to_string(file).await?;
    build_archive_from_source(name, entrypoint, src.as_str(), w).await?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn build_archive_from_source<W: AsyncWrite + Unpin + Send>(
    name: &str,
    entrypoint: String,
    src: &str,
    w: W,
) -> Result<()> {
    let mut hasher = Sha256::new();

    let aggr_reg = tremor_script::registry::aggr();
    let mut hl = highlighter::Term::stderr();

    let mut deploy = match Deploy::parse(
        &src,
        &*FN_REGISTRY.read().map_err(|_| Error::ReadLock)?,
        &aggr_reg,
    ) {
        Ok(deploy) => deploy,
        Err(e) => {
            hl.format_error(&e)?;
            return Err(e.into());
        }
    };
    let mut other_warnings = BTreeSet::new();

    for stmt in &deploy.deploy.stmts {
        match stmt {
            DeployStmt::FlowDefinition(_)
            | DeployStmt::PipelineDefinition(_)
            | DeployStmt::ConnectorDefinition(_) => (),
            DeployStmt::DeployFlowStmt(f) => {
                let warning = Warning {
                    class: Class::Behaviour,
                    outer: f.extent(),
                    inner: f.extent(),
                    msg: "Deploying flows in applications is not supported. This statement will be ignored".to_string(),
                };
                other_warnings.insert(warning);
            }
        }
    }
    let flows: HashMap<_, _> = deploy
        .deploy
        .scope
        .content
        .flows
        .iter()
        .map(|(k, v)| {
            debug!("Flow {k} added to tremor archive.");
            Ok((
                alias::Instance(k.to_string()),
                AppFlow {
                    args: v
                        .clone()
                        .params
                        .args
                        .0
                        .iter()
                        .map(|(k, v)| {
                            let reg = &*FN_REGISTRY.read().map_err(|_| Error::ReadLock)?;
                            let helper = Helper::new(reg, &aggr_reg);

                            Ok((
                                k.to_string(),
                                v.clone()
                                    .map(|v| v.try_into_value(&helper))
                                    .transpose()?
                                    .map(OwnedValue::from),
                            ))
                        })
                        .collect::<Result<_>>()?,
                },
            ))
        })
        .collect::<Result<_>>()?;
    if !flows.contains_key(&alias::Instance(entrypoint.clone())) {
        let w = Warning {
            class: Class::Behaviour,
            outer: deploy.extent(),
            inner: deploy.extent(),
            msg: format!("Entrypoint flow `{entrypoint}` not defined"),
        };
        other_warnings.insert(w);
    }

    deploy.warnings.extend(other_warnings);
    deploy.format_warnings_with(&mut hl)?;

    // first hash main.troy
    hasher.update(src.as_bytes());
    // then hash all the modules
    for aid in MODULES
        .read()
        .map_err(|_| Error::ReadLock)?
        .modules()
        .iter()
        .map(|m| m.arena_idx)
    {
        if let Some(src) = Arena::get(aid)? {
            hasher.update(src.as_bytes());
        }
    }
    let hash = base64::encode(hasher.finalize().as_slice());
    info!("App {name} Package hash: {hash}");

    let app = TremorAppDef {
        name: alias::App(name.to_string()),
        entrypoint,
        sha256: hash,
        flows,
    };

    let mut ar = Builder::new_non_terminated(w);
    let app = simd_json::to_vec(&app)?;
    let mut header = Header::new_gnu();
    header.set_size(app.len() as u64);
    header.set_cksum();
    ar.append_data(&mut header, "app.json", app.as_slice())
        .await?;

    let mut header = Header::new_gnu();
    header.set_size(src.as_bytes().len() as u64);
    header.set_cksum();
    ar.append_data(&mut header, "main.troy", src.as_bytes())
        .await?;
    let modules = MODULES
        .read()
        .map_err(|_| Error::ReadLock)?
        .modules()
        .iter()
        .map(|m| (m.arena_idx, m.paths().to_vec()))
        .collect::<Vec<_>>();
    for (id, paths) in modules {
        if let Some(src) = Arena::get(id)? {
            for p in &paths {
                let mut file: PathBuf = p.module().iter().collect();
                file.push(p.id());
                let mut header = Header::new_gnu();
                header.set_size(src.as_bytes().len() as u64);
                header.set_cksum();
                debug!("Adding module {paths:?} with id {id} as file {file:?} to archive");
                ar.append_data(&mut header, file, src.as_bytes()).await?;
            }
        } else {
            error!("Module {paths:?} not found");
        }
    }
    ar.into_inner().await?;
    Ok(())
}

/// gets the app name from an archive
/// # Errors
/// if the archive is invalid
pub async fn get_app(src: impl AsyncRead + Unpin + Send) -> Result<TremorAppDef> {
    let mut ar = Archive::new(src);

    let mut entries = ar.entries()?;
    let mut app = entries.next().await.ok_or(Error::Empty)??;

    if app.path()?.to_string_lossy() != "app.json" {
        return Err(Error::SpecMissing);
    }
    let mut content = String::new();
    app.read_to_string(&mut content).await?;

    let app: TremorAppDef = serde_json::from_str(&content)?;
    Ok(app)
}

/// Extract app deploy an all used arena indices
/// # Errors
/// if the archive is invalid
pub async fn extract(
    src: impl AsyncRead + Unpin + Send,
) -> Result<(TremorAppDef, Deploy, Vec<arena::Index>)> {
    let mut ar = Archive::new(src);

    let mut entries = ar.entries()?;
    let mut app = entries.next().await.ok_or(Error::Empty)??;

    if app.path()?.to_string_lossy() != "app.json" {
        return Err(Error::SpecMissing);
    }
    let mut content = String::new();
    app.read_to_string(&mut content).await?;

    let app: TremorAppDef = serde_json::from_str(&content)?;
    let mut main = entries.next().await.ok_or(Error::NoEntrypoint)??;

    content.clear();
    main.read_to_string(&mut content).await?;
    let main = content;

    let mut modules = PreCachedNodes::new();

    while let Some(e) = entries.next().await {
        let mut entry = e?;
        let path = entry.path()?;
        let mut module: Vec<_> = path
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect();
        let id = module.pop().ok_or(Error::NoModuleName)?;
        let module = NodeId::new(id.clone(), module.clone(), NodeMeta::dummy());

        info!("included library: {}", entry.path()?.to_string_lossy());
        let mut contents = String::new();
        entry.read_to_string(&mut contents).await?;
        let (aid, _) = Arena::insert(&contents)?;
        modules.insert(module, aid);
    }
    let aggr_reg = tremor_script::registry::aggr();
    let deploy = Deploy::parse_with_cache(
        &main,
        &*FN_REGISTRY.read().map_err(|_| Error::ReadLock)?,
        &aggr_reg,
        &modules,
    )?;
    let mut aids = modules.values().collect::<Vec<_>>();
    aids.push(deploy.aid);
    Ok((app, deploy, aids))
}
