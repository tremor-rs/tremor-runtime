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

use crate::errors::Result;
use async_std::path::PathBuf;
use sha2::{Digest, Sha256};
use simd_json::OwnedValue;
use std::collections::HashMap;
use std::{collections::BTreeSet, io::Read};
use tar::{Archive, Header};
use tokio::io::AsyncWriteExt;
use tremor_common::{asy::file, base64};
use tremor_script::{arena, NodeMeta};
use tremor_script::{
    arena::Arena,
    ast::{
        warning::{Class, Warning},
        DeployStmt, Helper, NodeId,
    },
    deploy::Deploy,
    highlighter::{self, Highlighter},
    module::MODULES,
    prelude::Ranged,
    FN_REGISTRY,
};

use super::store::{AppId, FlowId};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AppFlow {
    /// arguments with possible default values
    /// arguments without default values are required
    pub args: HashMap<String, Option<OwnedValue>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TremorAppDef {
    pub name: AppId,
    /// hash of all the included files
    /// starting with the main.troy and then all `use`d files in order
    pub sha256: String,
    pub flows: HashMap<FlowId, AppFlow>,
}

impl TremorAppDef {
    #[must_use]
    pub fn name(&self) -> &AppId {
        &self.name
    }
}

/// Packages a tremor application into a tarball, entry point is the `main.troy` file, target the tar.gz file
/// # Errors
/// if the tarball cannot be created
pub async fn package(target: &str, entrypoint: &str, name: Option<String>) -> Result<()> {
    let mut output = file::create(target).await?;
    let name = PathBuf::from(entrypoint)
        .file_name()
        .map(PathBuf::from)
        .and_then(|p| {
            p.file_stem()
                .and_then(std::ffi::OsStr::to_str)
                .map(ToString::to_string)
        })
        .or(name)
        .ok_or("Failed to get name")?;
    output
        .write_all(&build_archive(&name, entrypoint).await?)
        .await?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn build_archive(name: &str, entrypoint: &str) -> Result<Vec<u8>> {
    use tar::Builder;
    let src = file::read_to_string(entrypoint).await?;
    let mut hasher = Sha256::new();

    let aggr_reg = tremor_script::registry::aggr();
    let mut hl = highlighter::Term::stderr();
    let src_str: &str = &src;
    let mut deploy = match Deploy::parse(&src_str, &*FN_REGISTRY.read()?, &aggr_reg) {
        Ok(deploy) => deploy,
        Err(e) => {
            hl.format_error(&e)?;
            return Err(e.into());
        }
    };
    let mut other_warnings = BTreeSet::new();
    let reg = &*FN_REGISTRY.read()?;
    let helper = Helper::new(reg, &aggr_reg);

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
            Ok((
                FlowId(k.to_string()),
                AppFlow {
                    args: v
                        .clone()
                        .params
                        .args
                        .0
                        .iter()
                        .map(|(k, v)| {
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
    if !flows.contains_key(&FlowId("main".to_string())) {
        let w = Warning {
            class: Class::Behaviour,
            outer: deploy.extent(),
            inner: deploy.extent(),
            msg: "No main flow found".to_string(),
        };
        other_warnings.insert(w);
    }

    deploy.warnings.extend(other_warnings);
    deploy.format_warnings_with(&mut hl)?;

    // first hash main.troy
    hasher.update(src.as_bytes());
    // then hash all the modules
    for aid in MODULES.read()?.modules().iter().map(|m| m.arena_idx) {
        if let Some(src) = Arena::get(aid)? {
            hasher.update(src.as_bytes());
        }
    }
    let hash = base64::encode(hasher.finalize().as_slice());
    info!("App {} Package hash: {}", name, hash);

    let app = TremorAppDef {
        name: AppId(name.to_string()),
        sha256: hash,
        flows,
    };

    let mut ar = Builder::new(Vec::new());
    let app = simd_json::to_vec(&app)?;
    let mut header = Header::new_gnu();
    header.set_size(app.len() as u64);
    header.set_cksum();
    ar.append_data(&mut header, "app.json", app.as_slice())?;

    let mut header = Header::new_gnu();
    header.set_size(src.as_bytes().len() as u64);
    header.set_cksum();
    ar.append_data(&mut header, "main.troy", src.as_bytes())?;

    for (id, paths) in MODULES
        .read()?
        .modules()
        .iter()
        .map(|m| (m.arena_idx, m.paths.clone()))
    {
        if let Some(src) = Arena::get(id)? {
            for p in &paths {
                let mut file: PathBuf = p.module().iter().collect();
                file.push(p.id());
                let mut header = Header::new_gnu();
                header.set_size(src.as_bytes().len() as u64);
                header.set_cksum();
                ar.append_data(&mut header, file, src.as_bytes())?;
            }
        }
    }

    Ok(ar.into_inner()?)
}

/// gets the app name from an archive
/// # Errors
/// if the archive is invalid
pub fn get_app(src: &[u8]) -> Result<TremorAppDef> {
    let mut ar = Archive::new(src);

    let mut entries = ar.entries()?;
    let mut app = entries.next().ok_or("Invalid archive: empty")??;

    if app.path()?.to_string_lossy() != "app.json" {
        return Err("Invalid archive: app.json missing".into());
    }
    let mut content = String::new();
    app.read_to_string(&mut content)?;

    let app: TremorAppDef = serde_json::from_str(&content)?;
    Ok(app)
}

/// Extract app deploy an all used arena indices
/// # Errors
/// if the archive is invalid
pub fn extract(src: &[u8]) -> Result<(TremorAppDef, Deploy, Vec<arena::Index>)> {
    let mut ar = Archive::new(src);

    let mut entries = ar.entries()?;
    let mut app = entries.next().ok_or("Invalid archive: empty")??;

    if app.path()?.to_string_lossy() != "app.json" {
        return Err("Invalid archive: app.json missing".into());
    }
    let mut content = String::new();
    app.read_to_string(&mut content)?;

    let app: TremorAppDef = serde_json::from_str(&content)?;

    let mut main = entries
        .next()
        .ok_or("Invalid archive: main.troy missing")??;

    content.clear();
    main.read_to_string(&mut content)?;
    let main = content;

    let mut modules = HashMap::new();

    for e in entries {
        let mut entry = e?;
        let path = entry.path()?;
        let mut module: Vec<_> = path
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect();
        let id = module.pop().ok_or("No module name")?;
        let module = NodeId::new(id.clone(), module.clone(), NodeMeta::dummy().to_owned());

        info!("included library: {}", entry.path()?.to_string_lossy());
        let mut contents = String::new();
        entry.read_to_string(&mut contents)?;
        let (aid, _) = Arena::insert(&contents)?;
        modules.insert(module, aid);
    }
    let aggr_reg = tremor_script::registry::aggr();

    let deploy = Deploy::parse_with_cache(&main, &*FN_REGISTRY.read()?, &aggr_reg, &modules)?;
    let mut aids = modules.into_values().collect::<Vec<_>>();
    aids.push(deploy.aid);
    Ok((app, deploy, aids))
}
