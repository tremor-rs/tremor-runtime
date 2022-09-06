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
use async_std::path::PathBuf;
use simd_json::OwnedValue;
use std::collections::HashMap;
use std::{collections::BTreeSet, io::Read};
use tar::{Archive, Header};
use tokio::io::AsyncWriteExt;
use tremor_common::asy::file;
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
    pub args: HashMap<String, Option<OwnedValue>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TremorAppDef {
    pub name: AppId,
    pub flows: HashMap<FlowId, AppFlow>,
}

impl TremorAppDef {
    pub fn name(&self) -> &AppId {
        &self.name
    }
}

/// Packages a tremor application into a tarball, entry point is the `main.troy` file, target the output target
pub async fn package(target: &str, entrypoint: &str, name: Option<String>) -> Result<()> {
    let mut output = file::create(target).await?;
    let name = PathBuf::from(entrypoint)
        .file_name()
        .map(PathBuf::from)
        .and_then(|p| {
            p.file_stem()
                .and_then(|p| p.to_str())
                .map(ToString::to_string)
        })
        .or(name)
        .ok_or("Failed to get name")?;
    output
        .write_all(&build_archive(&name, entrypoint).await?)
        .await?;
    Ok(())
}

async fn build_archive(name: &str, entrypoint: &str) -> Result<Vec<u8>> {
    use tar::Builder;
    let src = file::read_to_string(entrypoint).await?;

    let aggr_reg = tremor_script::registry::aggr();
    let mut h = highlighter::Term::stderr();
    let mut d = match Deploy::parse(&src, &*FN_REGISTRY.read()?, &aggr_reg) {
        Ok(d) => d,
        Err(e) => {
            h.format_error(&e)?;
            return Err(e.into());
        }
    };
    let mut other_warnings = BTreeSet::new();
    let reg = &*FN_REGISTRY.read()?;
    let helper = Helper::new(&reg, &aggr_reg);

    for s in &d.deploy.stmts {
        match s {
            DeployStmt::FlowDefinition(_)
            | DeployStmt::PipelineDefinition(_)
            | DeployStmt::ConnectorDefinition(_) => (),
            DeployStmt::DeployFlowStmt(f) => {
                let w = Warning {
                    class: Class::Behaviour,
                    outer: f.extent(),
                    inner: f.extent(),
                    msg: "Deploying flows in applications is not supported this statement will be ignored".to_string(),
                };
                other_warnings.insert(w);
            }
        }
    }
    let flows: HashMap<_, _> = d
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
            outer: d.extent(),
            inner: d.extent(),
            msg: "No main flow found".to_string(),
        };
        other_warnings.insert(w);
    }

    d.warnings.extend(other_warnings);
    d.format_warnings_with(&mut h)?;

    let app = TremorAppDef {
        name: AppId(name.to_string()),
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

pub fn extract(src: &[u8]) -> Result<(TremorAppDef, Deploy)> {
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
        let module = NodeId::new(&id, &module);

        info!("included library: {}", entry.path()?.to_string_lossy());
        let mut contents = String::new();
        entry.read_to_string(&mut contents)?;
        let (aid, _) = Arena::insert(&contents)?;
        modules.insert(module, aid);
    }
    let aggr_reg = tremor_script::registry::aggr();

    let deploy = Deploy::parse_with_cache(&main, &*FN_REGISTRY.read()?, &aggr_reg, &modules)?;

    Ok((app, deploy))
}
