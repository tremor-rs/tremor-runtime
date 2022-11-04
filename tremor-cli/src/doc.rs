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

use crate::cli::Doc;
use crate::errors::{Error, Result};
use crate::util::visit_path_str;
use std::ffi::OsStr;
use std::io::Read;
use std::path::{Path, PathBuf};
use tremor_script::arena::Arena;
use tremor_script::module::{Id, Module};

fn push_line(line: &str, buf: &mut String) {
    buf.push_str(line);
    if !line.ends_with('\n') {
        buf.push('\n');
    }
}

fn gen_module_docs(module: &Module, module_name: &str) -> String {
    let mut gen = String::new();
    if let Some(mod_doc) = module.docs.module.as_ref() {
        gen.push_str(&mod_doc.print_with_name(module_name));
    }
    if !module.content.connectors.is_empty() {
        push_line("## Connectors", &mut gen);

        for (connector_id, defn) in &module.content.connectors {
            push_line(&format!("### {connector_id}"), &mut gen);

            // TODO: create `ConnectorDoc`
            if let Some(doc) = defn.docs.as_ref() {
                push_line(doc, &mut gen);
            } else {
                // at least list undocumented connector
                // TODO: derive docs from `ConnectorDefinition`
                push_line(&format!("### {connector_id}"), &mut gen);
            }
        }
    }

    if !module.content.pipelines.is_empty() {
        push_line("## Pipelines", &mut gen);
        for (pipeline_id, defn) in &module.content.pipelines {
            // pipeline docs are somehow not part of the ast node, but are pushed into the helper
            if let Some(query_doc) = module.docs.queries.iter().find(|doc| doc.name == defn.id) {
                push_line(&query_doc.to_string(), &mut gen);
            } else {
                // at least list undocumented pipeline
                // TODO: derive docs from `PipelineDefinition`
                push_line(&format!("### {pipeline_id}"), &mut gen);
            }
        }
    }

    if !module.content.windows.is_empty() {
        push_line("## Windows", &mut gen);
        for (window_id, defn) in &module.content.windows {
            // window definition docs are put into queries somehow
            if let Some(window_doc) = module.docs.queries.iter().find(|doc| doc.name == defn.id) {
                push_line(&window_doc.to_string(), &mut gen);
            } else {
                // at least list undocumented window
                // TODO: derive docs from `WindowDefinition`
                push_line(&format!("### {window_id}"), &mut gen);
            }
        }
    }

    if !module.content.scripts.is_empty() {
        push_line("## Scripts", &mut gen);
        for (script_id, defn) in &module.content.scripts {
            // script definition docs are put into queries somehow
            if let Some(script_doc) = module.docs.queries.iter().find(|doc| doc.name == defn.id) {
                push_line(&script_doc.to_string(), &mut gen);
            } else {
                // at least list undocumented script
                // TODO: derive docs from `ScriptDefinition`
                push_line(&format!("### {script_id}"), &mut gen);
            }
        }
    }

    if !module.content.operators.is_empty() {
        push_line("## Operators", &mut gen);
        for (op_id, defn) in &module.content.operators {
            // script definition docs are put into queries somehow
            if let Some(op_doc) = module.docs.queries.iter().find(|doc| doc.name == defn.id) {
                push_line(&op_doc.to_string(), &mut gen);
            } else {
                // at least list undocumented operator
                // TODO: derive docs from `OperatorDefinition`
                push_line(&format!("### {op_id}"), &mut gen);
            }
        }
    }

    if !module.content.flows.is_empty() {
        push_line("## Flows", &mut gen);
        for (flow_id, defn) in &module.content.flows {
            push_line(&format!("### {flow_id}"), &mut gen);
            if let Some(doc) = defn.docs.as_ref() {
                push_line(doc, &mut gen);
            } else {
                // at least list undocumented flow
                // TODO: add derive docs from `FlowDefinition`
                push_line(&format!("### {flow_id}"), &mut gen);
            }
        }
    }

    if !module.content.consts.is_empty() {
        push_line("## Constants", &mut gen);
        for (c_id, defn) in &module.content.consts {
            if let Some(const_doc) = module.docs.consts.iter().find(|doc| doc.name == defn.id) {
                push_line(&const_doc.to_string(), &mut gen);
            } else {
                // at least list undocumented constant
                // TODO: add derive docs from `Const`
                push_line(&format!("### {c_id}"), &mut gen);
            }
        }
    }

    if !module.content.functions.is_empty() {
        push_line("## Functions", &mut gen);
        for (fn_id, defn) in &module.content.functions {
            if let Some(fn_doc) = module.docs.fns.iter().find(|doc| doc.name == defn.name) {
                push_line(&fn_doc.to_string(), &mut gen);
            } else {
                // at least list undocumented function
                // TODO: add derive docs from `FnDefn`
                push_line(&format!("### {fn_id}"), &mut gen);
            }
        }
    }
    gen
}

fn gen_doc(
    is_interactive: bool,
    rel_path: Option<&Path>,
    dest_path: Option<&str>,
    path: &Path,
) -> Result<()> {
    let rel_path = rel_path
        .ok_or_else(|| Error::from(format!("Bad relative path: {}", path.to_string_lossy())))?;
    let dest_path = dest_path.ok_or_else(|| Error::from("Bad destination path"))?;

    let module = match path.extension().and_then(OsStr::to_str) {
        Some("tremor" | "troy") => {
            let mut raw = String::new();
            let mut input = crate::open_file(path, None)?;
            input.read_to_string(&mut raw)?;
            let module_id = Id::from(raw.as_bytes());
            let mut ids = Vec::new();
            let (aid, raw) = Arena::insert(&raw)?;
            Module::load(module_id, &mut ids, aid, raw)?
        }
        Some(_) | None => return Ok(()),
    };

    let file_path_str = rel_path.to_string_lossy().to_string();
    let module_name = file_path_str
        .rsplit('/')
        .next()
        .ok_or_else(|| Error::from("Could not isolate relative path"))?
        .replace(".tremor", "")
        .replace(".troy", "");
    let gen = gen_module_docs(&module, module_name.as_str());

    if is_interactive {
        println!("{}", &gen);
    }

    let mut dest_file = PathBuf::new();
    dest_file.push(dest_path);
    dest_file.push(rel_path);
    let mut src_path = path.to_path_buf();
    src_path.set_extension("");
    if src_path.exists() && src_path.is_dir() {
        // create an index file for module level docs
        // this works best with our current docusaurus based setup
        //
        // Example: aggr.md -> aggr/index.md
        dest_file.set_extension("");
        dest_file.push("index");
    }
    dest_file.set_extension("md");
    let parent = dest_file.parent().and_then(Path::to_str).ok_or_else(|| {
        Error::from(format!(
            "Could not get output path for {}",
            dest_file.to_string_lossy()
        ))
    })?;
    std::fs::create_dir_all(parent)
        .map_err(|e| Error::from(format!("Unable to generate output module folder: {}", e)))?;
    std::fs::write(dest_file, &gen)
        .map_err(|e| Error::from(format!("Unable to generate output: {}", e)))?;

    Ok(())
}

impl Doc {
    pub(crate) fn run(&self) -> Result<()> {
        tremor_script::module::Manager::add_path(&self.dir)?;
        let is_interactive = self.interactive;
        let dest_path = self.outdir.clone();
        visit_path_str(&self.dir, &move |rel_path, src_path| {
            // The closure exposes a 1-arity capture conforming to the PathVisitor 1-arity alias'd fn
            // whilst binding the locally defined is_interactive and dest_path command parameters
            // thereby adapting the 3-arity gen_doc to a 1-arity visitor callback fn
            //
            // This would be so much more elegant in erlang! Surely there's a more convivial syntax in rust?
            //
            gen_doc(is_interactive, rel_path, Some(&dest_path), src_path)
        })
    }
}
