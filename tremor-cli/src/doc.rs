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

use crate::errors::{Error, Result};
use crate::util::visit_path_str;
use crate::{
    cli::Doc,
    env::{self, TremorCliEnv},
};
use std::io::Read;
use std::path::{Path, PathBuf};
use tremor_script::script::Script;

fn gen_doc(
    is_interactive: bool,
    rel_path: Option<&Path>,
    dest_path: Option<&str>,
    env: &TremorCliEnv,
    path: &Path,
) -> Result<()> {
    let rel_path = rel_path
        .ok_or_else(|| Error::from(format!("Bad relative path: {}", path.to_string_lossy())))?;
    let dest_path = dest_path.ok_or_else(|| Error::from("Bad destination path"))?;

    if let Some(ext) = path.extension() {
        let ext = ext.to_str();
        if Some("tremor") != ext {
            return Ok(()); // Skip unless it is a .tremor file
        }
    }

    let mut raw = String::new();
    let mut input = crate::open_file(path, None)?;
    input.read_to_string(&mut raw)?;

    let file_str = rel_path.to_string_lossy().to_string();
    let name = file_str
        .rsplit('/')
        .next()
        .ok_or_else(|| Error::from("Could not isolate relative path"))?
        .replace(".tremor", "");

    let runnable = Script::parse(&raw, &env.fun)?;
    let docs = runnable.docs();
    let consts = &docs.consts;
    let fns = &docs.fns;

    let mut gen = String::new();
    if let Some(m) = &docs.module {
        gen.push_str(&m.print_with_name(&name));
    }
    if !consts.is_empty() {
        gen.push_str("## Constants");
        for c in consts {
            gen.push_str(&c.to_string());
        }
    }

    if !fns.is_empty() {
        gen.push_str("## Functions");
        for f in fns {
            gen.push_str(&f.to_string());
        }
    }

    if is_interactive {
        println!("{}", &gen);
    }

    let mut dest_file = PathBuf::new();
    dest_file.push(dest_path);
    dest_file.push(&rel_path);
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
        let mut env = env::setup()?;
        env.module_path.add(&self.dir);
        let is_interactive = self.interactive;
        let dest_path = self.outdir.clone();
        visit_path_str(&self.dir, &move |rel_path, src_path| {
            // The closure exposes a 1-arity capture conforming to the PathVisitor 1-arity alias'd fn
            // whilst binding the locally defined is_interactive and dest_path command parameters
            // thereby adapting the 3-arity gen_doc to a 1-arity visitor callback fn
            //
            // This would be so much more elegant in erlang! Surely there's a more convivial syntax in rust?
            //
            gen_doc(is_interactive, rel_path, Some(&dest_path), &env, src_path)
        })
    }
}
