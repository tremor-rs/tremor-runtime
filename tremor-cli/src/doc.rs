// Copyright 2018-2020, Wayfair GmbH
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

use crate::util::*;
use clap::ArgMatches;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use tremor_runtime::errors::{Error, Result};
use tremor_script::path::load as load_module_path;
use tremor_script::registry;
use tremor_script::registry::Registry;
use tremor_script::script::Script;

fn gen_doc(
    is_interactive: bool,
    rel_path: &Option<&Path>,
    dest_path: &Option<String>,
    path: &Path,
) -> Result<()> {
    let mut raw = String::new();
    let input = File::open(&path);
    if let Err(e) = input {
        eprintln!("Error processing file {}: {}", &path.to_str().unwrap(), e);
        // ALLOW: main.rs
        std::process::exit(1);
    }
    input?.read_to_string(&mut raw)?;

    #[allow(unused_mut)]
    let mut reg: Registry = registry::registry();

    let mp = load_module_path();

    let name = rel_path
        .unwrap()
        .to_string_lossy()
        .to_string()
        .rsplit("/")
        .next()
        .unwrap()
        .replace(".tremor", "");

    match Script::parse(&mp, &path.to_str().unwrap(), raw.clone(), &reg) {
        Ok(runnable) => {
            let docs = runnable.docs();
            let consts = &docs.consts;
            let fns = &docs.fns;

            let _x: &tremor_script::ast::Script = runnable.script.suffix();

            let mut gen = String::new();
            if let Some(m) = &docs.module {
                gen.push_str(&m.print_with_name(&name));
            }
            if !consts.is_empty() {
                gen.push_str("## Constants");
                for c in consts {
                    gen.push_str(&c.to_string())
                }
            }

            if !fns.is_empty() {
                gen.push_str("## Functions");
                for f in fns {
                    gen.push_str(&f.to_string())
                }
            }

            if is_interactive {
                println!("{}", &gen);
            }

            if let &Some(path) = &dest_path {
                let mut dest_file = PathBuf::new();
                dest_file.push(path);
                dest_file.push(&rel_path.unwrap());
                if let Err(e) =
                    std::fs::create_dir_all(dest_file.parent().unwrap().to_string_lossy().as_ref())
                {
                    return Err(format!(
                        "Unable to generate output module folder: {}",
                        &e.to_string()
                    )
                    .into());
                };
                if let Err(e) =
                    std::fs::write(&dest_file.to_str().unwrap().replace(".tremor", ".md"), &gen)
                {
                    return Err(format!("Unable to generate output: {}", &e.to_string()).into());
                }
            }

            Ok(())
        }
        Err(_error) => {
            // ALLOW: main.rs
            std::process::exit(1);
        }
    }
}

pub(crate) fn run_cmd(matches: &ArgMatches) -> Result<()> {
    let src_path = matches
        .value_of("DIR")
        .ok_or_else(|| Error::from("No path provided"))?;

    let dest_path = matches.value_of("OUTDIR").ok_or("docs")?.to_string();
    let is_interactive: bool = matches.is_present("interactive");

    visit_path_str(src_path, &move |rel_path, src_path| {
        // The clojure exposes a 1-arity capture conforming to the PathVisitor 1-arity alias'd fn
        // whilst binding the locally defined is_interactive and dest_path command parameters
        // thereby adapting the 3-arity gen_doc to a 1-arity visitor callback fn
        //
        // This would be so much more elegant in erlang! Surely there's a more convivial syntax in rust?
        //
        gen_doc(
            is_interactive,
            &rel_path,
            &Some(dest_path.clone()),
            src_path,
        )
    })
}
