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

use crate::errors::{Error, Result};
use crate::util::visit_path_str;
use clap::ArgMatches;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
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
    if let Some(rel_path) = *rel_path {
        if let Some(dest_path) = dest_path {
            if let Err(e) = input {
                eprintln!("Error processing file {}: {}", &path.to_string_lossy(), e);
                // ALLOW: main.rs
                std::process::exit(1);
            }
            input?.read_to_string(&mut raw)?;

            #[allow(unused_mut)]
            let mut reg: Registry = registry::registry();

            let mp = load_module_path();

            match rel_path.to_string_lossy().to_string().rsplit('/').next() {
                Some(name) => {
                    let name = name.replace(".tremor", "");
                    if let Some(path) = path.to_str() {
                        match Script::parse(&mp, &path, raw, &reg) {
                            Ok(runnable) => {
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

                                let mut dest_file = PathBuf::new();
                                dest_file.push(dest_path);
                                dest_file.push(&rel_path);

                                if let Some(parent) = dest_file.parent() {
                                    if let Err(e) =
                                        std::fs::create_dir_all(parent.to_string_lossy().as_ref())
                                    {
                                        return Err(format!(
                                            "Unable to generate output module folder: {}",
                                            &e.to_string()
                                        )
                                        .into());
                                    };
                                    if let Err(e) = std::fs::write(
                                        &dest_file.to_string_lossy().replace(".tremor", ".md"),
                                        &gen,
                                    ) {
                                        return Err(format!(
                                            "Unable to generate output: {}",
                                            &e.to_string()
                                        )
                                        .into());
                                    }
                                }

                                Ok(())
                            }
                            Err(_error) => {
                                // ALLOW: main.rs
                                std::process::exit(1);
                            }
                        }
                    } else {
                        Err("Bad path".into())
                    }
                }
                None => Err("Could not isolate relative path".into()),
            }
        } else {
            Err("Bad destination path".into())
        }
    } else {
        Err("Bad relative path".into())
    }
}

pub(crate) fn run_cmd(matches: &ArgMatches) -> Result<()> {
    let src_path = matches
        .value_of("DIR")
        .ok_or_else(|| Error::from("No path provided"))?;

    let dest_path = matches.value_of("OUTDIR").ok_or("docs")?.to_string();
    let is_interactive: bool = matches.is_present("interactive");

    Ok(visit_path_str(src_path, &move |rel_path, src_path| {
        // The closure exposes a 1-arity capture conforming to the PathVisitor 1-arity alias'd fn
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
    })?)
}
