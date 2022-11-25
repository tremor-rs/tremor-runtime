//! NIF for `tremor-script`

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

// #![deny(missing_docs)] this is a nif
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic,
    clippy::mod_module_files
)]

use rustler::error::Error as NifError;
use rustler::{Encoder, Env, NifResult, Term};
use tremor_script::{prelude::*, registry, Script};
// =================================================================================================
// resource
// =================================================================================================

/// Loads the nif
#[must_use]
pub fn on_load(_env: Env, _load_info: Term) -> bool {
    true
}
rustler::atoms! {
    ok,
}

#[rustler::nif]
fn eval<'e>(env: Env<'e>, src: &str) -> NifResult<Term<'e>> {
    let reg: Registry = registry::registry();
    let script = Script::parse(src, &reg).map_err(|_| NifError::Atom("compilation"))?;

    let mut event = Value::object();
    let mut meta = Value::object();
    let mut state = Value::null();
    let value = script
        .run(
            &EventContext::new(0, None),
            AggrType::Emit,
            &mut event,
            &mut state,
            &mut meta,
        )
        .map_err(|_| NifError::Atom("runtime"))?;
    let r = match value {
        Return::Drop => String::from(r#"{"drop": null}"#),
        Return::Emit { value, .. } => format!(r#"{{"emit": {}}}"#, value.encode()),
        Return::EmitEvent { .. } => format!(r#"{{"emit": {}}}"#, event.encode()),
    };
    Ok((ok(), r).encode(env))
}

rustler::init!("ts", [eval], load = on_load);
