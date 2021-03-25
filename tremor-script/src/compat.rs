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

// This file is urely for testing w/ EQC
#![cfg(not(tarpaulin_include))]

use crate::prelude::*;
use crate::{
    errors::{Error, Result},
    registry::{self, Registry},
    script::{AggrType, Return, Script},
    EventContext, Value,
};
use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;

fn eval(src: &str) -> Result<String> {
    let reg: Registry = registry::registry();
    // let aggr_reg: AggrRegistry = registry::aggr_registry();
    let script = Script::parse(&crate::path::load(), "<eval>", src.to_string(), &reg)
        .map_err(|e| e.error)?;

    let mut event = Value::object();
    let mut meta = Value::object();
    let mut state = Value::null();
    let value = script.run(
        &EventContext::new(0, None),
        AggrType::Emit,
        &mut event,
        &mut state,
        &mut meta,
    )?;
    Ok(match value {
        Return::Drop => String::from(r#"{"drop": null}"#),
        Return::Emit { value, .. } => format!(r#"{{"emit": {}}}"#, value.encode()),
        Return::EmitEvent { .. } => format!(r#"{{"emit": {}}}"#, event.encode()),
    })
}

#[no_mangle]

pub extern "C" fn tremor_script_c_eval(script: *const c_char, dst: *mut u8, len: usize) -> usize {
    let cstr = unsafe { CStr::from_ptr(script) };
    match cstr
        .to_str()
        .map_err(Error::from)
        .and_then(|ref mut s| eval(s))
    {
        Ok(result) => {
            if result.len() < len {
                unsafe {
                    let src = result.as_ptr().cast::<u8>();
                    ptr::copy_nonoverlapping(src, dst, result.len());
                    *dst.add(result.len()) = 0;
                    0
                }
            } else {
                result.len()
            }
        }
        Err(e) => {
            eprintln!("ERROR: {}", e);
            1
        }
    }
}
