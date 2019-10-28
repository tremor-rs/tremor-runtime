// Copyright 2018-2019, Wayfair GmbH
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

use crate::errors::*;
use crate::registry::Registry; // AggrRegistry
use crate::script::{AggrType, Return, Script};
use crate::{registry, EventContext};
use simd_json::borrowed::{Object, Value};
use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;

#[cfg_attr(tarpaulin, skip)]
fn eval(src: &str) -> Result<String> {
    let reg: Registry = registry::registry();
    // let aggr_reg: AggrRegistry = registry::aggr_registry();
    let script = Script::parse(src, &reg)?;

    let mut event = Value::from(Object::new());
    let mut meta = Value::from(Object::new());
    let value = script.run(
        &EventContext {
            at: 0,
            origin_uri: None,
        },
        AggrType::Emit,
        &mut event,
        &mut meta,
    )?;
    Ok(match value {
        Return::Drop => String::from(r#"{"drop": null}"#),
        Return::Emit { value, .. } => format!(r#"{{"emit": {}}}"#, value.encode()),
        Return::EmitEvent { .. } => format!(r#"{{"emit": {}}}"#, event.encode()),
    })
}

#[no_mangle]
#[cfg_attr(tarpaulin, skip)]
pub extern "C" fn tremor_script_c_eval(script: *const c_char, dst: *mut u8, len: usize) -> usize {
    let cstr = unsafe { CStr::from_ptr(script) };
    match cstr.to_str().map_err(Error::from).and_then(|s| eval(s)) {
        Ok(result) => {
            let result = result.clone();
            if result.len() < len {
                unsafe {
                    let src = result.as_ptr() as *const u8;
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
