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

use std::ffi::CStr;
use std::os::raw::c_char;

use crate::registry::Context;

#[derive(Clone, Default, PartialEq, Debug)]
struct FakeContext {}
impl Context for FakeContext {}

fn eval(src: &str) -> String {
    /*
    use crate::ast;
    use crate::interpreter;
    use crate::registry;
    use simd_json::borrowed::{Map, Value};

    let reg: registry::Registry<FakeContext> = registry::registry();
    let mut helper = ast::Helper::new(&reg);
    let script: ast::Script1 = serde_json::from_str(src).expect("");
    let script: ast::Script<_> = script.up(&mut helper).expect("");

    let script = interpreter::rentals::Script::new(Box::new(src.to_string()), |_| script);

    let runnable = interpreter::Script {
        script,
        source: String::new(),
        locals: helper.locals.clone(),
        warnings: helper.into_warnings(),
    };
    // let runnable: interpreter::Script = interpreter::Script::parse(src, &reg).expect("parse failed");
    let mut event = simd_json::borrowed::Value::Object(Map::new());
    let ctx = FakeContext {};
    let mut global_map = Value::Object(interpreter::LocalMap::new());
    let value = runnable.run(&ctx, &mut event, &mut global_map);
    let result = format!(
        "{} ",
        serde_json::to_string_pretty(&value.expect("")).expect("")
    );
    result
    TODO
     */
    String::from(src)
}

#[no_mangle]
pub extern "C" fn tremor_script_c_eval(c_ptr: *const c_char) -> *const u8 {
    let cstr = unsafe { CStr::from_ptr(c_ptr) };

    match cstr.to_str() {
        Ok(s) => {
            let result = eval(s).clone();
            let c_ptr = result.as_ptr() as *mut u8;
            unsafe {
                (*c_ptr.add(result.len() - 1)) = 0;
            }
            c_ptr
        }
        Err(e) => {
            dbg!(e);
            "ko".as_ptr() as *const u8
        }
    }
}
