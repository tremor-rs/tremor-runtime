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

// this are macros
// #![cfg_attr(coverage, no_coverage)]

macro_rules! instance {
    // crate::INSTANCE is never mutated after the initial setting
    // in main::run() so we can use this safely.
    () => {
        unsafe { crate::INSTANCE.to_string() }
    };
}

// stolen from https://github.com/popzxc/stdext-rs and slightly adapted
#[cfg(test)]
macro_rules! function_name {
    () => {{
        // Okay, this is ugly, I get it. However, this is the best we can get on a stable rust.
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let mut name = type_name_of(f);
        if let Some(stripped) = name.strip_suffix("::f") {
            name = stripped;
        };
        while let Some(stripped) = name.strip_suffix("::{{closure}}") {
            name = stripped;
        }
        if let Some(last) = name.split("::").last() {
            last
        } else {
            name
        }
    }};
}
