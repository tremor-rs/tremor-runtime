// Copyright 2020, The Tremor Team
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
use tremor_pipeline::query::Query;
use tremor_script::errors::CompilerError;

fn to_pipe(file_name: String, query: &str) -> std::result::Result<(), CompilerError> {
    let reg = tremor_script::registry();
    let aggr_reg = tremor_script::aggr_registry();
    let module_path = tremor_script::path::load();
    let cus = vec![];
    let q = Query::parse(&module_path, query, &file_name, cus, &reg, &aggr_reg)?;
    let mut uid = 0;
    q.to_pipe(&mut uid).map_err(|error| CompilerError {
        error: format!("{}", error).into(),
        cus: vec![],
    })?;
    Ok(())
}

macro_rules! test_files {

    ($($file:ident),*) => {
        $(
            #[test]
            fn $file() -> std::result::Result<(), CompilerError> {
                let contents = include_bytes!(concat!("queries/", stringify!($file), ".trickle"));
                to_pipe("test.trickle".to_string(), std::str::from_utf8(contents).map_err(|e| CompilerError{error: e.into(), cus: vec![]})?)
            }
        )*
    };
}

test_files!(for_in_select, script_with_args);
