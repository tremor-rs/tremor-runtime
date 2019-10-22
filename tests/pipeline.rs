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
use pretty_assertions::assert_eq;
use serde_yaml;
use simd_json::to_borrowed_value;
use simd_json::BorrowedValue as Value;
use std::fs::File;
use std::io::prelude::*;
use tremor_pipeline;
use tremor_pipeline::Event;
use tremor_runtime;
use tremor_runtime::errors::*;
use xz2::read::XzDecoder;

macro_rules! test_cases {

    ($($file:ident),*) => {
        $(
            #[test]
            fn $file() -> Result<()> {

                tremor_runtime::functions::load();
                let pipeline_file = concat!("tests/fixtures/", stringify!($file), "/pipeline.yaml");
                let in_file = concat!("tests/fixtures/", stringify!($file), "/in.xz");
                let out_file = concat!("tests/fixtures/", stringify!($file), "/out.xz");

                let mut file = File::open(pipeline_file)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;
                let config: tremor_pipeline::config::Pipeline = serde_yaml::from_str(&contents)?;
                let pipeline = tremor_pipeline::build_pipeline(config)?;
                let mut pipeline = pipeline.to_executable_graph(tremor_pipeline::buildin_ops)?;

                let file = File::open(in_file)?;
                let mut in_data = Vec::new();
                XzDecoder::new(file).read_to_end(&mut in_data)?;
                let mut in_lines = in_data
                    .lines()
                    .collect::<std::result::Result<Vec<String>, _>>()?;
                let mut in_bytes = Vec::new();
                unsafe {
                    for line in &mut in_lines {
                        in_bytes.push(line.as_bytes_mut())
                    }
                }
                let mut in_json = Vec::new();
                for bytes in in_bytes {
                    in_json.push(to_borrowed_value(bytes)?)
                }

                let file = File::open(out_file)?;
                let mut out_data = Vec::new();
                XzDecoder::new(file).read_to_end(&mut out_data)?;
                let mut out_lines = out_data
                    .lines()
                    .collect::<std::result::Result<Vec<String>, _>>()?;
                let mut out_bytes = Vec::new();
                unsafe {
                    for line in &mut out_lines {
                        out_bytes.push(line.as_bytes_mut())
                    }
                }
                let mut out_json = Vec::new();
                for bytes in out_bytes {
                    out_json.push(to_borrowed_value(bytes)?)
                }
                out_json.reverse();

                let mut results = Vec::new();
                for (id, json) in in_json.into_iter().enumerate() {
                    let event = Event {
                        id: id as u64,
                        data: json.clone_static().into(),
                        ingest_ns: id as u64,
                        kind: None,
                        is_batch: false,
                    };
                    let mut r = Vec::new();
                    pipeline.enqueue("in", event, &mut r)?;
                    results.append(&mut r);
                }
                assert_eq!(results.len(), out_json.len());
                for (_, result) in results {
                    for value in result.value_iter() {
                        if let Some(expected) = out_json.pop() {
                            assert_eq!(sorsorted_serialize(value)?, sorsorted_serialize(&expected)?);
                        }
                    }
                }
                Ok(())
            }
        )*
    };
}

fn sorsorted_serialize<'v>(j: &Value<'v>) -> Result<String> {
    let mut w = Vec::new();
    sorted_serialize_(j, &mut w)?;
    Ok(String::from_utf8(w)?)
}
fn sorted_serialize_<'v, W: Write>(j: &Value<'v>, w: &mut W) -> Result<()> {
    match j {
        Value::Null | Value::Bool(_) | Value::I64(_) | Value::F64(_) | Value::String(_) => {
            write!(w, "{}", j.encode())?;
        }
        Value::Array(a) => {
            let mut iter = a.iter();
            write!(w, "[")?;

            if let Some(e) = iter.next() {
                sorted_serialize_(e, w)?
            }

            for e in iter {
                write!(w, ",")?;
                sorted_serialize_(e, w)?
            }
            write!(w, "]")?;
        }
        Value::Object(o) => {
            let mut v: Vec<(String, Value<'v>)> =
                o.iter().map(|(k, v)| (k.to_string(), v.clone())).collect();

            v.sort_by_key(|(k, _)| k.to_string());
            let mut iter = v.into_iter();

            write!(w, "{{")?;

            if let Some((k, v)) = iter.next() {
                sorted_serialize_(&Value::from(k), w)?;
                write!(w, ":")?;
                sorted_serialize_(&v, w)?;
            }

            for (k, v) in iter {
                write!(w, ",")?;
                sorted_serialize_(&Value::from(k), w)?;
                write!(w, ":")?;
                sorted_serialize_(&v, w)?;
            }
            write!(w, "}}")?;
        }
    }
    Ok(())
}

test_cases!(
    default_rule,
    dimensions,
    example_rule,
    layered_limiting,
    lru,
    merge,
    multi_dimensions,
    passthrough,
    patch,
    rewrite_root,
    tremor_map
);
