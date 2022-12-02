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

extern crate lalrpop;
use chrono_tz::TZ_VARIANTS;
use std::{io::Write, path::PathBuf};

const TIMEZONE_DOCS: &str = r#"### Timezones extracted from TZ data
### Never use the actual values, they are going to change.
###
### Usage:
###
### ```tremor
### use std::datetime;
### let date_str = datetime::format(datetime::with_timezone(0, datetime::timezones::EUROPE_BERLIN), datetime::formats::RFC3339);
### ```
"#;

fn create_timezones_tremor(file: &mut Vec<u8>) {
    //let mut file = std::fs::File::create(&path).expect("Unable to create file timezones.tremor");
    file.write_all(TIMEZONE_DOCS.as_bytes()).unwrap();

    for (index, tz) in TZ_VARIANTS.iter().enumerate() {
        let name = tz.name();
        let ident_name = tz
            .name()
            .replace("GMT+", "GMT_PLUS_")
            .replace("GMT-", "GMT_MINUS_")
            .chars()
            .map(|c| if c.is_alphanumeric() { c } else { '_' })
            .map(|c| c.to_ascii_uppercase())
            .collect::<String>();
        let docstring = format!("## Timezone name constant for {name}\n");
        let line = format!("const {ident_name} = {index};\n");
        file.write_all(docstring.as_bytes())
            .expect("expected writing to timezones.tremor to succeed");
        file.write_all(line.as_bytes())
            .expect("expected writing to timezones.tremor to succeed");
    }
}

// We can't really do this as the files would need to be generated here but need to live in
// tremor-runtime. This creates a chicken & Egg problem as we'd need to depend on aech other
// to build.
// const MIME_DOCS: &str = r#"### Mimetypes mappings to codecs
// ###
// ### Usage:
// ###
// ### ```tremor
// ### use tremor::mime;
// ### let codec = mime_to_codec["application/json"];
// ### ```
// "#;

// fn create_mime_file(file: &mut Vec<u8>) {
//     //let mut file = std::fs::File::create(&path).expect("Unable to create file timezones.tremor");
//     file.write_all(MIME_DOCS.as_bytes()).unwrap();
//     file write_all("## Mapping of mimetypes to codecs\n")
//     file.write_all("const mime_to_codec = {\n");
//     for (m, c) in ... {
//     }
//     file.write_all("};\m");
//     file write_all("## Mapping of codecs to mimetypes\n")
//     file.write_all("const codec_to_mime = {\n");
//     for (m, c) in ... {
//     }
//     file.write_all("};\m");
// }

/// we generate the file into a buffer and compare it against the current one we only change it if
/// those two differ if so, we overwrite the old one.
///
/// The reasoning behing this is that a buidl script shouldn't modify anything outside of `$OUT_DIR`.
/// If we do nonetheless, cargo (righfully) complains during publishing the crate.
/// With this logic it only writes when either the data or this build script got updated.
/// We are going to catch this and do a clean commit, to avoid it happenign during publish.
fn maybe_rewrite_file<G>(path: &[&str], generator: G)
where
    G: Fn(&mut Vec<u8>),
{
    let mut generated = Vec::new();
    generator(&mut generated);
    let mut stdlib_file = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for p in path {
        stdlib_file.push(p);
    }

    let stdlib = std::fs::read(&stdlib_file).unwrap_or_else(|_| {
        panic!(
            "Expected {} to exist in the stdlib",
            stdlib_file.to_string_lossy()
        )
    });
    if stdlib != generated {
        // only write if the contents differ
        std::fs::write(&stdlib_file, &generated).unwrap_or_else(|_| {
            panic!(
                "Expected to be able to write to {}",
                stdlib_file.to_string_lossy()
            )
        });
    }
}

fn main() {
    lalrpop::Configuration::new()
        .use_cargo_dir_conventions()
        .process()
        .expect("Unable to initialize LALRPOP");

    println!("cargo:rustc-cfg=can_join_spans");
    println!("cargo:rustc-cfg=can_show_location_of_runtime_parse_error");

    maybe_rewrite_file(
        &["lib", "std", "datetime", "timezones.tremor"],
        create_timezones_tremor,
    );
    // maybe_rewrite_file(&["lib", "tremor", "mime.tremor"], create_mime_file);
}
