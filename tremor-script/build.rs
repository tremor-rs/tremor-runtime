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

fn create_timezones_tremor(file: &mut impl Write) {
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

fn main() {
    lalrpop::Configuration::new()
        .use_cargo_dir_conventions()
        .process()
        .expect("Unable to initialize LALRPOP");

    println!("cargo:rustc-cfg=can_join_spans");
    println!("cargo:rustc-cfg=can_show_location_of_runtime_parse_error");

    // we generate the timezones file into a buffer and compare it against the current one
    // we only change it if those two differ
    // if so, we overwrite the old one.
    //
    // The reasoning behing this is that a buidl script shouldn't modify anything outside of `$OUT_DIR`.
    // If we do nonetheless, cargo (righfully) complains during publishing the crate.
    // With this logic it only writes when either chrono_tz or this build script got updated.
    // We are going to catch this and do a clean commit, to avoid it happenign during publish.
    let mut generated = Vec::new();
    create_timezones_tremor(&mut generated);
    let mut stdlib_file = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    stdlib_file.push("lib");
    stdlib_file.push("std");
    stdlib_file.push("datetime");
    stdlib_file.push("timezones.tremor");

    let stdlib =
        std::fs::read(&stdlib_file).expect("Expected timezones.tremor to exist in the stdlib");
    if stdlib != generated {
        // only write if the contents differ
        std::fs::write(&stdlib_file, &generated)
            .expect("Expected to be able to write to timezones.tremor")
    }
}
