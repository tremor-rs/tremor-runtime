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

extern crate getopts;

use getopts::Options;
use rand::prelude::*;
use std::env;
use std::fs;
use std::path::Path;

#[cfg_attr(tarpaulin, skip)]
fn main() {
    let args: Vec<String> = env::args().collect();
    let programme = args[0].clone();

    let mut opts = Options::new();
    opts.optopt("d", "output_dir", "output directory", "STRING");
    opts.optopt("f", "no_of_files", "no of files", "NUMBER");
    opts.optopt("r", "no_of_records", "no of records", "NUMBER");
    opts.optflag(
        "j",
        "jitter",
        "jitter the payload by adding fewer or more records to each file",
    );
    opts.optflag("h", "help", "print this help menu");
    opts.optflag("v", "verbose", "run in verbose mode");
    // opts.optflag("s", "use_subdirectories", "create files in subdirectories as well as the top level one");

    let mut output_dir = env::current_dir()
        .unwrap()
        .as_os_str()
        .to_str()
        .unwrap()
        .to_string();
    let mut no_of_files: i64 = 10;
    let mut no_of_records: i64 = 10;
    let mut use_jitter: bool = false;
    // let mut use_subdirs: bool = false;

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => panic!(f.to_string()),
    };

    if matches.opt_present("h") {
        print_usage(&programme, opts);
        return;
    };

    if matches.opt_present("d") {
        output_dir = matches.opt_get("d").unwrap().unwrap();
    };

    if matches.opt_present("f") {
        no_of_files = matches.opt_get("f").unwrap().unwrap();
    };

    if matches.opt_present("r") {
        no_of_records = matches.opt_get("r").unwrap().unwrap();
    };

    if matches.opt_present("j") {
        use_jitter = true;
    };

    // if matches.opt_present("s") {
    //    println!("use subdirs");
    //    use_subdirs = true;
    // };

    if matches.opt_present("v") {
        println!("Run in verbose mode");
        println!("output dir:    {:?}", output_dir);
        println!("no of files:   {}", no_of_files);
        println!("no of records: {}", no_of_records);
        // println!("use subdirs {}", use_subdirs);
        println!("use jitter:    {}", use_jitter);
    };

    let mut n_recs = 1;
    let mut filecontents = String::new();

    for n in 0..no_of_files {
        let upper_bound = get_upper(use_jitter, no_of_records);
        for _r in 0..upper_bound {
            let json = "{\"record_no\":".to_owned() + &n_recs.to_string() + &"}\n".to_owned();
            filecontents.push_str(&json);
            n_recs += 1;
        }

        let dir = output_dir.clone();
        let filepath = dir + &"/file_no_".to_owned() + &n.to_string() + &".json".to_owned();
        let filename = Path::new(&filepath);
        fs::write(filename, &filecontents).unwrap();
        filecontents.truncate(0);
    }
}

fn get_upper(use_jitter: bool, no_of_records: i64) -> i64 {
    if use_jitter {
        let mut rng = rand::thread_rng();
        let mut random: f64 = rng.gen();
        random -= 0.5;
        let offset = random * (2 * no_of_records) as f64;
        no_of_records + offset as i64
    } else {
        no_of_records
    }
}

fn print_usage(programme: &str, opts: Options) {
    let brief = format!("Usage: {} FILE options]", programme);
    print!("{}", opts.usage(&brief));
}
