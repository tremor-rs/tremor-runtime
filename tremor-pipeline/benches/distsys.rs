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

use jemallocator;
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

use base64;
use criterion::{criterion_group, criterion_main, BatchSize, Benchmark, Criterion, Throughput};
use hashbrown::HashMap;
use rmp_serde as rmps;
use serde_yaml;
use std::fs::File;
use std::io::BufReader;
use std::io::{BufRead, Read};
use tremor_pipeline::{self, Event, EventValue};
use xz2::read::XzDecoder;

fn benchmark_json(c: &mut Criterion) {
    // Pipeline
    let file = File::open("benches/distsys-json.yaml").expect("could not open file");
    let buffered_reader = BufReader::new(file);
    let config = serde_yaml::from_reader(buffered_reader).unwrap();
    let pipeline = tremor_pipeline::build_pipeline(config).unwrap();
    let mut exec = pipeline
        .to_executable_graph(tremor_pipeline::buildin_ops)
        .unwrap();

    // Data
    let source_data_file = File::open("benches/bench.json.xz").unwrap();
    let mut data = vec![];
    XzDecoder::new(source_data_file)
        .read_to_end(&mut data)
        .expect("Neither a readable nor valid XZ compressed file error");
    let lines: Vec<Event> = data
        .lines()
        .map(|d| d.unwrap().into_bytes())
        .enumerate()
        .map(|(i, d)| Event {
            id: i as u64,
            ingest_ns: i as u64,
            meta: HashMap::new(),
            value: EventValue::Raw(d),
            kind: None,
        })
        .collect();
    let l = lines.len();
    c.bench(
        "pipeline throughput (json)",
        Benchmark::new("distsys", move |b| {
            b.iter_batched(
                || lines.clone(),
                |data| {
                    for e in data.into_iter() {
                        exec.enqueue("in", e).unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        })
        .throughput(Throughput::Elements(l as u32)),
    );
}

fn benchmark_msgpack(c: &mut Criterion) {
    // Pipeline
    let file = File::open("benches/distsys-msgpack.yaml").expect("could not open file");
    let buffered_reader = BufReader::new(file);
    let config = serde_yaml::from_reader(buffered_reader).unwrap();
    let pipeline = tremor_pipeline::build_pipeline(config).unwrap();
    let mut exec = pipeline
        .to_executable_graph(tremor_pipeline::buildin_ops)
        .unwrap();

    // Data
    let source_data_file = File::open("benches/bench.msgpack.xz").unwrap();
    let mut data = vec![];
    XzDecoder::new(source_data_file)
        .read_to_end(&mut data)
        .expect("Neither a readable nor valid XZ compressed file error");
    let lines: Vec<Event> = data
        .lines()
        .map(|d| base64::decode(&d.unwrap()).unwrap())
        .enumerate()
        .map(|(i, d)| Event {
            id: i as u64,
            ingest_ns: i as u64,
            meta: HashMap::new(),
            value: EventValue::Raw(d),
            kind: None,
        })
        .collect();
    let l = lines.len();
    c.bench(
        "pipeline throughput (msgpack)",
        Benchmark::new("distsys", move |b| {
            b.iter_batched(
                || lines.clone(),
                |data| {
                    for e in data.into_iter() {
                        exec.enqueue("in", e).unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        })
        .throughput(Throughput::Elements(l as u32)),
    );
}

fn benchmark_sql(c: &mut Criterion) {
    // Pipeline
    let file = File::open("benches/sql-json.yaml").expect("could not open file");
    let buffered_reader = BufReader::new(file);
    let config = serde_yaml::from_reader(buffered_reader).unwrap();
    let pipeline = tremor_pipeline::build_pipeline(config).unwrap();
    let mut exec = pipeline
        .to_executable_graph(tremor_pipeline::buildin_ops)
        .unwrap();

    // Data
    let source_data_file = File::open("benches/sql.json.xz").unwrap();
    let mut data = vec![];
    XzDecoder::new(source_data_file)
        .read_to_end(&mut data)
        .expect("Neither a readable nor valid XZ compressed file error");
    let lines: Vec<Event> = data
        .lines()
        .map(|d| d.unwrap().into_bytes())
        .enumerate()
        .map(|(i, d)| Event {
            id: i as u64,
            ingest_ns: i as u64,
            meta: HashMap::new(),
            value: EventValue::Raw(d),
            kind: None,
        })
        .collect();
    let l = lines.len();
    c.bench(
        "pipeline throughput (sql)",
        Benchmark::new("sql", move |b| {
            b.iter_batched(
                || lines.clone(),
                |data| {
                    for e in data.into_iter() {
                        exec.enqueue("in", e).unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        })
        .throughput(Throughput::Elements(l as u32)),
    );
}

fn benchmark_sql_msgpack(c: &mut Criterion) {
    // Pipeline
    let file = File::open("benches/sql-msgpack.yaml").expect("could not open file");
    let buffered_reader = BufReader::new(file);
    let config = serde_yaml::from_reader(buffered_reader).unwrap();
    let pipeline = tremor_pipeline::build_pipeline(config).unwrap();
    let mut exec = pipeline
        .to_executable_graph(tremor_pipeline::buildin_ops)
        .unwrap();

    // Data
    let source_data_file = File::open("benches/sql.msgpack.xz").unwrap();
    let mut data = vec![];
    XzDecoder::new(source_data_file)
        .read_to_end(&mut data)
        .expect("Neither a readable nor valid XZ compressed file error");
    let lines: Vec<Event> = data
        .lines()
        .map(|d| base64::decode(&d.unwrap()).unwrap())
        .enumerate()
        .map(|(i, d)| Event {
            id: i as u64,
            ingest_ns: i as u64,
            meta: HashMap::new(),
            value: EventValue::Raw(d),
            kind: None,
        })
        .collect();
    let l = lines.len();
    c.bench(
        "pipeline throughput (sql msgpack)",
        Benchmark::new("distsys", move |b| {
            b.iter_batched(
                || lines.clone(),
                |data| {
                    for e in data.into_iter() {
                        exec.enqueue("in", e).unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        })
        .throughput(Throughput::Elements(l as u32)),
    );
}

criterion_group!(
    benches,
    benchmark_sql,
    benchmark_sql_msgpack,
    benchmark_json,
    benchmark_msgpack
);
criterion_main!(benches);
