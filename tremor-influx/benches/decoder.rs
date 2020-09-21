// Copyright 2020, The Tremor Team
//
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

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tremor_influx::decode;

pub fn influx_benchmark(c: &mut Criterion) {
    c.bench_function("simple-value", |b| {
        b.iter(|| {
            black_box(decode::<simd_json::BorrowedValue>(
                "weather,location=us-midwest,season=summer temperature=82 1465839830100400200",
                0,
            ))
        })
    });
    c.bench_function("int-value", |b| {
        b.iter(|| {
            black_box(decode::<simd_json::BorrowedValue>(
                "weather temperature_i=82i 1465839830100400200",
                0,
            ))
        })
    });
    c.bench_function("str-value", |b| {
        b.iter(|| {
            black_box(decode::<simd_json::BorrowedValue>(
                r#"weather,location=us-midwest temperature_str="too warm" 1465839830100400200"#,
                0,
            ))
        })
    });
    c.bench_function("escaped-value", |b| {
        b.iter(|| {
            black_box(decode::<simd_json::BorrowedValue>(
                r#"weather,location=us-midwest temperature_str="too hot\\\\\cold" 1465839830100400206"#,
                0,
            ))
        })
    });
}

criterion_group!(benches, influx_benchmark);
criterion_main!(benches);
