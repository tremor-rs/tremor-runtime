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
