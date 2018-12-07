#[macro_use]
extern crate criterion;

extern crate mimir;

extern crate serde_json;

use criterion::Criterion;

use mimir::*;

use serde_json::Value;

/*
    The following method (bench) provides a ballpark number for parsing and evaluating rule/s.
    It is not meant to be a reflection of the execution in the tremor pipeline.
*/
fn bench(r: &Rules<u32>, json: &str) {
    let vals: Value = serde_json::from_str(json).unwrap();
    let _ = r.eval_first_wins(&vals);
}

fn bm1(c: &mut Criterion) {
    let mut r = Rules::default();
    r.add_rule(0, "key1:g\"da?a\"").unwrap();
    let json = r#"{"key1": "data"}"#;
    c.bench_function("glob da?a", move |b| b.iter(|| bench(&r, json)));
}

fn bm1_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    r.add_rule(0, "key1:g\"da?a\"").unwrap();
    let json = r#"{"key1": "data"}"#;
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("glob da?a nojsonparse", move |b| {
        b.iter(|| r.eval_first_wins(&val))
    });
}

fn bm2(c: &mut Criterion) {
    let mut r = Rules::default();
    r.add_rule(0, "key1:\"data\"").unwrap();
    let json = r#"{"key1": "data3"}"#;
    c.bench_function("contains data", move |b| b.iter(|| bench(&r, json)));
}

fn bm2_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    r.add_rule(0, "key1:\"data\"").unwrap();
    let json = r#"{"key1": "data3"}"#;
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("contains data nojsonparse", move |b| {
        b.iter(|| r.eval_first_wins(&val))
    });
}

fn bm3(c: &mut Criterion) {
    let mut r = Rules::default();
    r.add_rule(0, "key1:\"data\"").unwrap();
    let json = r#"{"key1": "data3"}"#;
    c.bench_function("contains data", move |b| b.iter(|| bench(&r, json)));
}

fn bm3_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    r.add_rule(0, "key1:\"data\"").unwrap();
    let json = r#"{"key1": "data3"}"#;
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("contains data nojsonparse", move |b| {
        b.iter(|| r.eval_first_wins(&val))
    });
}

fn bm4(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key1": "data1"}"#;
    r.add_rule(0, "key1=\"data1\"").unwrap();
    c.bench_function("equals data1", move |b| b.iter(|| bench(&r, json)));
}

fn bm4_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key1": "data1"}"#;
    r.add_rule(0, "key1=\"data1\"").unwrap();
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("equals data1 nojsonparse", move |b| {
        b.iter(|| r.eval_first_wins(&val))
    });
}

fn bm5(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key1": {"sub1": "data1"}}"#;
    r.add_rule(0, "key1.sub1=\"data1\"").unwrap();
    c.bench_function("equals subkey data1", move |b| b.iter(|| bench(&r, json)));
}

fn bm5_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key1": {"sub1": "data1"}}"#;
    r.add_rule(0, "key1.sub1=\"data1\"").unwrap();
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("equals subkey data1 nojsonparse", move |b| {
        b.iter(|| r.eval_first_wins(&val))
    });
}

fn bm6(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key1": {"sub1": "data1"}}"#;
    r.add_rule(0, "key1.sub1:\"dat\"").unwrap();
    c.bench_function("contains subkey dat", move |b| b.iter(|| bench(&r, json)));
}

fn bm6_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key1": {"sub1": "data1"}}"#;
    r.add_rule(0, "key1.sub1:\"dat\"").unwrap();
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("contains subkey dat nojsonparse", move |b| {
        b.iter(|| r.eval_first_wins(&val))
    });
}

fn bm7(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{
            "key1": {
                    "subkey1": "data1"
                },
                "key2": {
                    "subkey2": "data2"
                 },
                "key3": "data3"
             }"#;
    r.add_rule(0, "key1.subkey1:\"data1\" key3:\"data3\" or (key1.subkey1:\"dat\" and key2.subkey2=\"data2\")").unwrap();
    c.bench_function("multi-component", move |b| b.iter(|| bench(&r, json)));
}

fn bm7_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{
            "key1": {
                    "subkey1": "data1"
                },
                "key2": {
                    "subkey2": "data2"
                 },
                "key3": "data3"
             }"#;
    r.add_rule(0, "key1.subkey1:\"data1\" key3:\"data3\" or (key1.subkey1:\"dat\" and key2.subkey2=\"data2\")").unwrap();
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("multi-component", move |b| {
        b.iter(|| r.eval_first_wins(&val))
    });
}

fn bm8(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":5}"#;
    r.add_rule(0, "key=5").unwrap();
    c.bench_function("equals int", move |b| b.iter(|| bench(&r, json)));
}

fn bm8_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":5}"#;
    r.add_rule(0, "key=5").unwrap();
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("equals int nojsonparse", move |b| {
        b.iter(|| r.eval_first_wins(&val))
    });
}

fn bm9(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":5}"#;
    r.add_rule(0, "key>1").unwrap();
    r.add_rule(1, "key>4").unwrap();
    c.bench_function("gt int", move |b| b.iter(|| bench(&r, json)));
}

fn bm9_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":5}"#;
    r.add_rule(0, "key>1").unwrap();
    r.add_rule(1, "key>4").unwrap();
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("gt int", move |b| b.iter(|| r.eval_first_wins(&val)));
}

fn bm10(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":5}"#;
    r.add_rule(0, "key>-6").unwrap();
    c.bench_function("gt neg int", move |b| b.iter(|| bench(&r, json)));
}

fn bm10_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":5}"#;
    r.add_rule(0, "key>-6").unwrap();
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("gt neg int nojsonparse", move |b| {
        b.iter(|| r.eval_first_wins(&val))
    });
}

fn bm11(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":5}"#;
    r.add_rule(0, "key<10").unwrap();
    r.add_rule(1, "key<9").unwrap();
    c.bench_function("lt int", move |b| b.iter(|| bench(&r, json)));
}

fn bm11_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":5}"#;
    r.add_rule(0, "key<10").unwrap();
    r.add_rule(1, "key<9").unwrap();
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("lt int nojsonparse", move |b| {
        b.iter(|| r.eval_first_wins(&val))
    });
}

fn bm12(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":5.0}"#;
    r.add_rule(0, "key<10.0").unwrap();
    r.add_rule(1, "key<9.0").unwrap();
    c.bench_function("lt float", move |b| b.iter(|| bench(&r, json)));
}

fn bm12_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":5.0}"#;
    r.add_rule(0, "key<10.0").unwrap();
    r.add_rule(1, "key<9.0").unwrap();
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("lt float nojsonparse", move |b| {
        b.iter(|| r.eval_first_wins(&val))
    });
}

fn bm13(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":5}"#;
    r.add_rule(0, "key<=5").unwrap();
    r.add_rule(1, "key<=11").unwrap();
    c.bench_function("ltoe int", move |b| b.iter(|| bench(&r, json)));
}

fn bm13_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":5}"#;
    r.add_rule(0, "key<=5").unwrap();
    r.add_rule(1, "key<=11").unwrap();
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("ltoe int nojsonparse", move |b| {
        b.iter(|| r.eval_first_wins(&val))
    });
}

fn bm14(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":5}"#;
    r.add_rule(0, "key >= 3").unwrap();
    r.add_rule(1, "key >= 4").unwrap();
    c.bench_function("gtoe int", move |b| b.iter(|| bench(&r, json)));
}

fn bm14_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":5}"#;
    r.add_rule(0, "key >= 3").unwrap();
    r.add_rule(1, "key >= 4").unwrap();
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("gtoe int nojsonparse", move |b| {
        b.iter(|| r.eval_first_wins(&val))
    });
}

fn bm15(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":5.0}"#;
    r.add_rule(0, "key >= 3.5").unwrap();
    r.add_rule(1, "key >= 4.5").unwrap();
    c.bench_function("gtoe float", move |b| b.iter(|| bench(&r, json)));
}

fn bm15_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":5.0}"#;
    r.add_rule(0, "key >= 3.5").unwrap();
    r.add_rule(1, "key >= 4.5").unwrap();
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("gtoe float nojsonparse", move |b| {
        b.iter(|| r.eval_first_wins(&val))
    });
}

fn bm16(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":"data"}"#;
    r.add_rule(0, "key:/d.*/").unwrap();
    c.bench_function("regex d.*", move |b| b.iter(|| bench(&r, json)));
}

fn bm16_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":"data"}"#;
    r.add_rule(0, "key:/d.*/").unwrap();
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("regex d.* nojsonparse", move |b| {
        b.iter(|| r.eval_first_wins(&val))
    });
}

fn bm17(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":"data"}"#;
    r.add_rule(0, "key:/e.*/").unwrap();
    c.bench_function("regex e.* false", move |b| b.iter(|| bench(&r, json)));
}

fn bm17_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":"data"}"#;
    r.add_rule(0, "key:/e.*/").unwrap();
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("regex e.* false nojsonparse", move |b| {
        b.iter(|| r.eval_first_wins(&val))
    });
}

fn bm18(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":"data"}"#;
    r.add_rule(0, "NOT key:/d.*/").unwrap();
    c.bench_function("regex not d.*", move |b| b.iter(|| bench(&r, json)));
}

fn bm18_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":"data"}"#;
    r.add_rule(0, "NOT key:/d.*/").unwrap();
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("regex not d.* nojsonparse", move |b| {
        b.iter(|| r.eval_first_wins(&val))
    });
}

fn bm19(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":"\\/"}"#;
    r.add_rule(0, r#"key:/\\//"#).unwrap();
    c.bench_function("regex \\ parse", move |b| b.iter(|| bench(&r, json)));
}

fn bm19_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":"\\/"}"#;
    r.add_rule(0, r#"key:/\\//"#).unwrap();
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("regex \\ parse nojsonparse", move |b| {
        b.iter(|| r.eval_first_wins(&val))
    });
}

fn bm20(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{
            "key1": {
                    "subkey1": "data1"
                },
                "key2": {
                    "subkey2": "data2"
                 },
                "key3": "data3"
             }"#;
    r.add_rule(0, "!(key1.subkey1:\"data1\" NOT key3:\"data3\" or NOT (key1.subkey1:\"dat\" and key2.subkey2=\"data2\"))").unwrap();
    c.bench_function("compound", move |b| b.iter(|| bench(&r, json)));
}

fn bm20_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{
            "key1": {
                    "subkey1": "data1"
                },
                "key2": {
                    "subkey2": "data2"
                 },
                "key3": "data3"
             }"#;
    r.add_rule(0, "!(key1.subkey1:\"data1\" NOT key3:\"data3\" or NOT (key1.subkey1:\"dat\" and key2.subkey2=\"data2\"))").unwrap();
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("compound", move |b| b.iter(|| r.eval_first_wins(&val)));
}

fn bm21(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":"data"}"#;
    r.add_rule(0, "key:[\"foo\", \"data\", \"bar\"]").unwrap();
    c.bench_function("inline str list", move |b| b.iter(|| bench(&r, json)));
}

fn bm21_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":"data"}"#;
    r.add_rule(0, "key:[\"foo\", \"data\", \"bar\"]").unwrap();
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("inline str list nojsonparse", move |b| {
        b.iter(|| r.eval_first_wins(&val))
    });
}

fn bm22(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":4}"#;
    r.add_rule(0, "key:[3, 4, 5]").unwrap();
    c.bench_function("inline int list", move |b| b.iter(|| bench(&r, json)));
}

fn bm22_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":4}"#;
    r.add_rule(0, "key:[3, 4, 5]").unwrap();
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("inline int list nojsonparse", move |b| {
        b.iter(|| r.eval_first_wins(&val))
    });
}

fn bm23(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":4.1}"#;
    r.add_rule(0, "key:[3.1, 4.1, 5.1]").unwrap();
    c.bench_function("inline float list", move |b| b.iter(|| bench(&r, json)));
}

fn bm23_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":4.1}"#;
    r.add_rule(0, "key:[3.1, 4.1, 5.1]").unwrap();
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("inline float list", move |b| {
        b.iter(|| r.eval_first_wins(&val))
    });
}

fn bm24(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":["v1", "v2", "v3"]}"#;
    r.add_rule(0, "key:\"v2\"").unwrap();
    c.bench_function("array str json", move |b| b.iter(|| bench(&r, json)));
}

fn bm24_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":["v1", "v2", "v3"]}"#;
    r.add_rule(0, "key:\"v2\"").unwrap();
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("array str json", move |b| {
        b.iter(|| r.eval_first_wins(&val))
    });
}

fn bm25(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":[3, 4, 5]}"#;
    r.add_rule(0, "key:4").unwrap();
    c.bench_function("array int json", move |b| b.iter(|| bench(&r, json)));
}

fn bm25_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":[3, 4, 5]}"#;
    r.add_rule(0, "key:4").unwrap();
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("array int json", move |b| {
        b.iter(|| r.eval_first_wins(&val))
    });
}

fn bm26(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":[3.1, 4.1, 5.1]}"#;
    r.add_rule(0, "key:4.1").unwrap();
    c.bench_function("array float json", move |b| b.iter(|| bench(&r, json)));
}

fn bm26_nojsonparse(c: &mut Criterion) {
    let mut r = Rules::default();
    let json = r#"{"key":[3.1, 4.1, 5.1]}"#;
    r.add_rule(0, "key:4.1").unwrap();
    let val = serde_json::from_str(json).unwrap();
    c.bench_function("array float json nojsonparse", move |b| {
        b.iter(|| r.eval_first_wins(&val))
    });
}

criterion_group!(
    benches,
    bm1,
    bm2,
    bm3,
    bm4,
    bm5,
    bm6,
    bm7,
    bm8,
    bm9,
    bm10,
    bm11,
    bm12,
    bm13,
    bm14,
    bm15,
    bm16,
    bm17,
    bm18,
    bm19,
    bm20,
    bm21,
    bm22,
    bm23,
    bm24,
    bm25,
    bm26,
    bm1_nojsonparse,
    bm2_nojsonparse,
    bm3_nojsonparse,
    bm4_nojsonparse,
    bm5_nojsonparse,
    bm6_nojsonparse,
    bm7_nojsonparse,
    bm8_nojsonparse,
    bm9_nojsonparse,
    bm10_nojsonparse,
    bm11_nojsonparse,
    bm12_nojsonparse,
    bm13_nojsonparse,
    bm14_nojsonparse,
    bm15_nojsonparse,
    bm16_nojsonparse,
    bm17_nojsonparse,
    bm18_nojsonparse,
    bm19_nojsonparse,
    bm20_nojsonparse,
    bm21_nojsonparse,
    bm22_nojsonparse,
    bm23_nojsonparse,
    bm24_nojsonparse,
    bm25_nojsonparse,
    bm26_nojsonparse,
);
criterion_main!(benches);
