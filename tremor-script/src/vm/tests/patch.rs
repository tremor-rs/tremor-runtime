// Copyright 2020-2024, The Tremor Team
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
use super::*;

#[test]
fn patch_insert() -> Result<()> {
    let p = compile(false, r#"patch {} of  insert "foo" => 42 end"#)?;

    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            Const { idx: 0 },
            Record { size: 0 },
            LoadV1,
            Const { idx: 1 },
            String { size: 1 },
            TestRecortPresent,
            JumpFalse { dst: 10 },
            Const { idx: 2 },
            Error,
            Const { idx: 3 },
            RecordSet,
            SwapV1,
        ]
    );

    assert_eq!(
        run(&p)?,
        literal!({
            "foo": 42
        })
    );
    Ok(())
}

#[test]
fn patch_default_key() -> Result<()> {
    let p = compile(false, r#"patch {} of  default "foo" => 42 end"#)?;

    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            Const { idx: 0 },
            Record { size: 0 },
            LoadV1,
            Const { idx: 1 },
            String { size: 1 },
            TestRecortPresent,
            JumpTrue { dst: 10 },
            Const { idx: 2 },
            RecordSet,
            SwapV1,
        ]
    );

    assert_eq!(
        run(&p)?,
        literal!({
            "foo": 42
        })
    );
    Ok(())
}

#[test]
fn patch_default() -> Result<()> {
    let p = compile(
        false,
        r#"patch {"snot": 42} of  default  => {"snot": 23, "badger": 23, "cake": "cookie"} end"#,
    )?;

    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            Const { idx: 0 },
            String { size: 1 },
            Const { idx: 1 },
            Const { idx: 2 },
            Record { size: 1 },
            LoadV1,
            Const { idx: 0 },
            String { size: 1 },
            Const { idx: 3 },
            Const { idx: 4 },
            String { size: 1 },
            Const { idx: 3 },
            Const { idx: 5 },
            String { size: 1 },
            Const { idx: 6 },
            String { size: 1 },
            Const { idx: 2 },
            Record { size: 3 },
            SwapV1,
            TestRecordIsEmpty,
            SwapV1,
            JumpTrue { dst: 32 },
            RecordPop,
            TestRecortPresent,
            JumpTrue { dst: 29 },
            Swap,
            RecordSet,
            Jump { dst: 19 },
            Pop,
            Pop,
            Jump { dst: 19 },
            SwapV1,
        ]
    );

    assert_eq!(
        run(&p)?,
        literal!({
            "snot": 42,
            "badger": 23,
            "cake": "cookie",
        })
    );
    Ok(())
}

#[test]
fn patch_default_present() -> Result<()> {
    let p = compile(false, r#"patch {"foo":"bar"} of  default "foo" => 42 end"#)?;

    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            Const { idx: 0 },
            String { size: 1 },
            Const { idx: 1 },
            String { size: 1 },
            Const { idx: 2 },
            Record { size: 1 },
            LoadV1,
            Const { idx: 0 },
            String { size: 1 },
            TestRecortPresent,
            JumpTrue { dst: 14 },
            Const { idx: 3 },
            RecordSet,
            SwapV1,
        ]
    );

    assert_eq!(
        run(&p)?,
        literal!({
            "foo": "bar"
        })
    );
    Ok(())
}

#[test]
fn patch_insert_error() -> Result<()> {
    let p = compile(false, r#"patch {"foo":"bar"} of insert "foo" => 42 end"#)?;

    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            Const { idx: 0 },
            String { size: 1 },
            Const { idx: 1 },
            String { size: 1 },
            Const { idx: 2 },
            Record { size: 1 },
            LoadV1,
            Const { idx: 0 },
            String { size: 1 },
            TestRecortPresent,
            JumpFalse { dst: 14 },
            Const { idx: 3 },
            Error,
            Const { idx: 4 },
            RecordSet,
            SwapV1,
        ]
    );

    assert!(run(&p).is_err(),);
    Ok(())
}

#[test]
fn patch_update() -> Result<()> {
    let p = compile(false, r#"patch {"foo":"bar"} of update "foo" => 42 end"#)?;

    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            Const { idx: 0 },
            String { size: 1 },
            Const { idx: 1 },
            String { size: 1 },
            Const { idx: 2 },
            Record { size: 1 },
            LoadV1,
            Const { idx: 0 },
            String { size: 1 },
            TestRecortPresent,
            JumpTrue { dst: 14 },
            Const { idx: 3 },
            Error,
            Const { idx: 4 },
            RecordSet,
            SwapV1,
        ]
    );

    assert_eq!(
        run(&p)?,
        literal!({
            "foo": 42
        })
    );
    Ok(())
}

#[test]
fn patch_update_error() -> Result<()> {
    let p = compile(false, r#"patch {} of update "foo" => 42 end"#)?;

    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            Const { idx: 0 },
            Record { size: 0 },
            LoadV1,
            Const { idx: 1 },
            String { size: 1 },
            TestRecortPresent,
            JumpTrue { dst: 10 },
            Const { idx: 2 },
            Error,
            Const { idx: 3 },
            RecordSet,
            SwapV1,
        ]
    );

    assert!(run(&p).is_err(),);
    Ok(())
}

#[test]
fn patch_upsert_1() -> Result<()> {
    let p = compile(false, r#"patch {"foo":"bar"} of upsert "foo" => 42 end"#)?;

    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            Const { idx: 0 },
            String { size: 1 },
            Const { idx: 1 },
            String { size: 1 },
            Const { idx: 2 },
            Record { size: 1 },
            LoadV1,
            Const { idx: 0 },
            String { size: 1 },
            Const { idx: 3 },
            RecordSet,
            SwapV1,
        ]
    );

    assert_eq!(
        run(&p)?,
        literal!({
            "foo": 42
        })
    );
    Ok(())
}

#[test]
fn patch_upsert_2() -> Result<()> {
    let p = compile(false, r#"patch {} of upsert "foo" => 42 end"#)?;

    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            Const { idx: 0 },
            Record { size: 0 },
            LoadV1,
            Const { idx: 1 },
            String { size: 1 },
            Const { idx: 2 },
            RecordSet,
            SwapV1,
        ]
    );

    assert_eq!(
        run(&p)?,
        literal!({
            "foo": 42
        })
    );
    Ok(())
}

#[test]
fn patch_patch_patch() -> Result<()> {
    let p = compile(
        false,
        r#"patch patch {"foo":"bar"} of upsert "bar" => "baz" end of insert "baz" => 42 end"#,
    )?;

    println!("{p}");

    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            StoreV1,
            Const { idx: 0 },
            String { size: 1 },
            Const { idx: 1 },
            String { size: 1 },
            Const { idx: 2 },
            Record { size: 1 },
            LoadV1,
            Const { idx: 1 },
            String { size: 1 },
            Const { idx: 3 },
            String { size: 1 },
            RecordSet,
            SwapV1,
            LoadV1,
            Const { idx: 3 },
            String { size: 1 },
            TestRecortPresent,
            JumpFalse { dst: 22 },
            Const { idx: 4 },
            Error,
            Const { idx: 5 },
            RecordSet,
            SwapV1,
        ]
    );

    assert_eq!(
        run(&p)?,
        literal!({
            "foo": "bar",
            "bar": "baz",
            "baz": 42
        })
    );
    Ok(())
}

#[test]
fn patch_merge() -> Result<()> {
    let p = compile(
        false,
        r#"patch {"snot":"badger"} of merge => {"badger":"snot"} end"#,
    )?;

    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            Const { idx: 0 },
            String { size: 1 },
            Const { idx: 1 },
            String { size: 1 },
            Const { idx: 2 },
            Record { size: 1 },
            LoadV1,
            Const { idx: 1 },
            String { size: 1 },
            Const { idx: 0 },
            String { size: 1 },
            Const { idx: 2 },
            Record { size: 1 },
            RecordMerge,
            SwapV1,
        ]
    );

    assert_eq!(
        run(&p)?,
        literal!({
            "snot": "badger",
            "badger": "snot"
        })
    );
    Ok(())
}

#[test]
fn patch_merge_key() -> Result<()> {
    let p = compile(
        false,
        r#"(patch event of merge "object" => {"badger":"snot"} end).object"#,
    )?;

    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            LoadEvent,
            LoadV1,
            Const { idx: 0 },
            String { size: 1 },
            Const { idx: 1 },
            String { size: 1 },
            Const { idx: 2 },
            Record { size: 1 },
            Const { idx: 3 },
            String { size: 1 },
            RecordMergeKey,
            SwapV1,
            GetKey { key: 0 },
        ]
    );

    assert_eq!(
        run(&p)?,
        literal!({
            "a": [1, 2, 3],
            "b": 2,
            "c": 3,
            "o": {
                "d": 4,
                "e": 5,
            },
            "badger": "snot",
        })
    );
    Ok(())
}
