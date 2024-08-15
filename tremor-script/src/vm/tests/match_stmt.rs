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
fn test_match_if_else() -> Result<()> {
    let p = compile(false, "match event.int of case 42 => 42 case _ => 0 end")?;

    println!("{p}");
    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            LoadEvent,
            GetKey { key: 0 },
            LoadV1,
            Const { idx: 0 },
            TestEq,
            JumpFalse { dst: 9 },
            Const { idx: 0 },
            Jump { dst: 10 },
            Const { idx: 1 },
            LoadV1,
            SwapV1,
        ]
    );

    assert_eq!(run(&p)?, 42);
    Ok(())
}

#[test]
fn test_match_record_type() -> Result<()> {
    let p = compile(
        false,
        "match event.object of case 42 => 42 case %{} => \"record\" case _ => 0 end",
    )?;

    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            LoadEvent,
            GetKey { key: 0 },
            LoadV1,
            Const { idx: 0 },
            TestEq,
            JumpFalse { dst: 9 },
            Const { idx: 0 },
            Jump { dst: 16 },
            TestIsRecord,
            JumpFalse { dst: 11 },
            JumpFalse { dst: 15 },
            Const { idx: 1 },
            String { size: 1 },
            Jump { dst: 16 },
            Const { idx: 2 },
            LoadV1,
            SwapV1,
        ]
    );

    assert_eq!(run(&p)?, "record");
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn test_match_touple() -> Result<()> {
    let p = compile(
        false,
        r"
        match [42, 23] of 
          case %(42, 24) => 24
          case %(42, 23, 7) => 7
          case %(42, 23) => 42
          case _ => 0
        end",
    )?;

    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            Const { idx: 0 },
            Const { idx: 1 },
            Const { idx: 2 },
            Array { size: 2 },
            LoadV1,
            TestIsArray,
            JumpFalse { dst: 28 },
            InspectLen,
            Const { idx: 3 },
            Binary { op: Eq },
            LoadRB,
            JumpFalse { dst: 28 },
            CopyV1,
            ArrayReverse,
            ArrayPop,
            SwapV1,
            Const { idx: 0 },
            TestEq,
            LoadV1,
            JumpFalse { dst: 27 },
            ArrayPop,
            SwapV1,
            Const { idx: 4 },
            TestEq,
            LoadV1,
            JumpFalse { dst: 27 },
            Pop,
            JumpFalse { dst: 31 },
            Const { idx: 4 },
            Jump { dst: 88 },
            TestIsArray,
            JumpFalse { dst: 59 },
            InspectLen,
            Const { idx: 5 },
            Binary { op: Eq },
            LoadRB,
            JumpFalse { dst: 59 },
            CopyV1,
            ArrayReverse,
            ArrayPop,
            SwapV1,
            Const { idx: 0 },
            TestEq,
            LoadV1,
            JumpFalse { dst: 58 },
            ArrayPop,
            SwapV1,
            Const { idx: 1 },
            TestEq,
            LoadV1,
            JumpFalse { dst: 58 },
            ArrayPop,
            SwapV1,
            Const { idx: 6 },
            TestEq,
            LoadV1,
            JumpFalse { dst: 58 },
            Pop,
            JumpFalse { dst: 62 },
            Const { idx: 6 },
            Jump { dst: 88 },
            TestIsArray,
            JumpFalse { dst: 84 },
            InspectLen,
            Const { idx: 3 },
            Binary { op: Eq },
            LoadRB,
            JumpFalse { dst: 84 },
            CopyV1,
            ArrayReverse,
            ArrayPop,
            SwapV1,
            Const { idx: 0 },
            TestEq,
            LoadV1,
            JumpFalse { dst: 83 },
            ArrayPop,
            SwapV1,
            Const { idx: 1 },
            TestEq,
            LoadV1,
            JumpFalse { dst: 83 },
            Pop,
            JumpFalse { dst: 87 },
            Const { idx: 0 },
            Jump { dst: 88 },
            Const { idx: 7 },
            LoadV1,
            SwapV1,
        ]
    );

    assert_eq!(run(&p)?, 42);
    Ok(())
}
#[test]
fn test_match_search_tree() -> Result<()> {
    let p = compile(
        true,
        r"
        match event.int of
          case 1 => 1
          case 2 => 2
          case 3 => 3
          case 4 => 4
          case 5 => 5
          case 6 => 6
          case 7 => 7
          case 8 => 8
          case 9 => 9
          case 42 => 42
          case _ => 23
        end",
    )?;

    // assert_eq!(p.opcodes, &[]);

    assert_eq!(run(&p)?, 42);
    Ok(())
}

#[test]
fn test_match_assign() -> Result<()> {
    let p = compile(
        false,
        r"
        match 42 of 
          case 24 => 24
          case 7 => 7
          case a = 42 => a
          case _ => 0
        end",
    )?;

    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            Const { idx: 0 },
            LoadV1,
            Const { idx: 1 },
            TestEq,
            JumpFalse { dst: 8 },
            Const { idx: 1 },
            Jump { dst: 22 },
            Const { idx: 0 },
            TestEq,
            JumpFalse { dst: 13 },
            CopyV1,
            StoreLocal {
                elements: 0,
                idx: 0,
            },
            JumpFalse { dst: 16 },
            LoadLocal { idx: 0 },
            Jump { dst: 22 },
            Const { idx: 2 },
            TestEq,
            JumpFalse { dst: 21 },
            Const { idx: 2 },
            Jump { dst: 22 },
            Const { idx: 3 },
            LoadV1,
            SwapV1,
        ]
    );

    assert_eq!(run(&p)?, 42);
    Ok(())
}

#[test]
fn test_match_assign_nested() -> Result<()> {
    let p = compile(
        false,
        r"
        match [42] of 
          case a = %(42) => a
          case _ => 0
        end",
    )?;

    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            Const { idx: 0 },
            Const { idx: 1 },
            Array { size: 1 },
            LoadV1,
            TestIsArray,
            JumpFalse { dst: 21 },
            InspectLen,
            Const { idx: 2 },
            Binary { op: Eq },
            LoadRB,
            JumpFalse { dst: 21 },
            CopyV1,
            ArrayReverse,
            ArrayPop,
            SwapV1,
            Const { idx: 0 },
            TestEq,
            LoadV1,
            JumpFalse { dst: 20 },
            Pop,
            JumpFalse { dst: 24 },
            CopyV1,
            StoreLocal {
                elements: 0,
                idx: 0,
            },
            JumpFalse { dst: 27 },
            LoadLocal { idx: 0 },
            Jump { dst: 28 },
            Const { idx: 3 },
            LoadV1,
            SwapV1,
        ]
    );

    assert_eq!(run(&p)?, literal!([42]));
    Ok(())
}

#[test]
fn test_match_record_key_present() -> Result<()> {
    let p = compile(
        false,
        r"
        match event of 
          case %{present obj} => 1
          case %{present object} =>2
          case _ => 3
        end",
    )?;

    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            LoadEvent,
            LoadV1,
            TestIsRecord,
            JumpFalse { dst: 6 },
            TestRecordContainsKey { key: 0 },
            JumpFalse { dst: 9 },
            Const { idx: 0 },
            Jump { dst: 16 },
            TestIsRecord,
            JumpFalse { dst: 12 },
            TestRecordContainsKey { key: 1 },
            JumpFalse { dst: 15 },
            Const { idx: 1 },
            Jump { dst: 16 },
            Const { idx: 2 },
            LoadV1,
            SwapV1,
        ]
    );

    assert_eq!(run(&p)?, 2);
    Ok(())
}

#[test]
fn test_match_record_key_absent() -> Result<()> {
    let p = compile(
        false,
        r"
        match event of 
          case %{absent obj} => 1
          case %{absent object} =>2
          case _ => 3
        end",
    )?;

    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            LoadEvent,
            LoadV1,
            TestIsRecord,
            JumpFalse { dst: 7 },
            TestRecordContainsKey { key: 0 },
            NotRB,
            JumpFalse { dst: 10 },
            Const { idx: 0 },
            Jump { dst: 18 },
            TestIsRecord,
            JumpFalse { dst: 14 },
            TestRecordContainsKey { key: 1 },
            NotRB,
            JumpFalse { dst: 17 },
            Const { idx: 1 },
            Jump { dst: 18 },
            Const { idx: 2 },
            LoadV1,
            SwapV1,
        ]
    );

    assert_eq!(run(&p)?, 1);
    Ok(())
}

#[test]
fn test_record_binary() -> Result<()> {
    let p = compile(
        false,
        r"
        match event of 
          case %{ int > 23 } => 1
          case %{ bool == true } =>2
          case _ => 3
        end",
    )?;
    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            LoadEvent,
            LoadV1,
            TestIsRecord,
            JumpFalse { dst: 13 },
            TestRecordContainsKey { key: 0 },
            JumpFalse { dst: 13 },
            GetKeyRegV1 { key: 0 },
            SwapV1,
            Const { idx: 0 },
            TestGt,
            Pop,
            LoadV1,
            JumpFalse { dst: 16 },
            Const { idx: 1 },
            Jump { dst: 24 },
            TestRecordContainsKey { key: 1 },
            JumpFalse { dst: 23 },
            Const { idx: 2 },
            TestEq,
            JumpFalse { dst: 23 },
            Const { idx: 3 },
            Jump { dst: 24 },
            Const { idx: 4 },
            LoadV1,
            SwapV1,
        ]
    );
    assert_eq!(run(&p)?, 1);
    Ok(())
}

#[test]
fn test_record_tuple() -> Result<()> {
    let p = compile(
        false,
        r#"
        match {"array": [1], "int": 30} of 
          case %{ int < 23 } => 1
          case %{ array ~= %(1) } => 2
          case _ => 3
        end"#,
    )?;
    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            Const { idx: 0 },
            String { size: 1 },
            Const { idx: 1 },
            Const { idx: 2 },
            Array { size: 1 },
            Const { idx: 3 },
            String { size: 1 },
            Const { idx: 4 },
            Const { idx: 5 },
            Record { size: 2 },
            LoadV1,
            TestIsRecord,
            JumpFalse { dst: 22 },
            TestRecordContainsKey { key: 0 },
            JumpFalse { dst: 22 },
            GetKeyRegV1 { key: 0 },
            SwapV1,
            Const { idx: 6 },
            TestLt,
            Pop,
            LoadV1,
            JumpFalse { dst: 25 },
            Const { idx: 1 },
            Jump { dst: 52 },
            TestIsRecord,
            JumpFalse { dst: 48 },
            TestRecordContainsKey { key: 1 },
            JumpFalse { dst: 48 },
            GetKeyRegV1 { key: 1 },
            SwapV1,
            TestIsArray,
            JumpFalse { dst: 47 },
            InspectLen,
            Const { idx: 1 },
            Binary { op: Eq },
            LoadRB,
            JumpFalse { dst: 47 },
            CopyV1,
            ArrayReverse,
            ArrayPop,
            SwapV1,
            Const { idx: 1 },
            TestEq,
            LoadV1,
            JumpFalse { dst: 46 },
            Pop,
            LoadV1,
            JumpFalse { dst: 51 },
            Const { idx: 7 },
            Jump { dst: 52 },
            Const { idx: 8 },
            LoadV1,
            SwapV1,
        ]
    );
    assert_eq!(run(&p)?, 2);
    Ok(())
}
