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
use tremor_value::literal;

use super::{Op::*, *};
use crate::{
    arena::Arena,
    ast::{optimizer::Optimizer, BinOpKind::*, Helper, UnaryOpKind},
    lexer::Lexer,
    parser::g::ScriptParser,
    registry, AggrRegistry, Compiler,
};

mod match_stmt;
mod patch;

fn compile(optimize: bool, src: &str) -> Result<Program<'static>> {
    let mut compiler: Compiler = Compiler::new();
    let reg = registry::registry();
    let fake_aggr_reg = AggrRegistry::default();
    let (aid, src) = Arena::insert(src)?;

    let tokens = Lexer::new(src, aid).collect::<Result<Vec<_>>>()?;
    let filtered_tokens = tokens.into_iter().filter(|t| !t.value.is_ignorable());

    let script_raw = ScriptParser::new().parse(filtered_tokens)?;
    let mut helper = Helper::new(&reg, &fake_aggr_reg);
    // helper.consts.args = args.clone_static();
    let mut script = script_raw.up_script(&mut helper)?;
    if optimize {
        Optimizer::new(&helper).walk_script(&mut script)?;
    }
    let p = compiler.compile(script)?;
    println!("{p}");
    Ok(p)
}

fn run<'v>(p: &Program<'v>) -> Result<Value<'v>> {
    let vm = Vm::new();
    let mut event = literal!({
        "int": 42,
        "float": 42.0,
        "bool": true,
        "null": null,
        "string": "string",
        "array": [1, 2, 3],
        "object": {
            "a": [1, 2, 3],
            "b": 2,
            "c": 3,
            "o": {
                "d": 4,
                "e": 5,
            }
        }
    });
    if let Return::Emit { value, .. } = vm.run(&mut event, p)? {
        Ok(value)
    } else {
        Err("Expected Emit".into())
    }
}

// We are using 32 bytes for all indexes and other arguments, this allows us
// to keep the opcodes to 64 bytes total and in result fit in a single register
// with the lower part of the register being the argument to the op code
// and the upper part being the opcode itself.
// We ensure this by havbinga a gest for the size of the opcodes
#[test]
fn op_size() {
    assert_eq!(std::mem::size_of::<Op>(), 8);
}
#[test]
fn simple() -> Result<()> {
    let p = compile(false, "42")?;

    assert_eq!(p.opcodes, [Const { idx: 0 }]);
    assert_eq!(run(&p)?, 42);
    Ok(())
}

#[test]
fn simple_add() -> Result<()> {
    let p = compile(false, "42 + 42")?;

    assert_eq!(
        p.opcodes,
        [
            Const { idx: 0 },
            Const { idx: 0 },
            Binary {
                op: crate::ast::BinOpKind::Add
            }
        ]
    );
    assert_eq!(run(&p)?, 84);

    Ok(())
}

#[test]
fn simple_add_sub() -> Result<()> {
    let p = compile(false, "42 + 43 - 44")?;

    assert_eq!(
        p.opcodes,
        [
            Const { idx: 0 },
            Const { idx: 1 },
            Binary {
                op: crate::ast::BinOpKind::Add
            },
            Const { idx: 2 },
            Binary {
                op: crate::ast::BinOpKind::Sub
            }
        ]
    );
    assert_eq!(run(&p)?, 41);

    Ok(())
}

#[test]
fn logical_and() -> Result<()> {
    let p = compile(false, "true and false")?;

    assert_eq!(
        p.opcodes,
        [
            Const { idx: 0 },
            LoadRB,
            JumpFalse { dst: 5 },
            Const { idx: 1 },
            LoadRB,
            StoreRB
        ]
    );
    assert_eq!(run(&p)?, false);
    Ok(())
}

#[test]
fn logical_or() -> Result<()> {
    let p = compile(false, "true or false")?;

    println!("{p}");
    assert_eq!(
        p.opcodes,
        [
            Const { idx: 0 },
            LoadRB,
            JumpTrue { dst: 5 },
            Const { idx: 1 },
            LoadRB,
            StoreRB,
        ]
    );
    assert_eq!(run(&p)?, true);
    Ok(())
}

#[test]
fn logical_not() -> Result<()> {
    let p = compile(false, "not true")?;

    assert_eq!(
        p.opcodes,
        [
            Const { idx: 0 },
            Unary {
                op: UnaryOpKind::Not
            }
        ]
    );
    assert_eq!(run(&p)?, false);
    Ok(())
}

#[test]
fn simple_eq() -> Result<()> {
    let p = compile(false, "42 == 42")?;

    assert_eq!(
        p.opcodes,
        [
            Const { idx: 0 },
            Const { idx: 0 },
            Binary {
                op: crate::ast::BinOpKind::Eq
            }
        ]
    );
    assert_eq!(run(&p)?, true);
    Ok(())
}

#[test]
fn merge() -> Result<()> {
    let p = compile(false, r#"merge {"snot":"badger"} of {"badger":"snot"} end"#)?;

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
fn array_index_fast() -> Result<()> {
    let p = compile(false, "[1,2,3][1]")?;

    assert_eq!(
        p.opcodes,
        &[
            Const { idx: 0 },
            Const { idx: 1 },
            Const { idx: 2 },
            Const { idx: 3 },
            Array { size: 3 },
            IndexFast { idx: 1 },
        ]
    );

    assert_eq!(run(&p)?, 2);
    Ok(())
}

#[test]
fn array_index() -> Result<()> {
    let p = compile(false, "[1,2,3][1+1]")?;

    assert_eq!(
        p.opcodes,
        &[
            Const { idx: 0 },
            Const { idx: 1 },
            Const { idx: 2 },
            Const { idx: 3 },
            Array { size: 3 },
            Const { idx: 0 },
            Const { idx: 0 },
            Binary { op: Add },
            Get,
        ]
    );

    assert_eq!(run(&p)?, 3);
    Ok(())
}

#[test]
fn record_key() -> Result<()> {
    let p = compile(false, r#"{"snot":"badger"}.snot"#)?;

    assert_eq!(
        p.opcodes,
        &[
            Const { idx: 0 },
            String { size: 1 },
            Const { idx: 1 },
            String { size: 1 },
            Const { idx: 2 },
            Record { size: 1 },
            GetKey { key: 0 },
        ]
    );

    assert_eq!(run(&p)?, "badger");
    Ok(())
}

#[test]
fn record_path() -> Result<()> {
    let p = compile(false, r#"{"snot":"badger"}["snot"]"#)?;

    assert_eq!(
        p.opcodes,
        &[
            Const { idx: 0 },
            String { size: 1 },
            Const { idx: 1 },
            String { size: 1 },
            Const { idx: 2 },
            Record { size: 1 },
            Const { idx: 0 },
            String { size: 1 },
            Get,
        ]
    );

    assert_eq!(run(&p)?, "badger");
    Ok(())
}

#[test]
fn array_range() -> Result<()> {
    let p = compile(false, "[1,2,3][0:2]")?;

    assert_eq!(
        p.opcodes,
        &[
            Const { idx: 0 },
            Const { idx: 1 },
            Const { idx: 2 },
            Const { idx: 3 },
            Array { size: 3 },
            Const { idx: 4 },
            Const { idx: 1 },
            Range
        ]
    );

    let r = run(&p)?;
    assert_eq!(r[0], 1);
    assert_eq!(r[1], 2);
    Ok(())
}

#[test]
fn event_key() -> Result<()> {
    let p = compile(false, "event.int")?;

    assert_eq!(p.opcodes, &[LoadEvent, GetKey { key: 0 },]);

    assert_eq!(run(&p)?, 42);
    Ok(())
}

#[test]
fn event_nested() -> Result<()> {
    let p = compile(false, "event.object.a")?;

    assert_eq!(
        p.opcodes,
        &[LoadEvent, GetKey { key: 0 }, GetKey { key: 1 }]
    );

    assert_eq!(run(&p)?, literal!([1, 2, 3]));
    Ok(())
}

#[test]
fn event_nested_index() -> Result<()> {
    let p = compile(false, "event.array[1]")?;

    assert_eq!(
        p.opcodes,
        &[LoadEvent, GetKey { key: 0 }, IndexFast { idx: 1 },]
    );

    assert_eq!(run(&p)?, 2);
    Ok(())
}

#[test]
fn test_local() -> Result<()> {
    let p = compile(false, "let a = 42; a")?;

    assert_eq!(p.max_locals, 0);
    assert_eq!(
        p.opcodes,
        &[
            Const { idx: 0 },
            StoreLocal {
                idx: 0,
                elements: 0
            },
            LoadLocal { idx: 0 },
        ]
    );

    assert_eq!(run(&p)?, 42);
    Ok(())
}

#[test]
fn test_local_event() -> Result<()> {
    let p = compile(false, "let a = event; a.int")?;

    assert_eq!(p.max_locals, 0);
    assert_eq!(
        p.opcodes,
        &[
            LoadEvent,
            StoreLocal {
                idx: 0,
                elements: 0
            },
            LoadLocal { idx: 0 },
            GetKey { key: 0 },
        ]
    );

    assert_eq!(run(&p)?, 42);
    Ok(())
}

#[test]
fn test_event_assign_nested() -> Result<()> {
    let p = compile(false, "let event.string = \"snot\"; event.string")?;

    assert_eq!(p.max_locals, 0);
    assert_eq!(
        p.opcodes,
        &[
            Const { idx: 0 },
            String { size: 1 },
            Const { idx: 1 },
            StoreEvent { elements: 1 },
            LoadEvent,
            GetKey { key: 0 },
        ]
    );

    assert_eq!(run(&p)?, "snot");
    Ok(())
}

#[test]
fn test_event_assign_nested_new() -> Result<()> {
    let p = compile(false, "let event.badger = \"snot\"; event.badger")?;

    assert_eq!(p.max_locals, 0);
    assert_eq!(
        p.opcodes,
        &[
            Const { idx: 0 },
            String { size: 1 },
            Const { idx: 1 },
            StoreEvent { elements: 1 },
            LoadEvent,
            GetKey { key: 0 },
        ]
    );

    assert_eq!(run(&p)?, "snot");
    Ok(())
}

#[test]
fn test_event_array_assign_nested() -> Result<()> {
    let p = compile(false, "let event.array[1] = 42; event.array")?;

    assert_eq!(p.max_locals, 0);
    assert_eq!(
        p.opcodes,
        &[
            Const { idx: 0 },
            Const { idx: 1 },
            Const { idx: 2 },
            StoreEvent { elements: 2 },
            LoadEvent,
            GetKey { key: 0 },
        ]
    );

    assert_eq!(run(&p)?, literal!([1, 42, 3]));
    Ok(())
}

#[test]
fn test_local_assign_nested() -> Result<()> {
    let p = compile(false, "let a = {}; let a.b = 1; a.b")?;

    assert_eq!(p.max_locals, 0);
    assert_eq!(
        p.opcodes,
        &[
            Const { idx: 0 },
            Record { size: 0 },
            StoreLocal {
                elements: 0,
                idx: 0,
            },
            Const { idx: 1 },
            Const { idx: 2 },
            StoreLocal {
                elements: 1,
                idx: 0,
            },
            LoadLocal { idx: 0 },
            GetKey { key: 0 },
        ]
    );

    assert_eq!(run(&p)?, 1);
    Ok(())
}

#[test]
fn test_local_array_assign_nested() -> Result<()> {
    let p = compile(false, "let a = [1,2]; let a[0]=-1; a")?;

    assert_eq!(p.max_locals, 0);
    assert_eq!(
        p.opcodes,
        &[
            Const { idx: 0 },
            Const { idx: 1 },
            Const { idx: 2 },
            Array { size: 2 },
            StoreLocal {
                elements: 0,
                idx: 0,
            },
            Const { idx: 0 },
            Unary {
                op: UnaryOpKind::Minus,
            },
            Const { idx: 3 },
            StoreLocal {
                elements: 1,
                idx: 0,
            },
            LoadLocal { idx: 0 },
        ]
    );

    assert_eq!(run(&p)?, literal!([-1, 2]));
    Ok(())
}
