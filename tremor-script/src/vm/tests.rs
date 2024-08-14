use tremor_value::literal;

use super::{Op::*, *};
use crate::{
    arena::Arena,
    ast::{optimizer::Optimizer, BinOpKind::*, Helper, UnaryOpKind},
    lexer::Lexer,
    parser::g::ScriptParser,
    registry, AggrRegistry, Compiler,
};

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
            "a": 1,
            "b": 2,
            "c": 3
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
            "a": 1,
            "b": 2,
            "c": 3,
            "badger": "snot",
        })
    );
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

    assert_eq!(run(&p)?, 1);
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

#[test]
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
            JumpFalse { dst: 26 },
            InspectLen,
            Const { idx: 3 },
            Binary { op: Eq },
            LoadRB,
            JumpFalse { dst: 26 },
            CopyV1,
            ArrayReverse,
            ArrayPop,
            Const { idx: 0 },
            Binary { op: Eq },
            LoadRB,
            JumpFalse { dst: 25 },
            ArrayPop,
            Const { idx: 4 },
            Binary { op: Eq },
            LoadRB,
            JumpFalse { dst: 25 },
            Pop,
            JumpFalse { dst: 29 },
            Const { idx: 4 },
            Jump { dst: 81 },
            TestIsArray,
            JumpFalse { dst: 54 },
            InspectLen,
            Const { idx: 5 },
            Binary { op: Eq },
            LoadRB,
            JumpFalse { dst: 54 },
            CopyV1,
            ArrayReverse,
            ArrayPop,
            Const { idx: 0 },
            Binary { op: Eq },
            LoadRB,
            JumpFalse { dst: 53 },
            ArrayPop,
            Const { idx: 1 },
            Binary { op: Eq },
            LoadRB,
            JumpFalse { dst: 53 },
            ArrayPop,
            Const { idx: 6 },
            Binary { op: Eq },
            LoadRB,
            JumpFalse { dst: 53 },
            Pop,
            JumpFalse { dst: 57 },
            Const { idx: 6 },
            Jump { dst: 81 },
            TestIsArray,
            JumpFalse { dst: 77 },
            InspectLen,
            Const { idx: 3 },
            Binary { op: Eq },
            LoadRB,
            JumpFalse { dst: 77 },
            CopyV1,
            ArrayReverse,
            ArrayPop,
            Const { idx: 0 },
            Binary { op: Eq },
            LoadRB,
            JumpFalse { dst: 76 },
            ArrayPop,
            Const { idx: 1 },
            Binary { op: Eq },
            LoadRB,
            JumpFalse { dst: 76 },
            Pop,
            JumpFalse { dst: 80 },
            Const { idx: 0 },
            Jump { dst: 81 },
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
