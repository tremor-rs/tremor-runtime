use tremor_value::literal;

use super::{Op::*, *};
use crate::{
    arena::Arena,
    ast::{Helper, Script},
    lexer::Lexer,
    parser::g::ScriptParser,
    registry, AggrRegistry, Compiler,
};

fn parse(src: &str) -> Result<Script<'static>> {
    let reg = registry::registry();
    let fake_aggr_reg = AggrRegistry::default();
    let (aid, src) = Arena::insert(src)?;

    let tokens = Lexer::new(src, aid).collect::<Result<Vec<_>>>()?;
    let filtered_tokens = tokens.into_iter().filter(|t| !t.value.is_ignorable());

    let script_raw = ScriptParser::new().parse(filtered_tokens)?;
    let mut helper = Helper::new(&reg, &fake_aggr_reg);
    // helper.consts.args = args.clone_static();
    let script = script_raw.up_script(&mut helper)?;
    // Optimizer::new(&helper).walk_script(&mut script)?;
    Ok(script)
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
    let mut compiler: Compiler = Compiler::new();

    let script = parse("42")?;
    let p = compiler.compile(script)?;

    assert_eq!(p.consts.len(), 1);
    assert_eq!(p.opcodes, [Const { idx: 0 }]);
    assert_eq!(run(&p)?, 42);
    Ok(())
}

#[test]
fn simple_add() -> Result<()> {
    let mut compiler: Compiler = Compiler::new();

    let script = parse("42 + 42")?;
    let p = compiler.compile(script)?;

    assert_eq!(p.consts.len(), 1);
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
    let mut compiler: Compiler = Compiler::new();

    let script = parse("42 + 43 - 44")?;
    let p = compiler.compile(script)?;

    assert_eq!(p.consts.len(), 3);
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
    let mut compiler: Compiler = Compiler::new();

    let script = parse("true and false")?;
    let p = compiler.compile(script)?;

    assert_eq!(p.consts.len(), 2);
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
    let mut compiler: Compiler = Compiler::new();

    let script = parse("true or false")?;
    let p = compiler.compile(script)?;

    println!("{p}");
    assert_eq!(p.consts.len(), 2);
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
    let mut compiler: Compiler = Compiler::new();

    let script = parse("not true")?;
    let p = compiler.compile(script)?;

    assert_eq!(p.consts.len(), 1);
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
    let mut compiler: Compiler = Compiler::new();

    let script = parse("42 == 42")?;
    let p = compiler.compile(script)?;

    assert_eq!(p.consts.len(), 1);
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
    let mut compiler: Compiler = Compiler::new();

    let script = parse(r#"patch {} of  insert "foo" => 42 end"#)?;
    let p = compiler.compile(script)?;

    assert_eq!(p.consts.len(), 3);
    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            Record { size: 0 },
            LoadV1,
            Const { idx: 0 },
            String { size: 1 },
            TestRecortPresent,
            JumpFalse { dst: 9 },
            Const { idx: 1 },
            Error,
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
    let mut compiler: Compiler = Compiler::new();

    let script = parse(r#"patch {} of  default "foo" => 42 end"#)?;
    let p = compiler.compile(script)?;

    assert_eq!(p.consts.len(), 2);
    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            Record { size: 0 },
            LoadV1,
            Const { idx: 0 },
            String { size: 1 },
            TestRecortPresent,
            JumpTrue { dst: 9 },
            Const { idx: 1 },
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
fn patch_default_present() -> Result<()> {
    let mut compiler: Compiler = Compiler::new();

    let script = parse(r#"patch {"foo":"bar"} of  default "foo" => 42 end"#)?;
    let p = compiler.compile(script)?;

    assert_eq!(p.consts.len(), 3);
    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            Const { idx: 0 },
            String { size: 1 },
            Const { idx: 1 },
            String { size: 1 },
            Record { size: 1 },
            LoadV1,
            Const { idx: 0 },
            String { size: 1 },
            TestRecortPresent,
            JumpTrue { dst: 13 },
            Const { idx: 2 },
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
    let mut compiler: Compiler = Compiler::new();

    let script = parse(r#"patch {"foo":"bar"} of insert "foo" => 42 end"#)?;
    let p = compiler.compile(script)?;

    assert_eq!(p.consts.len(), 4);
    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            Const { idx: 0 },
            String { size: 1 },
            Const { idx: 1 },
            String { size: 1 },
            Record { size: 1 },
            LoadV1,
            Const { idx: 0 },
            String { size: 1 },
            TestRecortPresent,
            JumpFalse { dst: 13 },
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
fn patch_update() -> Result<()> {
    let mut compiler: Compiler = Compiler::new();

    let script = parse(r#"patch {"foo":"bar"} of update "foo" => 42 end"#)?;
    let p = compiler.compile(script)?;

    assert_eq!(p.consts.len(), 4);
    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            Const { idx: 0 },
            String { size: 1 },
            Const { idx: 1 },
            String { size: 1 },
            Record { size: 1 },
            LoadV1,
            Const { idx: 0 },
            String { size: 1 },
            TestRecortPresent,
            JumpTrue { dst: 13 },
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
fn patch_update_error() -> Result<()> {
    let mut compiler: Compiler = Compiler::new();

    let script = parse(r#"patch {} of update "foo" => 42 end"#)?;
    let p = compiler.compile(script)?;

    assert_eq!(p.consts.len(), 3);
    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            Record { size: 0 },
            LoadV1,
            Const { idx: 0 },
            String { size: 1 },
            TestRecortPresent,
            JumpTrue { dst: 9 },
            Const { idx: 1 },
            Error,
            Const { idx: 2 },
            RecordSet,
            SwapV1,
        ]
    );

    assert!(run(&p).is_err(),);
    Ok(())
}

#[test]
fn patch_upsert_1() -> Result<()> {
    let mut compiler: Compiler = Compiler::new();

    let script = parse(r#"patch {"foo":"bar"} of upsert "foo" => 42 end"#)?;
    let p = compiler.compile(script)?;

    assert_eq!(p.consts.len(), 3);
    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            Const { idx: 0 },
            String { size: 1 },
            Const { idx: 1 },
            String { size: 1 },
            Record { size: 1 },
            LoadV1,
            Const { idx: 0 },
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
fn patch_upsert_2() -> Result<()> {
    let mut compiler: Compiler = Compiler::new();

    let script = parse(r#"patch {} of upsert "foo" => 42 end"#)?;
    let p = compiler.compile(script)?;

    assert_eq!(p.consts.len(), 2);
    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            Record { size: 0 },
            LoadV1,
            Const { idx: 0 },
            String { size: 1 },
            Const { idx: 1 },
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
    let mut compiler: Compiler = Compiler::new();

    let script = parse(
        r#"patch patch {"foo":"bar"} of upsert "bar" => "baz" end of insert "baz" => 42 end"#,
    )?;
    let p = compiler.compile(script)?;

    println!("{p}");
    assert_eq!(p.consts.len(), 5);
    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            StoreV1,
            Const { idx: 0 },
            String { size: 1 },
            Const { idx: 1 },
            String { size: 1 },
            Record { size: 1 },
            LoadV1,
            Const { idx: 1 },
            String { size: 1 },
            Const { idx: 2 },
            String { size: 1 },
            RecordSet,
            SwapV1,
            LoadV1,
            Const { idx: 2 },
            String { size: 1 },
            TestRecortPresent,
            JumpFalse { dst: 21 },
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
            "foo": "bar",
            "bar": "baz",
            "baz": 42
        })
    );
    Ok(())
}

#[test]
fn array_index_fast() -> Result<()> {
    let mut compiler: Compiler = Compiler::new();

    let script = parse("[1,2,3][1]")?;
    let p = compiler.compile(script)?;

    assert_eq!(p.consts.len(), 4);
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
    let mut compiler: Compiler = Compiler::new();

    let script = parse("[1,2,3][1+1]")?;
    let p = compiler.compile(script)?;

    assert_eq!(p.consts.len(), 4);
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
            Binary { op: BinOpKind::Add },
            Get,
        ]
    );

    assert_eq!(run(&p)?, 3);
    Ok(())
}

#[test]
fn record_key() -> Result<()> {
    let mut compiler: Compiler = Compiler::new();

    let script = parse(r#"{"snot":"badger"}.snot"#)?;
    let p = compiler.compile(script)?;

    assert_eq!(p.consts.len(), 2);
    assert_eq!(
        p.opcodes,
        &[
            Const { idx: 0 },
            String { size: 1 },
            Const { idx: 1 },
            String { size: 1 },
            Record { size: 1 },
            GetKey { key: 0 },
        ]
    );

    assert_eq!(run(&p)?, "badger");
    Ok(())
}

#[test]
fn record_path() -> Result<()> {
    let mut compiler: Compiler = Compiler::new();

    let script = parse(r#"{"snot":"badger"}["snot"]"#)?;
    let p = compiler.compile(script)?;

    assert_eq!(p.consts.len(), 2);
    assert_eq!(
        p.opcodes,
        &[
            Const { idx: 0 },
            String { size: 1 },
            Const { idx: 1 },
            String { size: 1 },
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
    let mut compiler: Compiler = Compiler::new();

    let script = parse("[1,2,3][0:2]")?;
    let p = compiler.compile(script)?;

    assert_eq!(p.consts.len(), 5);
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
    let mut compiler: Compiler = Compiler::new();

    let script = parse("event.int")?;
    let p = compiler.compile(script)?;

    assert_eq!(p.consts.len(), 0);
    assert_eq!(p.opcodes, &[LoadEvent, GetKey { key: 0 },]);

    assert_eq!(run(&p)?, 42);
    Ok(())
}

#[test]
fn event_nested() -> Result<()> {
    let mut compiler: Compiler = Compiler::new();

    let script = parse("event.object.a")?;
    let p = compiler.compile(script)?;

    assert_eq!(p.consts.len(), 0);
    assert_eq!(
        p.opcodes,
        &[LoadEvent, GetKey { key: 0 }, GetKey { key: 1 }]
    );

    assert_eq!(run(&p)?, 1);
    Ok(())
}

#[test]
fn event_nested_index() -> Result<()> {
    let mut compiler: Compiler = Compiler::new();

    let script = parse("event.array[1]")?;
    let p = compiler.compile(script)?;

    assert_eq!(p.consts.len(), 0);
    assert_eq!(
        p.opcodes,
        &[LoadEvent, GetKey { key: 0 }, IndexFast { idx: 1 },]
    );

    assert_eq!(run(&p)?, 2);
    Ok(())
}

#[test]
fn test_local() -> Result<()> {
    let mut compiler: Compiler = Compiler::new();

    let script = parse("let a = 42; a")?;
    let p = compiler.compile(script)?;

    assert_eq!(p.consts.len(), 1);
    assert_eq!(p.max_locals, 0);
    assert_eq!(
        p.opcodes,
        &[
            Const { idx: 0 },
            StoreLocal { idx: 0 },
            LoadLocal { idx: 0 },
        ]
    );

    assert_eq!(run(&p)?, 42);
    Ok(())
}

#[test]
fn test_local_event() -> Result<()> {
    let mut compiler: Compiler = Compiler::new();

    let script = parse("let a = event; a.int")?;
    let p = compiler.compile(script)?;

    assert_eq!(p.consts.len(), 0);
    assert_eq!(p.max_locals, 0);
    assert_eq!(
        p.opcodes,
        &[
            LoadEvent,
            StoreLocal { idx: 0 },
            LoadLocal { idx: 0 },
            GetKey { key: 0 },
        ]
    );

    assert_eq!(run(&p)?, 42);
    Ok(())
}

#[test]
fn test_match_if_else() -> Result<()> {
    let mut compiler: Compiler = Compiler::new();

    let script = parse("match event.int of case 42 => 42 case _ => 0 end")?;
    let p = compiler.compile(script)?;

    println!("{p}");
    assert_eq!(p.consts.len(), 2);
    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            LoadEvent,
            GetKey { key: 0 },
            LoadV1,
            CopyV1,
            Const { idx: 0 },
            Binary { op: BinOpKind::Eq },
            LoadRB,
            JumpFalse { dst: 11 },
            Const { idx: 0 },
            Jump { dst: 12 },
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
    use BinOpKind::Eq;
    let mut compiler: Compiler = Compiler::new();

    let script =
        parse("match event.object of case 42 => 42 case %{} => \"record\" case _ => 0 end")?;
    let p = compiler.compile(script)?;

    println!("{p}");
    assert_eq!(p.consts.len(), 3);
    assert_eq!(
        p.opcodes,
        &[
            StoreV1,
            LoadEvent,
            GetKey { key: 0 },
            LoadV1,
            CopyV1,
            Const { idx: 0 },
            Binary { op: Eq },
            LoadRB,
            JumpFalse { dst: 11 },
            Const { idx: 0 },
            Jump { dst: 19 },
            CopyV1,
            TestIsRecord,
            JumpFalse { dst: 14 },
            JumpFalse { dst: 18 },
            Const { idx: 1 },
            String { size: 1 },
            Jump { dst: 19 },
            Const { idx: 2 },
            LoadV1,
            SwapV1,
        ]
    );

    assert_eq!(run(&p)?, "record");
    Ok(())
}
