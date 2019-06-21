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

// NOTE: we use a lot of arguments here, we are aware of that but tough luck
// FIXME: investigate if re-writing would make code better
#![allow(clippy::too_many_arguments)]

use crate::ast::{self, Expr, Helper};
use crate::errors::*;
use crate::lexer::{self, TokenFuns};

use crate::ast::BaseExpr;
use crate::highlighter::{DumbHighlighter, Highlighter};
use crate::parser::grammar;
use crate::pos::Range;
use crate::registry::{Context, Registry};
use crate::runtime::NormalizedSegment;
use halfbrown::hashmap;
use halfbrown::HashMap;
use simd_json::borrowed::{Map, Value};
use simd_json::value::ValueTrait;
use std::borrow::Borrow;
use std::collections::LinkedList;
use std::io::Write;
use std::iter::Iterator;

#[derive(Debug, Serialize, PartialEq)]
pub enum Return<'event> {
    Emit {
        value: Value<'event>,
        port: Option<String>,
    },
    Drop,
    EmitEvent {
        port: Option<String>,
    },
}

pub struct CheekyStack<T> {
    stack: LinkedList<T>,
}

impl<T> std::default::Default for CheekyStack<T> {
    fn default() -> Self {
        Self {
            stack: LinkedList::new(),
        }
    }
}

impl<T> CheekyStack<T> {
    pub fn push(&self, v: T) -> &T {
        // We can do this since adding a element will never change existing elements
        // within a linked list
        #[allow(mutable_transmutes)]
        #[allow(clippy::transmute_ptr_to_ptr)]
        let s: &mut LinkedList<T> = unsafe { std::mem::transmute(&self.stack) };
        s.push_front(v);
        if let Some(v) = s.front() {
            v
        } else {
            // NOTE This is OK since we just pushed we know there is a element in the stack.
            unreachable!()
        }
    }
    #[allow(dead_code)] // NOTE: Dman dual main and lib crate ...
    pub fn clear(&mut self) {
        self.stack.clear();
    }
}

pub type ValueStack<'event> = CheekyStack<Value<'event>>;
pub type LocalMap<'map> = simd_json::value::borrowed::Map<'map>;

pub trait Interpreter<'run, 'event, 'script, Ctx>
where
    Ctx: Context + 'static,
{
    fn run(
        &'script self,
        context: &'run Ctx,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut Value<'event>,
        stack: &'run ValueStack<'event>,
    ) -> Result<Cont<'run, 'event>>;

    fn resolve(
        &'script self,
        context: &'run Ctx,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut Value<'event>,
        path: &'script ast::Path<Ctx>,
        stack: &'run ValueStack<'event>,
    ) -> Result<&'run Value<'event>>;

    fn assign(
        &'script self,
        context: &'run Ctx,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut Value<'event>,
        path: &'script ast::Path<Ctx>,
        value: Value<'event>,
        stack: &'run ValueStack<'event>,
    ) -> Result<&'run Value<'event>>;
}

#[derive(Debug)]
pub enum Cont<'run, 'event>
where
    'event: 'run,
{
    Cont(&'run Value<'event>),
    Emit(Value<'event>, Option<String>),
    Drop,
    EmitEvent(Option<String>),
}
impl<'run, 'event> Cont<'run, 'event>
where
    'event: 'run,
{
    pub fn into_value<Ctx: Context>(
        self,
        expr: &Expr<Ctx>,
        inner: &Expr<Ctx>,
    ) -> Result<&'run Value<'event>> {
        match self {
            Cont::Cont(v) => Ok(v),
            Cont::EmitEvent(_p) => Err(ErrorKind::InvalidEmit(expr.into(), inner.into()).into()),
            Cont::Emit(_v, _p) => Err(ErrorKind::InvalidEmit(expr.into(), inner.into()).into()),
            Cont::Drop => Err(ErrorKind::InvalidDrop(expr.into(), inner.into()).into()),
        }
    }
}

impl<'run, 'event> From<Cont<'run, 'event>> for Return<'event>
where
    'event: 'run,
{
    // This clones the data since we're returning it out of the scope of the
    // esecution - we might want to investigate if we can get rid of this in some cases.
    fn from(v: Cont<'run, 'event>) -> Self {
        match v {
            Cont::Cont(value) => Return::Emit {
                value: value.clone(),
                port: None,
            },
            Cont::Emit(value, port) => Return::Emit { value, port },
            Cont::EmitEvent(port) => Return::EmitEvent { port },
            Cont::Drop => Return::Drop,
        }
    }
}

macro_rules! demit {
    ($data:expr) => {
        match $data {
            Cont::Cont(r) => r,
            Cont::Emit(v, p) => return Ok(Cont::Emit(v, p)),
            Cont::Drop => return Ok(Cont::Drop),
            Cont::EmitEvent(p) => return Ok(Cont::EmitEvent(p)),
        }
    };
}

#[derive(Debug)]
pub struct Script<Ctx>
where
    Ctx: Context + 'static,
{
    pub script: ast::Script<Ctx>,
    pub source: String, //tokens: Vec<std::result::Result<TokenSpan<'script>, LexerError>>
    pub warnings: Vec<ast::Warning>,
}

impl<'run, 'event, 'script, Ctx> Script<Ctx>
where
    Ctx: Context + 'static,
    'script: 'event,
    'event: 'run,
{
    pub fn parse(script: &'script str, reg: &Registry<Ctx>) -> Result<Self> {
        let mut script = script.to_string();
        //FIXME: There is a bug in the lexer that requires a tailing ' ' otherwise
        //       it will not recognize a singular 'keywkrd'
        //       Also: darach is a snot badger!
        script.push(' ');
        let lexemes: Result<Vec<_>> = lexer::tokenizer(&script).collect();
        let lexemes = lexemes?;
        let mut filtered_tokens = Vec::new();

        for t in lexemes {
            let keep = !t.value.is_ignorable();
            if keep {
                filtered_tokens.push(Ok(t));
            }
        }

        let mut helper = Helper::new(reg);
        let ast = grammar::ScriptParser::new()
            .parse(filtered_tokens)?
            .up(&mut helper)?;

        Ok(Script {
            script: ast,
            source: script,
            warnings: helper.into_warnings(),
        })
    }

    /*
    pub fn format_parser_error(script: &str, e: Error) -> String {
        let mut h = DumbHighlighter::default();
        if Self::format_error_from_script(script, &mut h, &e).is_ok() {
            h.to_string()
        } else {
            format!("Failed to extract code for error: {}", e)
        }
    }
     */
    pub fn highlight_script_with<H: Highlighter>(script: &str, h: &mut H) -> std::io::Result<()> {
        let tokens: Vec<_> = lexer::tokenizer(&script).collect();
        h.highlight(tokens)
    }

    pub fn format_error_from_script<H: Highlighter>(
        script: &str,
        h: &mut H,
        e: &Error,
    ) -> std::io::Result<()> {
        let tokens: Vec<_> = lexer::tokenizer(&script).collect();
        match e.context() {
            (Some(Range(start, end)), _) => {
                h.highlight_runtime_error(tokens, start, end, Some(e.into()))
            }

            _other => {
                let _ = write!(h.get_writer(), "Error: {}", e);
                h.finalize()
            }
        }
    }

    pub fn format_warnings_with<H: Highlighter>(&self, h: &mut H) -> std::io::Result<()> {
        for w in &self.warnings {
            let tokens: Vec<_> = lexer::tokenizer(&self.source).collect();
            h.highlight_runtime_error(tokens, w.outer.0, w.outer.1, Some(w.into()))?;
        }
        Ok(())
    }

    #[allow(dead_code)] // NOTE: Dman dual main and lib crate ...
    pub fn format_error(&self, e: Error) -> String {
        let mut h = DumbHighlighter::default();
        if self.format_error_with(&mut h, &e).is_ok() {
            h.to_string()
        } else {
            format!("Failed to extract code for error: {}", e)
        }
    }

    pub fn format_error_with<H: Highlighter>(&self, h: &mut H, e: &Error) -> std::io::Result<()> {
        Self::format_error_from_script(&self.source, h, e)
    }

    pub fn run(
        &'script self,
        context: &'run Ctx,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
    ) -> Result<Return<'event>> {
        let stack = ValueStack::default();
        let mut local = simd_json::borrowed::Value::Object(hashmap! {});
        let exprs = &self.script.exprs;
        let mut val = &Value::Null;
        for expr in exprs {
            match expr.run(context, event, meta, &mut local, &stack)? {
                Cont::Drop => return Ok(Return::Drop),
                Cont::Emit(value, port) => return Ok(Return::Emit { value, port }),
                Cont::EmitEvent(port) => return Ok(Return::EmitEvent { port }),
                Cont::Cont(v) => val = v,
            }
        }

        Ok(Return::Emit {
            value: val.clone(),
            port: None,
        })
    }
}

enum PredicateCont {
    NoMatch,
    Match,
}

// Err Free zone

impl<'script, 'event, 'run, Ctx: Context> Expr<Ctx>
where
    'script: 'event,
    'event: 'run,
{
    fn merge_values(
        &self,
        inner: &Expr<Ctx>,
        value: &'run mut Value<'event>,
        replacement: &'run Value<'event>,
    ) -> Result<()> {
        if !replacement.is_object() {
            //NOTE: We got to clone here since we're duplicating values
            *value = replacement.clone();
            return Ok(());
        }

        if !value.is_object() {
            *value = Value::Object(hashmap! {});
        }

        match value {
            Value::Object(ref mut map) => {
                match replacement {
                    Value::Object(rep) => {
                        for (k, v) in rep {
                            if v.is_null() {
                                map.remove(k);
                            } else {
                                match map.get_mut(k) {
                                    Some(k) => self.merge_values(inner, k, v)?,
                                    None => {
                                        //NOTE: We got to clone here since we're duplicating values
                                        map.insert(k.to_string().into(), v.clone());
                                    }
                                }
                            }
                        }
                    }
                    other => {
                        return self.error_type_conflict(&inner, other.kind(), ValueType::Object)
                    }
                }
            }
            other => return self.error_type_conflict(&inner, other.kind(), ValueType::Object),
        }

        Ok(())
    }

    fn literal(
        &'script self,
        context: &'run Ctx,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut Value<'event>,
        stack: &'run ValueStack<'event>,
        expr: &'script ast::Literal<Ctx>,
    ) -> Result<&'run Value<'event>> {
        match &expr.value {
            ast::LiteralValue::Native(owned) => Ok(stack.push(owned.clone().into())),
            ast::LiteralValue::List(ref list) => {
                let mut r: Vec<Value<'event>> = Vec::with_capacity(list.len());
                for expr in list {
                    r.push(
                        expr.run(context, event, meta, local, stack)?
                            .into_value(&self, &expr)?
                            .clone(),
                    );
                }
                Ok(stack.push(Value::Array(r)))
            }
        }
    }

    fn invoke(
        &'script self,
        context: &'run Ctx,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut Value<'event>,
        stack: &'run ValueStack<'event>,
        expr: &'script ast::Invoke<Ctx>,
    ) -> Result<&'run Value<'event>> {
        let mut argv: Vec<&simd_json::borrowed::Value> = Vec::new();
        for arg in &expr.args {
            // FIXME: We should find a way to get rid of those clones
            let result = arg
                .run(context, event, meta, local, stack)?
                .into_value(&self, &arg)?
                .clone();
            argv.push(stack.push(result));
        }
        (expr.invocable)(context, &argv)
            .map(simd_json::value::borrowed::Value::from)
            .map(|v| stack.push(v))
            .map_err(|e| e.into_err(&self, &self, None))
    }

    fn unary(
        &'script self,
        context: &'run Ctx,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut Value<'event>,
        stack: &'run ValueStack<'event>,
        expr: &'script ast::UnaryExpr<Ctx>,
    ) -> Result<&'run Value<'event>> {
        let rhs = expr
            .expr
            .run(context, event, meta, local, stack)?
            .into_value(&self, &expr.expr)?;
        match (&expr.kind, rhs) {
            (ast::UnaryOpKind::Minus, Value::I64(x)) => Ok(stack.push(Value::I64(-*x))),
            (ast::UnaryOpKind::Minus, Value::F64(x)) => Ok(stack.push(Value::F64(-*x))),
            (ast::UnaryOpKind::Plus, Value::I64(x)) => Ok(stack.push(Value::I64(*x))),
            (ast::UnaryOpKind::Plus, Value::F64(x)) => Ok(stack.push(Value::F64(*x))),
            (ast::UnaryOpKind::Not, Value::Bool(b)) => Ok(stack.push(Value::Bool(!*b))),
            (op, val) => self.error_invalid_unary(&expr.expr, *op, &val),
        }
    }

    fn binary(
        &'script self,
        context: &'run Ctx,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut Value<'event>,
        stack: &'run ValueStack<'event>,
        expr: &'script ast::BinExpr<Ctx>,
    ) -> Result<&'run Value<'event>> {
        // Lazy Heinz doesn't want to write that 10000 times
        // - snot badger - Darach
        use ast::BinOpKind::*;
        // FIXME: We should eventually get rid of those but
        // it is needed as at this time we can't guaranteee
        // that the rhs evaluation won't invalidate the reference
        let lhs = expr
            .lhs
            .run(context, event, meta, local, stack)?
            .into_value(&self, &expr.lhs)?
            .clone();
        let rhs = expr
            .rhs
            .run(context, event, meta, local, stack)?
            .into_value(&self, &expr.rhs)?;
        let error = std::f64::EPSILON;
        let r = match (&expr.kind, &lhs, rhs) {
            (Eq, Value::Null, Value::Null) => Ok(Value::Bool(true)),
            (NotEq, Value::Null, Value::Null) => Ok(Value::Bool(false)),
            (And, Value::Bool(l), Value::Bool(r)) => Ok(Value::Bool(*l && *r)),
            (Or, Value::Bool(l), Value::Bool(r)) => Ok(Value::Bool(*l || *r)),
            (NotEq, Value::Object(l), Value::Object(r)) => Ok(Value::Bool(*l != *r)),
            (NotEq, Value::Array(l), Value::Array(r)) => Ok(Value::Bool(*l != *r)),
            (NotEq, Value::Bool(l), Value::Bool(r)) => Ok(Value::Bool(*l != *r)),
            (NotEq, Value::String(l), Value::String(r)) => Ok(Value::Bool(*l != *r)),
            (NotEq, Value::I64(l), Value::I64(r)) => Ok(Value::Bool(*l != *r)),
            (NotEq, Value::I64(l), Value::F64(r)) => {
                Ok(Value::Bool(((*l as f64) - *r).abs() > error))
            }
            (NotEq, Value::F64(l), Value::I64(r)) => {
                Ok(Value::Bool((*l - (*r as f64)).abs() > error))
            }
            (NotEq, Value::F64(l), Value::F64(r)) => Ok(Value::Bool((*l - *r).abs() > error)),
            (Eq, Value::Object(l), Value::Object(r)) => Ok(Value::Bool(*l == *r)),
            (Eq, Value::Array(l), Value::Array(r)) => Ok(Value::Bool(*l == *r)),
            (Eq, Value::Bool(l), Value::Bool(r)) => Ok(Value::Bool(*l == *r)),
            (Eq, Value::String(l), Value::String(r)) => Ok(Value::Bool(*l == *r)),
            (Eq, Value::I64(l), Value::I64(r)) => Ok(Value::Bool(*l == *r)),
            (Eq, Value::I64(l), Value::F64(r)) => Ok(Value::Bool(((*l as f64) - *r).abs() < error)),
            (Eq, Value::F64(l), Value::I64(r)) => Ok(Value::Bool((*l - (*r as f64)).abs() < error)),
            (Eq, Value::F64(l), Value::F64(r)) => Ok(Value::Bool((*l - *r).abs() < error)),
            (Gte, Value::I64(l), Value::I64(r)) => Ok(Value::Bool(*l >= *r)),
            (Gte, Value::I64(l), Value::F64(r)) => Ok(Value::Bool((*l as f64) >= *r)),
            (Gte, Value::F64(l), Value::I64(r)) => Ok(Value::Bool(*l >= (*r as f64))),
            (Gte, Value::F64(l), Value::F64(r)) => Ok(Value::Bool(*l >= *r)),
            (Gt, Value::I64(l), Value::I64(r)) => Ok(Value::Bool(*l > *r)),
            (Gt, Value::I64(l), Value::F64(r)) => Ok(Value::Bool((*l as f64) > *r)),
            (Gt, Value::F64(l), Value::I64(r)) => Ok(Value::Bool(*l > (*r as f64))),
            (Gt, Value::F64(l), Value::F64(r)) => Ok(Value::Bool(*l > *r)),
            (Lt, Value::I64(l), Value::I64(r)) => Ok(Value::Bool(*l < *r)),
            (Lt, Value::I64(l), Value::F64(r)) => Ok(Value::Bool((*l as f64) < *r)),
            (Lt, Value::F64(l), Value::I64(r)) => Ok(Value::Bool(*l < (*r as f64))),
            (Lt, Value::F64(l), Value::F64(r)) => Ok(Value::Bool(*l < *r)),
            (Lte, Value::I64(l), Value::I64(r)) => Ok(Value::Bool(*l <= *r)),
            (Lte, Value::I64(l), Value::F64(r)) => Ok(Value::Bool((*l as f64) <= *r)),
            (Lte, Value::F64(l), Value::I64(r)) => Ok(Value::Bool(*l <= (*r as f64))),
            (Lte, Value::F64(l), Value::F64(r)) => Ok(Value::Bool(*l <= *r)),
            (Add, Value::String(l), Value::String(r)) => Ok(format!("{}{}", *l, *r).into()),
            (Add, Value::I64(l), Value::I64(r)) => Ok(Value::I64(*l + *r)),
            (Add, Value::I64(l), Value::F64(r)) => Ok(Value::F64((*l as f64) + *r)),
            (Add, Value::F64(l), Value::I64(r)) => Ok(Value::F64(*l + (*r as f64))),
            (Add, Value::F64(l), Value::F64(r)) => Ok(Value::F64(*l + *r)),
            (Sub, Value::I64(l), Value::I64(r)) => Ok(Value::I64(*l - *r)),
            (Sub, Value::I64(l), Value::F64(r)) => Ok(Value::F64((*l as f64) - *r)),
            (Sub, Value::F64(l), Value::I64(r)) => Ok(Value::F64(*l - (*r as f64))),
            (Sub, Value::F64(l), Value::F64(r)) => Ok(Value::F64(*l - *r)),
            (Mul, Value::I64(l), Value::I64(r)) => Ok(Value::I64(*l * *r)),
            (Mul, Value::I64(l), Value::F64(r)) => Ok(Value::F64((*l as f64) * *r)),
            (Mul, Value::F64(l), Value::I64(r)) => Ok(Value::F64(*l * (*r as f64))),
            (Mul, Value::F64(l), Value::F64(r)) => Ok(Value::F64(*l * *r)),
            (Div, Value::I64(l), Value::I64(r)) => Ok(Value::F64((*l as f64) / (*r as f64))),
            (Div, Value::I64(l), Value::F64(r)) => Ok(Value::F64((*l as f64) / *r)),
            (Div, Value::F64(l), Value::I64(r)) => Ok(Value::F64(*l / (*r as f64))),
            (Div, Value::F64(l), Value::F64(r)) => Ok(Value::F64(*l / *r)),
            (Mod, Value::I64(l), Value::I64(r)) => Ok(Value::I64(*l % *r)),
            (op, left, right) => self.error_invalid_binary(&expr.lhs, *op, &left, &right),
        };
        r.map(|v| stack.push(v))
    }

    /*
    fn predicate_expr<'script, 'event, 'run, Ctx>(
        &'script self,
        context: &'run Ctx,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut Value<'event>,
        stack: &'run ValueStack<'event>,
        expr: &'script ast::BinExpr,
    ) -> Result<bool>
    where
        Ctx: Context + 'static,
        'script: 'event,
        'event: 'run,
    {
        let pred = expr
            .lhs
            .run(context, event, meta, local, stack)?
            .into_value(self)?;
        if let Value::Bool(test) = pred {
            Ok(test)
        } else {
            Err(self.error_type_conflict(Some(*expr.lhs.clone()), ValueType::Boolean, pred.into()))
        }
    }
    */

    fn rp(
        &'script self,
        context: &'run Ctx,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut Value<'event>,
        stack: &'run ValueStack<'event>,
        target: &'run Value<'event>,
        rp: &'script ast::RecordPattern<Ctx>,
    ) -> Result<PredicateCont> {
        for pp in &rp.fields {
            let path = pp.lhs();

            match pp.borrow() {
                ast::PredicatePattern::TildeEq { test, .. } => {
                    let testee: &Value = match target {
                        Value::Object(ref o) => {
                            if let Some(v) = o.get(path) {
                                v
                            } else {
                                return Ok(PredicateCont::NoMatch);
                            }
                        }
                        _ => {
                            return Ok(PredicateCont::NoMatch);
                        }
                    };

                    if test.extractor.extract(testee).is_err() {
                        return Ok(PredicateCont::NoMatch);
                    }
                }
                ast::PredicatePattern::Eq { rhs, not, .. } => {
                    let testee: &Value = match target {
                        Value::Object(ref o) => {
                            if let Some(v) = o.get(path) {
                                v
                            } else {
                                return Ok(PredicateCont::NoMatch);
                            }
                        }
                        _ => {
                            return Ok(PredicateCont::NoMatch);
                        }
                    };
                    let rhs = rhs
                        .run(context, event, meta, local, stack)?
                        .into_value(self, &rhs)?;
                    let r = testee == rhs;
                    let m = if *not { !r } else { r };
                    if !m {
                        return Ok(PredicateCont::NoMatch);
                    };
                }
                ast::PredicatePattern::RecordPatternEq { pattern, .. } => {
                    self.match_rp_expr(context, event, meta, local, stack, target, pattern)?;
                }
                ast::PredicatePattern::ArrayPatternEq { pattern, .. } => {
                    self.match_ap_expr(context, event, meta, local, stack, target, pattern)?;
                }
                ast::PredicatePattern::FieldPresent { lhs } => match target {
                    Value::Object(ref o) => {
                        if !o.contains_key(lhs.as_str()) {
                            return Ok(PredicateCont::NoMatch);
                        }
                    }
                    _ => return Ok(PredicateCont::NoMatch),
                },
                ast::PredicatePattern::FieldAbsent { lhs } => match target {
                    Value::Object(ref o) => {
                        if o.contains_key(lhs.as_str()) {
                            return Ok(PredicateCont::NoMatch);
                        }
                    }
                    _ => return Ok(PredicateCont::NoMatch),
                },
            }
        }

        // FIXME possibly missing a case?
        Ok(PredicateCont::Match)
    }

    fn match_rp_expr(
        &'script self,
        context: &'run Ctx,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut Value<'event>,
        stack: &'run ValueStack<'event>,
        target: &'run Value<'event>,
        rp: &'script ast::RecordPattern<Ctx>,
    ) -> Result<Option<Value<'event>>> {
        let mut acc = HashMap::with_capacity(rp.fields.len());
        for pp in &rp.fields {
            let key = pp.lhs();

            match pp {
                ast::PredicatePattern::TildeEq { test, .. } => {
                    let testee = match target {
                        Value::Object(ref o) => {
                            if let Some(v) = o.get(key) {
                                v
                            } else {
                                return Ok(None);
                            }
                        }
                        _ => return Ok(None),
                    };
                    if let Ok(x) = test.extractor.extract(&testee) {
                        acc.insert(key.to_string().into(), x);
                    } else {
                        // FIXME: We probably don't want to drop here
                        return Ok(None);
                    }
                }
                // FIXME: Why are we ignoring the LHS?
                ast::PredicatePattern::Eq { rhs, not, .. } => {
                    let testee = match target {
                        Value::Object(ref o) => {
                            if let Some(v) = o.get(key) {
                                v
                            } else {
                                return Ok(None);
                            }
                        }
                        _ => return Ok(None),
                    };
                    let rhs = rhs
                        .run(context, event, meta, local, stack)?
                        .into_value(self, &rhs)?;
                    let r = testee == rhs;
                    let m = if *not { !r } else { r };

                    if m {
                        continue;
                    } else {
                        // FIXME: We probably don't want to drop here
                        return Ok(None);
                    }
                }
                ast::PredicatePattern::FieldPresent { lhs } => match target {
                    Value::Object(ref o) => {
                        if o.contains_key(lhs.as_str()) {
                            continue;
                        } else {
                            return Ok(None);
                        }
                    }
                    _ => return Ok(None),
                },
                ast::PredicatePattern::FieldAbsent { lhs } => match target {
                    Value::Object(ref o) => {
                        if !o.contains_key(lhs.as_str()) {
                            continue;
                        } else {
                            return Ok(None);
                        }
                    }
                    _ => return Ok(None),
                },
                ast::PredicatePattern::RecordPatternEq { pattern, .. } => {
                    let testee = match target {
                        Value::Object(ref o) => {
                            if let Some(v) = o.get(key) {
                                v
                            } else {
                                return Ok(None);
                            }
                        }
                        _ => return Ok(None),
                    };
                    // FIXME destructure assign so we can get rid of dupe in assign cases
                    if let o @ Value::Object(_) = testee {
                        match self.rp(context, event, meta, local, stack, &o, pattern)? {
                            PredicateCont::Match => {
                                // NOTE We have to clone here since we duplicating data form one place
                                // into another
                                acc.insert(key.to_string().into(), o.clone());
                                continue;
                            }
                            PredicateCont::NoMatch => {
                                // FIXME abusing drop to short circuit and go to next outer(most) case
                                return Ok(None);
                            }
                        }
                    } else {
                        // FIXME abusing drop to short circuit and go to next outer(most) case
                        return Ok(None);
                    }
                }
                ast::PredicatePattern::ArrayPatternEq { pattern, .. } => {
                    let testee = match target {
                        Value::Object(ref o) => {
                            if let Some(v) = o.get(key) {
                                v
                            } else {
                                return Ok(None);
                            }
                        }
                        _ => return Ok(None),
                    };
                    // FIXME destructure assign so we can get rid of dupe in assign cases
                    if let a @ Value::Array(_) = testee {
                        match self.match_ap_expr(context, event, meta, local, stack, &a, pattern)? {
                            Some(r) => {
                                acc.insert(key.to_string().into(), r);
                                continue;
                            }
                            None => {
                                // FIXME abusing drop to short circuit and go to next outer(most) case
                                return Ok(None);
                            }
                        }
                    } else {
                        // FIXME abusing drop to short circuit and go to next outer(most) case
                        return Ok(None);
                    }
                }
            }
        }

        Ok(Some(Value::Object(acc)))
    }

    fn match_ap_expr(
        &'script self,
        context: &'run Ctx,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut Value<'event>,
        stack: &'run ValueStack<'event>,
        target: &'run Value<'event>,
        ap: &'script ast::ArrayPattern<Ctx>,
    ) -> Result<Option<Value<'event>>> {
        match target {
            Value::Array(ref a) => {
                let mut acc = Vec::with_capacity(a.len());
                let mut idx = 0;
                for candidate in a {
                    'inner: for expr in &ap.exprs {
                        match expr {
                            ast::ArrayPredicatePattern::Expr(e) => {
                                let r = e
                                    .run(context, event, meta, local, stack)?
                                    .into_value(&self, &e)?;
                                // NOTE: We are creating a new value here so we have to clone
                                if candidate == r {
                                    acc.push(Value::Array(vec![Value::Array(vec![
                                        Value::I64(idx),
                                        r.clone(),
                                    ])]));
                                }
                            }
                            ast::ArrayPredicatePattern::Tilde(test) => {
                                match test.extractor.extract(&candidate) {
                                    Ok(r) => {
                                        acc.push(Value::Array(vec![Value::Array(vec![
                                            Value::I64(idx),
                                            r,
                                        ])]));
                                    }
                                    _ => continue 'inner // return Ok(Cont::Drop(Value::Bool(true))),
                                }
                            }
                            ast::ArrayPredicatePattern::Record(rp) => {
                                match self.match_rp_expr(
                                    context, event, meta, local, stack, candidate, rp,
                                )? {
                                    Some(r) => {
                                        acc.push(Value::Array(vec![Value::Array(vec![
                                            Value::I64(idx),
                                            r,
                                        ])]));
                                    }
                                    _ => continue 'inner // return Ok(Cont::Drop(Value::Bool(true))),
                                }
                            }
                            ast::ArrayPredicatePattern::Array(ap) => {
                                match self.match_ap_expr(
                                    context, event, meta, local, stack, candidate, ap,
                                )? {
                                    Some(r) => {
                                        acc.push(Value::Array(vec![Value::Array(vec![
                                            Value::I64(idx),
                                            r,
                                        ])]));
                                    }
                                    _ => continue 'inner // return Ok(Cont::Drop(Value::Bool(true))),
                                }
                            }
                        }
                    }
                    idx += 1;
                }
                Ok(Some(Value::Array(acc)))
            }
            _ => Ok(None),
        }
    }

    fn execute_effectors<T: BaseExpr>(
        &'script self,
        context: &'run Ctx,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut Value<'event>,
        stack: &'run ValueStack<'event>,
        inner: &'script T,
        effectors: &'script [Expr<Ctx>],
    ) -> Result<Cont<'run, 'event>> {
        if effectors.is_empty() {
            return self.error_missing_effector(&Expr::dummy_from(inner));
        }
        // We know we have at least one element so [] access is safe!
        for effector in &effectors[..effectors.len() - 1] {
            demit!(effector.run(context, event, meta, local, stack)?);
        }
        let effector = &effectors[effectors.len() - 1];
        Ok(Cont::Cont(demit!(
            effector.run(context, event, meta, local, stack)?
        )))
    }

    fn test_guard(
        &'script self,
        context: &'run Ctx,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut Value<'event>,
        stack: &'run ValueStack<'event>,
        guard: &'script Option<Expr<Ctx>>,
    ) -> Result<bool> {
        if let Some(guard) = guard {
            let test = guard
                .run(context, event, meta, local, stack)?
                .into_value(&self, &guard)?;
            match test {
                Value::Bool(b) => Ok(*b),
                other => self.error_guard_not_bool(&guard, &other),
            }
        } else {
            Ok(true)
        }
    }

    fn match_expr(
        &'script self,
        context: &'run Ctx,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut Value<'event>,
        stack: &'run ValueStack<'event>,
        expr: &'script ast::Match<Ctx>,
    ) -> Result<Cont<'run, 'event>> {
        //FIXME: We really don't want to clone here
        // but for that we need to hard forbid mutation
        // in patterns
        let target: &Value<'event> = stack.push(
            expr.target
                .run(context, event, meta, local, stack)?
                .into_value(&self, &expr.target)?
                .clone(),
        );
        'predicate: for predicate in &expr.patterns {
            match predicate.pattern {
                ast::Pattern::Predicate(ref _pp) => {
                    //FIXME: How did this even get here?
                    return self.error_oops();
                }
                ast::Pattern::Record(ref rp) => {
                    if self
                        .match_rp_expr(context, event, meta, local, stack, &target, &rp)?
                        .is_some()
                    {
                        if !self.test_guard(context, event, meta, local, stack, &predicate.guard)? {
                            continue 'predicate;
                        };
                        return self.execute_effectors(
                            context,
                            event,
                            meta,
                            local,
                            stack,
                            predicate,
                            &predicate.exprs,
                        );
                    } else {
                        continue 'predicate;
                    }
                }
                ast::Pattern::Array(ref ap) => {
                    if self
                        .match_ap_expr(context, event, meta, local, stack, &target, &ap)?
                        .is_some()
                    {
                        if !self.test_guard(context, event, meta, local, stack, &predicate.guard)? {
                            continue 'predicate;
                        };
                        return self.execute_effectors(
                            context,
                            event,
                            meta,
                            local,
                            stack,
                            predicate,
                            &predicate.exprs,
                        );
                    } else {
                        continue 'predicate;
                    }
                }
                ast::Pattern::Expr(ref expr) => {
                    let res = expr
                        .run(context, event, meta, local, stack)?
                        .into_value(&self, &expr)?;

                    if target == res {
                        if !self.test_guard(context, event, meta, local, stack, &predicate.guard)? {
                            continue 'predicate;
                        };
                        return self.execute_effectors(
                            context,
                            event,
                            meta,
                            local,
                            stack,
                            predicate,
                            &predicate.exprs,
                        );
                    }
                    continue;
                }
                ast::Pattern::Assign(ref a) => {
                    let path = &a.id;
                    match *a.pattern {
                        ast::Pattern::Array(ref ap) => {
                            if let Some(v) = self
                                .match_ap_expr(context, event, meta, local, stack, &target, &ap)?
                            {
                                if !self.test_guard(
                                    context,
                                    event,
                                    meta,
                                    local,
                                    stack,
                                    &predicate.guard,
                                )? {
                                    continue 'predicate;
                                };
                                self.assign(context, event, meta, local, &path, v, stack)?;
                                return self.execute_effectors(
                                    context,
                                    event,
                                    meta,
                                    local,
                                    stack,
                                    predicate,
                                    &predicate.exprs,
                                );
                            } else {
                                continue 'predicate;
                            }
                        }
                        ast::Pattern::Record(ref rp) => {
                            if let Some(v) = self
                                .match_rp_expr(context, event, meta, local, stack, &target, &rp)?
                            {
                                if !self.test_guard(
                                    context,
                                    event,
                                    meta,
                                    local,
                                    stack,
                                    &predicate.guard,
                                )? {
                                    continue 'predicate;
                                };
                                self.assign(context, event, meta, local, &path, v, stack)?;
                                return self.execute_effectors(
                                    context,
                                    event,
                                    meta,
                                    local,
                                    stack,
                                    predicate,
                                    &predicate.exprs,
                                );
                            } else {
                                continue 'predicate;
                            }
                        }
                        ast::Pattern::Expr(ref expr) => {
                            let res = expr
                                .run(context, event, meta, local, stack)?
                                .into_value(&self, &expr)?;
                            let path = &a.id;
                            if target == res {
                                // NOTE We clone here since we write (aka mutate)
                                // this is intended behaviour
                                let res = res.clone();
                                self.assign(context, event, meta, local, &path, res, stack)?;
                                if !self.test_guard(
                                    context,
                                    event,
                                    meta,
                                    local,
                                    stack,
                                    &predicate.guard,
                                )? {
                                    continue 'predicate;
                                };
                                return self.execute_effectors(
                                    context,
                                    event,
                                    meta,
                                    local,
                                    stack,
                                    predicate,
                                    &predicate.exprs,
                                );
                            }
                        }
                        _ => return self.error_oops(),
                    }
                }
                ast::Pattern::Default => {
                    return self.execute_effectors(
                        context,
                        event,
                        meta,
                        local,
                        stack,
                        predicate,
                        &predicate.exprs,
                    );
                }
            }
        }
        self.error_no_clause_hit()
    }

    fn patch(
        &'script self,
        context: &'run Ctx,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut Value<'event>,
        stack: &'run ValueStack<'event>,
        expr: &'script ast::Patch<Ctx>,
    ) -> Result<&'run Value<'event>> {
        // NOTE: We clone this since we patch it - this should be not mutated but cloned
        let mut value = expr
            .target
            .run(context, event, meta, local, stack)?
            .into_value(&self, &expr.target)?
            .clone();

        for op in &expr.operations {
            // NOTE: This if is inside the for loop to prevent obj to be updated
            // between iterations and possibly lead to dangling pointers
            if let Value::Object(ref mut obj) = value {
                match op {
                    ast::PatchOperation::Insert { ident, expr } => {
                        let new_key = std::borrow::Cow::Owned(ident.clone());
                        let new_value = expr
                            .run(context, event, meta, local, stack)?
                            .into_value(&self, &expr)?;
                        if obj.contains_key(&new_key) {
                            return self.error_patch_insert_key_exists(expr, ident.clone());
                        } else {
                            obj.insert(new_key, new_value.clone());
                        }
                    }
                    ast::PatchOperation::Update { ident, expr } => {
                        let new_key = std::borrow::Cow::Owned(ident.clone());
                        let new_value = expr
                            .run(context, event, meta, local, stack)?
                            .into_value(&self, &expr)?;
                        if obj.contains_key(&new_key) {
                            obj.insert(new_key, new_value.clone());
                        } else {
                            return self.error_patch_update_key_missing(expr, ident.clone());
                        }
                    }
                    ast::PatchOperation::Upsert { ident, expr } => {
                        let new_key = std::borrow::Cow::Owned(ident.clone());
                        let new_value = expr
                            .run(context, event, meta, local, stack)?
                            .into_value(&self, &expr)?;
                        obj.insert(new_key, new_value.clone());
                    }
                    ast::PatchOperation::Erase { ident } => {
                        let new_key = std::borrow::Cow::Owned(ident.clone());
                        obj.remove(&new_key);
                    }
                    ast::PatchOperation::Merge { ident, expr } => {
                        let new_key = std::borrow::Cow::Owned(ident.clone());
                        let merge_spec = expr
                            .run(context, event, meta, local, stack)?
                            .into_value(&self, &expr)?;
                        match obj.get_mut(&new_key) {
                            Some(value @ Value::Object(_)) => {
                                self.merge_values(&expr, value, &merge_spec)?;
                            }
                            Some(other) => {
                                return self.error_patch_merge_type_conflict(
                                    expr,
                                    ident.clone(),
                                    &other,
                                );
                            }
                            None => {
                                let mut new_value = Value::Object(hashmap! {});
                                self.merge_values(&expr, &mut new_value, &merge_spec)?;
                                obj.insert(new_key, new_value);
                            }
                        }
                    }
                    ast::PatchOperation::TupleMerge { expr } => {
                        let merge_spec = expr
                            .run(context, event, meta, local, stack)?
                            .into_value(&self, &expr)?;
                        self.merge_values(&expr, &mut value, &merge_spec)?;
                    }
                }
            } else {
                return self.error_type_conflict(&expr.target, value.kind(), ValueType::Object);
            }
        }
        Ok(stack.push(value))
    }

    fn merge(
        &'script self,
        context: &'run Ctx,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut Value<'event>,
        stack: &'run ValueStack<'event>,
        expr: &'script ast::Merge<Ctx>,
    ) -> Result<&'run Value<'event>> {
        // NOTE: We got to clone here since we're are going
        // to change the value
        let mut value = expr
            .target
            .run(context, event, meta, local, stack)?
            .into_value(&self, &expr.target)?
            .clone();

        if value.is_object() {
            let replacement = expr
                .expr
                .run(context, event, meta, local, stack)?
                .into_value(&self, &expr.expr)?;

            if replacement.is_object() {
                self.merge_values(&expr.expr, &mut value, &replacement)?;
                Ok(stack.push(value))
            } else {
                self.error_type_conflict(&expr.expr, replacement.kind(), ValueType::Object)
            }
        } else {
            self.error_type_conflict(&expr.target, value.kind(), ValueType::Object)
        }
    }

    fn comprehension(
        &'script self,
        context: &'run Ctx,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut Value<'event>,
        stack: &'run ValueStack<'event>,
        expr: &'script ast::Comprehension<Ctx>,
    ) -> Result<Cont<'run, 'event>> {
        let mut value_vec = vec![];
        let target = &expr.target;
        let cases = &expr.cases;
        let mut once = false;
        let target_value = target
            .run(context, event, meta, local, stack)?
            .into_value(&self, &target)?;

        if let Some(target_map) = target_value.as_object() {
            // Record comprehension case

            // NOTE: Since we we are going to create new data from this
            // object we are cloning it.
            // This is also required since we might mutate. If we restruct
            // mutation in the future we could get rid of this.
            let target_map = target_map.clone();

            'comprehension_outer: for x in target_map.into_iter() {
                for e in cases {
                    let new_key = std::borrow::Cow::Owned(e.key_name.clone());
                    let new_value = std::borrow::Cow::Owned(e.value_name.clone());

                    if let Value::Object(local_map) = local {
                        if !once {
                            if local_map.contains_key(&new_key) {
                                return self.error_overwriting_local_in_comprehension(
                                    &Expr::dummy_from(e),
                                    e.key_name.clone(),
                                );
                            }
                            if local_map.contains_key(&new_value) {
                                return self.error_overwriting_local_in_comprehension(
                                    &Expr::dummy_from(e),
                                    e.value_name.clone(),
                                );
                            }
                        } else {
                            once = true;
                        }

                        // FIXME: This is ugly, we should not have to clone here
                        // probably a halfbrown bug or a general hashmap problem?
                        local_map.insert(new_key.clone(), x.0.to_string().into());
                        local_map.insert(new_value.clone(), x.1.clone());
                    }

                    match &e.guard {
                        Some(expr) => {
                            let test = expr
                                .run(context, event, meta, local, stack)?
                                .into_value(&self, &expr)?;
                            match test {
                                Value::Bool(true) => {
                                    let v =
                                        demit!(e.expr.run(context, event, meta, local, stack,)?);
                                    // NOTE: We are creating a new value so we have to clone;
                                    value_vec.push(v.clone());
                                    if let Value::Object(local_map) = local {
                                        local_map.remove(&new_key);
                                        local_map.remove(&new_value);
                                    }
                                    continue 'comprehension_outer;
                                }
                                Value::Bool(false) => {
                                    if let Value::Object(local_map) = local {
                                        local_map.remove(&new_key);
                                        local_map.remove(&new_value);
                                    }
                                    continue;
                                }
                                other => {
                                    return self.error_type_conflict(
                                        &expr,
                                        other.kind(),
                                        ValueType::Bool,
                                    )
                                }
                            }
                        }
                        None => {
                            let v = demit!(e.expr.run(context, event, meta, local, stack)?);
                            // NOTE: We are creating a new value so we have to clone;
                            value_vec.push(v.clone());
                            if let Value::Object(local_map) = local {
                                local_map.remove(&new_key);
                                local_map.remove(&new_value);
                            }
                            continue 'comprehension_outer;
                        }
                    }
                }
            }
        } else if let Some(target_array) = target_value.as_array() {
            // Array comprehension case

            // NOTE: Since we we are going to create new data from this
            // object we are cloning it.
            // This is also required since we might mutate. If we restruct
            // mutation in the future we could get rid of this.
            let target_array = target_array.clone();

            let mut count = 0;
            'comp_array_outer: for x in target_array {
                for e in cases {
                    let new_key = std::borrow::Cow::Owned(e.key_name.clone());
                    let new_value = std::borrow::Cow::Owned(e.value_name.clone());

                    if let Value::Object(local_map) = local {
                        if !once {
                            if local_map.contains_key(&new_key) {
                                return self.error_overwriting_local_in_comprehension(
                                    &Expr::dummy_from(e),
                                    e.key_name.clone(),
                                );
                            }
                            if local_map.contains_key(&new_value) {
                                return self.error_overwriting_local_in_comprehension(
                                    &Expr::dummy_from(e),
                                    e.key_name.clone(),
                                );
                            }
                        } else {
                            once = true;
                        }

                        // FIXME: This is ugly, we should not have to clone here
                        // probably a halfbrown bug or a general hashmap problem?
                        local_map.insert(new_key.clone(), Value::from(count as u64));
                        local_map.insert(new_value.clone(), x.clone());
                    }

                    match &e.guard {
                        Some(expr) => {
                            let test = expr
                                .run(context, event, meta, local, stack)?
                                .into_value(&self, &expr)?;
                            match test {
                                Value::Bool(true) => {
                                    let v =
                                        demit!(e.expr.run(context, event, meta, local, stack,)?);
                                    value_vec.push(v.clone());
                                    if let Value::Object(local_map) = local {
                                        local_map.remove(&new_key);
                                        local_map.remove(&new_value);
                                    }
                                    count += 1;
                                    continue 'comp_array_outer;
                                }
                                Value::Bool(false) => {
                                    if let Value::Object(local_map) = local {
                                        local_map.remove(&new_key);
                                        local_map.remove(&new_value);
                                    }
                                    continue;
                                }
                                other => {
                                    return self.error_type_conflict(
                                        &expr,
                                        other.kind(),
                                        ValueType::Bool,
                                    )
                                }
                            }
                        }
                        None => {
                            let v = demit!(e.expr.run(context, event, meta, local, stack)?);
                            value_vec.push(v.clone());
                            if let Value::Object(local_map) = local {
                                local_map.remove(&new_key);
                                local_map.remove(&new_value);
                            }
                            count += 1;
                            continue 'comp_array_outer;
                        }
                    }
                }
                count += 1;
            }
        }
        Ok(Cont::Cont(stack.push(Value::Array(value_vec))))
    }
    fn resolve_path_segments(
        &'script self,
        context: &'run Ctx,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut Value<'event>,
        path: &'script ast::Path<Ctx>,
        stack: &'run ValueStack<'event>,
    ) -> Result<Vec<NormalizedSegment>> {
        let udp = match path {
            ast::Path::Local(path) => &path.segments,
            ast::Path::Meta(path) => &path.segments,
            ast::Path::Event(path) => &path.segments,
        };

        let mut segments: Vec<NormalizedSegment> = vec![];

        for segment in udp {
            match segment {
                ast::Segment::ElementSelector { expr, start, end } => {
                    match expr
                        .run(context, event, meta, local, stack)?
                        .into_value(&self, &expr)?
                    {
                        Value::I64(n) => segments.push(NormalizedSegment::Index {
                            idx: *n as usize,
                            start: *start,
                            end: *end,
                        }),
                        Value::String(s) => segments.push(NormalizedSegment::FieldRef {
                            id: s.to_string(),
                            start: *start,
                            end: *end,
                        }),
                        other => {
                            return self.error_type_conflict_mult(
                                expr,
                                other.kind(),
                                vec![ValueType::I64, ValueType::String],
                            )
                        }
                    }
                }
                ast::Segment::RangeSelector {
                    range_start,
                    range_end,
                    start_lower,
                    end_lower,
                    start_upper,
                    end_upper,
                } => {
                    let s = range_start
                        .run(context, event, meta, local, stack)?
                        .into_value(&self, &range_start)?;
                    if let Some(range_start) = s.as_u64() {
                        let range_start = range_start as usize;
                        let e = range_end
                            .run(context, event, meta, local, stack)?
                            .into_value(&self, &range_end)?;
                        if let Some(range_end) = e.as_u64() {
                            let range_end = range_end as usize;
                            segments.push(NormalizedSegment::Range {
                                range_start,
                                range_end,
                                start: *start_lower,
                                end: *end_upper,
                            });
                        } else {
                            return self.error_type_conflict(
                                &Expr::dummy(*start_upper, *end_upper),
                                e.kind(),
                                ValueType::I64,
                            );
                        }
                    } else {
                        return self.error_type_conflict(
                            &Expr::dummy(*start_lower, *end_lower),
                            s.kind(),
                            ValueType::I64,
                        );
                    }
                }
            }
        }
        Ok(segments)
    }
}

impl<'run, 'event, 'script, Ctx> Interpreter<'run, 'event, 'script, Ctx> for Expr<Ctx>
where
    Ctx: Context + 'static,
    'script: 'event,
    'event: 'run,
{
    fn resolve(
        &'script self,
        context: &'run Ctx,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut Value<'event>,
        path: &'script ast::Path<Ctx>,
        stack: &'run ValueStack<'event>,
    ) -> Result<&'run Value<'event>> {
        let segments = self.resolve_path_segments(context, event, meta, local, path, stack)?;

        let mut current: &Value = match path {
            ast::Path::Local(_path) => local,
            ast::Path::Meta(_path) => meta,
            ast::Path::Event(_path) => event,
        };

        let segments = &mut segments.iter().peekable();
        'outer: while let Some(segment) = segments.next() {
            if segments.peek().is_none() {
                match segment {
                    NormalizedSegment::FieldRef { id, start, end } => match current {
                        Value::Object(o) => {
                            let id = id.as_str();
                            if let Some(v) = o.get(id) {
                                return Ok(v);
                            } else {
                                return self.error_bad_key(
                                    &Expr::dummy(*start, *end),
                                    &path,
                                    id.into(),
                                );
                            }
                        }
                        other => {
                            return self.error_type_conflict(
                                &Expr::dummy(*start, *end),
                                other.kind(),
                                ValueType::Array,
                            )
                        }
                    },
                    NormalizedSegment::Index { idx, start, end } => match current {
                        Value::Array(a) => {
                            return if let Some(v) = a.get(*idx) {
                                Ok(v)
                            } else {
                                self.error_array_out_of_bound(
                                    &Expr::dummy(*start, *end),
                                    &path,
                                    *idx..*idx,
                                )
                            }
                        }
                        other => {
                            return self.error_type_conflict(
                                &Expr::dummy(*start, *end),
                                other.kind(),
                                ValueType::Array,
                            )
                        }
                    },
                    NormalizedSegment::Range {
                        range_start,
                        range_end,
                        start,
                        end,
                    } => match current {
                        Value::Array(a) => {
                            return if let Some(v) = a.get(*range_start..*range_end) {
                                Ok(stack.push(Value::Array(v.to_vec())))
                            } else {
                                self.error_array_out_of_bound(
                                    &Expr::dummy(*start, *end),
                                    &path,
                                    *range_start..*range_end,
                                )
                            }
                        }
                        other => {
                            return self.error_type_conflict(
                                &Expr::dummy(*start, *end),
                                other.kind(),
                                ValueType::Array,
                            )
                        }
                    },
                }
            } else {
                match segment {
                    NormalizedSegment::FieldRef { id, start, end } => match current {
                        Value::Object(o) => match o.get(id.as_str()) {
                            Some(c @ Value::Object(_)) => {
                                current = c;
                                continue 'outer;
                            }
                            Some(c @ Value::Array(_)) => {
                                current = c;
                                continue 'outer;
                            }
                            Some(other) => {
                                return self.error_type_conflict_mult(
                                    &Expr::dummy(*start, *end),
                                    other.kind(),
                                    vec![ValueType::Object, ValueType::Array],
                                )
                            }
                            None => {
                                return self.error_bad_key(
                                    &Expr::dummy(*start, *end),
                                    &path,
                                    id.clone(),
                                )
                            }
                        },
                        other => {
                            return self.error_type_conflict(
                                &Expr::dummy(*start, *end),
                                other.kind(),
                                ValueType::Object,
                            )
                        }
                    },
                    NormalizedSegment::Index { idx, start, end } => match current {
                        Value::Array(a) => {
                            if let Some(v) = a.get(*idx) {
                                current = v;
                            } else {
                                return self.error_array_out_of_bound(
                                    &Expr::dummy(*start, *end),
                                    &path,
                                    *idx..*idx,
                                );
                            }

                            continue;
                        }
                        other => {
                            return self.error_type_conflict(
                                &Expr::dummy(*start, *end),
                                other.kind(),
                                ValueType::Array,
                            )
                        }
                    },
                    NormalizedSegment::Range {
                        range_start,
                        range_end,
                        start,
                        end,
                    } => match current {
                        Value::Array(b) => {
                            if let Some(v) = b.get(*range_start..*range_end) {
                                current = stack.push(Value::Array(v.to_vec()));
                            } else {
                                return self.error_array_out_of_bound(
                                    &Expr::dummy(*start, *end),
                                    &path,
                                    *range_start..*range_end,
                                );
                            }
                            continue 'outer;
                        }
                        other => {
                            return self.error_type_conflict(
                                &Expr::dummy(*start, *end),
                                other.kind(),
                                ValueType::Array,
                            )
                        }
                    },
                }
            }
        }
        Ok(current)
    }

    fn assign(
        &'script self,
        context: &'run Ctx,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut Value<'event>,
        path: &'script ast::Path<Ctx>,
        value: Value<'event>,
        stack: &'run ValueStack<'event>,
    ) -> Result<&'run Value<'event>> {
        let segments = self.resolve_path_segments(context, event, meta, local, path, stack)?;

        if segments.is_empty() {
            if let ast::Path::Event(segments) = path {
                if segments.segments.is_empty() {
                    *event = value.clone();
                    return Ok(event);
                }
            }
        }
        let mut current: &mut Value = match path {
            ast::Path::Local(_path) => local,
            ast::Path::Meta(_path) => meta,
            ast::Path::Event(_path) => event,
        };

        let segments = &mut segments.iter().peekable();
        'assign_next: while let Some(segment) = segments.next() {
            if segments.peek().is_none() {
                match segment {
                    NormalizedSegment::FieldRef { id, start, end } => {
                        let id = id.to_string();
                        match current {
                            Value::Object(ref mut o) => {
                                o.insert(id.to_string().into(), value.clone());
                                if let Some(v) = o.get(id.as_str()) {
                                    return Ok(v);
                                } else {
                                    // NOTE We just added this so we know that it is in `o`
                                    unreachable!()
                                }
                            }
                            other => {
                                return self.error_type_conflict(
                                    &Expr::dummy(*start, *end),
                                    other.kind(),
                                    ValueType::Object,
                                )
                            }
                        }
                    }
                    NormalizedSegment::Index { start, end, .. } => {
                        return self.error_assign_array(&Expr::dummy(*start, *end))
                    }
                    NormalizedSegment::Range { start, end, .. } => {
                        return self.error_assign_array(&Expr::dummy(*start, *end))
                    }
                }
            } else {
                match segment {
                    NormalizedSegment::FieldRef { id, start, end } => {
                        if let Value::Object(ref mut map) = current {
                            if map.contains_key(id.as_str()) {
                                current = if let Some(v) = map.get_mut(id.as_str()) {
                                    v
                                } else {
                                    /* NOTE The code we want here is the following
                                    but rust does not allow that because it's stupid
                                    so we have to work around it.

                                      if let Some(v) = map.get_mut(id.as_str()) {
                                        current = v;
                                        continue 'assign_next;
                                      } else { ... }

                                     */
                                    unreachable!()
                                };
                                continue 'assign_next;
                            } else {
                                map.insert(id.to_string().into(), Value::Object(hashmap! {}));
                                // NOTE this is safe because we just added this element
                                // to the map.
                                current = if let Some(v) = map.get_mut(id.as_str()) {
                                    v
                                } else {
                                    unreachable!()
                                };
                                continue 'assign_next;
                            }
                        } else {
                            return self.error_type_conflict(
                                &Expr::dummy(*start, *end),
                                current.kind(),
                                ValueType::Object,
                            );
                        }
                    }
                    NormalizedSegment::Index { start, end, .. } => {
                        return self.error_assign_array(&Expr::dummy(*start, *end))
                    }
                    NormalizedSegment::Range { start, end, .. } => {
                        return self.error_assign_array(&Expr::dummy(*start, *end))
                    }
                }
            }
        }
        self.error_invalid_assign_target()
    }

    fn run(
        &'script self,

        context: &'run Ctx,
        event: &'run mut Value<'event>,
        meta: &'run mut Value<'event>,
        local: &'run mut Value<'event>,
        stack: &'run ValueStack<'event>,
    ) -> Result<Cont<'run, 'event>> {
        match self {
            Expr::Emit(expr) => match expr.borrow() {
                ast::EmitExpr {
                    expr: Expr::Path(ast::Path::Event(ast::EventPath { segments, .. })),
                    port,
                    ..
                } if segments.is_empty() => Ok(Cont::EmitEvent(port.clone())),
                expr => Ok(Cont::Emit(
                    demit!(expr.expr.run(context, event, meta, local, stack)?).clone(),
                    expr.port.clone(),
                )),
            },
            Expr::Drop { .. } => Ok(Cont::Drop),
            Expr::Literal(literal) => self
                .literal(context, event, meta, local, stack, literal)
                .map(Cont::Cont),
            Expr::Assign(expr) => {
                // NOTE Since we are assigning a new value we do cline here.
                // This is intended behaviour
                let value = demit!(expr.expr.run(context, event, meta, local, stack)?).clone();
                self.assign(context, event, meta, local, &expr.path, value, stack)
                    .map(Cont::Cont)
            }
            Expr::Path(path) => self
                .resolve(context, event, meta, local, path, stack)
                .map(Cont::Cont),
            Expr::RecordExpr(ref record) => {
                let mut object: Map = hashmap! {};
                for field in &record.fields {
                    let result = field
                        .value
                        .run(context, event, meta, local, stack)?
                        .into_value(&self, &field.value)?;
                    let key = field.name.clone();
                    object.insert(key.to_string().into(), result.clone());
                }
                Ok(Cont::Cont(stack.push(Value::Object(object))))
            }
            Expr::Invoke(ref call) => self
                .invoke(context, event, meta, local, stack, call)
                .map(Cont::Cont),
            Expr::Unary(ref expr) => self
                .unary(context, event, meta, local, stack, expr)
                .map(Cont::Cont),
            Expr::Binary(ref expr) => self
                .binary(context, event, meta, local, stack, expr)
                .map(Cont::Cont),
            Expr::MatchExpr(ref expr) => self.match_expr(context, event, meta, local, stack, expr),
            Expr::PatchExpr(ref expr) => self
                .patch(context, event, meta, local, stack, expr)
                .map(Cont::Cont),
            Expr::MergeExpr(ref expr) => self
                .merge(context, event, meta, local, stack, expr)
                .map(Cont::Cont),
            Expr::Comprehension(ref expr) => {
                self.comprehension(context, event, meta, local, stack, expr)
            }
            // Expr::PatternExpr(ref rp) => {
            // self.match_rp_expr(context, event, meta, local, stack, rp);
            //     match self.rp(context, event, meta, local, stack, &target, rp)? {
            //         PredicateCont::NoMatch => {
            //             Ok(Cont::Cont(Value::Null));
            //         }
            //         PredicateCont::Match => {
            //             let mut r = &Value::Null;
            //             for expr in &predicate.exprs {
            //                 r = demit!(expr.run(context, event, meta, local, stack,)?);
            //             }
            //             return Ok(Cont::Cont(r));
            //         }
            //     }
            // FIXME
            // Ok(Cont::Cont(Value::Null))
            // }
            _ => {
                // TODO FIXME replace with an error ( illegal expr )
                Ok(Cont::Cont(&Value::Null))
            }
        }
    }
}
