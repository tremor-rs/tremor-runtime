// Copyright 2020-2021, The Tremor Team
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

use super::super::prelude::*;
use crate::ast::{BooleanBinExpr, BooleanBinOpKind};
use crate::{
    ast::{base_expr::Ranged, binary::extend_bytes_from_value, NodeMeta},
    errors::{
        err_generic, err_need_int, error_array_out_of_bound, error_bad_key, error_decreasing_range,
        error_need_arr, error_need_obj,
    },
    interpreter::{exec_binary, exec_unary, Env},
    lexer::Span,
    EventContext, Value, NO_AGGRS, NO_CONSTS,
};
use simd_json::prelude::*;
use simd_json_derive::Serialize;
use tremor_value::KnownKey;

/// Walks a AST and performs constant folding on arguments
pub struct ConstFolder<'run, 'script> {
    /// Function Registry
    pub helper: &'run Helper<'script, 'run>,
}

fn fake_path(mid: &NodeMeta) -> Path {
    Path::Expr(ExprPath {
        expr: Box::new(ImutExpr::Local {
            idx: 0,
            mid: Box::new(mid.clone()),
        }),
        segments: vec![],
        var: 0,
        mid: Box::new(mid.clone()),
    })
}
impl<'run, 'script: 'run> DeployWalker<'script> for ConstFolder<'run, 'script> {}
impl<'run, 'script: 'run> QueryWalker<'script> for ConstFolder<'run, 'script> {}
impl<'run, 'script: 'run> ExprWalker<'script> for ConstFolder<'run, 'script> {}
impl<'run, 'script: 'run> ImutExprWalker<'script> for ConstFolder<'run, 'script> {}
impl<'run, 'script: 'run> DeployVisitor<'script> for ConstFolder<'run, 'script> {}
impl<'run, 'script: 'run> QueryVisitor<'script> for ConstFolder<'run, 'script> {}
impl<'run, 'script: 'run> ExprVisitor<'script> for ConstFolder<'run, 'script> {}

impl<'run, 'script: 'run> ImutExprVisitor<'script> for ConstFolder<'run, 'script> {
    #[allow(clippy::too_many_lines, clippy::match_same_arms)]
    fn leave_expr(&mut self, e: &mut ImutExpr<'script>) -> Result<()> {
        use ImutExpr::Literal as Lit;
        let mut buf = Lit(Literal {
            value: Value::const_null(),
            mid: Box::new(e.meta().clone()),
        });
        std::mem::swap(&mut buf, e);
        *e = match buf {
            // Datatypes
            ImutExpr::Record(Record { mid, base, fields }) if fields.is_empty() => Lit(Literal {
                value: base.into(),
                mid,
            }),
            e @ ImutExpr::Record(_) => e,

            ImutExpr::List(List { exprs, mid }) if exprs.iter().all(ImutExpr::is_lit) => {
                let value = exprs
                    .into_iter()
                    .filter_map(ImutExpr::into_lit)
                    .collect::<Vec<_>>()
                    .into();
                Lit(Literal { mid, value })
            }
            e @ ImutExpr::List(_) => e,

            ImutExpr::Bytes(Bytes { mid, value }) if value.iter().all(BytesPart::is_lit) => {
                let mut bytes: Vec<u8> = Vec::with_capacity(value.len());
                let outer = e.extent();
                let mut used = 0;
                let mut buf = 0;

                for (value, data_type, endianess, bits, inner) in
                    value.into_iter().filter_map(|part| {
                        let inner = part.extent();
                        let value = part.data.into_lit()?;
                        Some((value, part.data_type, part.endianess, part.bits, inner))
                    })
                {
                    extend_bytes_from_value(
                        &outer, &inner, data_type, endianess, bits, &mut buf, &mut used,
                        &mut bytes, &value,
                    )?;
                }
                if used > 0 {
                    bytes.push(buf >> (8 - used));
                }
                ImutExpr::literal(mid, Value::Bytes(bytes.into()))
            }
            e @ ImutExpr::Bytes(_) => e,

            // Operations
            ImutExpr::Binary(b) => {
                if let BinExpr {
                    mid,
                    kind,
                    lhs: Lit(Literal { value: lhs, .. }),
                    rhs: Lit(Literal { value: rhs, .. }),
                    ..
                } = b.as_ref()
                {
                    let value = exec_binary(b.as_ref(), b.as_ref(), *kind, lhs, rhs)?.into_owned();
                    Lit(Literal {
                        mid: mid.clone(),
                        value,
                    })
                } else {
                    ImutExpr::Binary(b)
                }
            }

            ImutExpr::BinaryBoolean(b) => {
                if let BooleanBinExpr {
                    mid,
                    kind,
                    lhs: Lit(Literal { value: lhs, .. }),
                    rhs: Lit(Literal { value: rhs, .. }),
                } = b.as_ref()
                {
                    let lhs = lhs.as_bool();
                    let rhs = rhs.as_bool();

                    match (kind, lhs, rhs) {
                        (BooleanBinOpKind::Or, Some(lhs), Some(rhs)) => {
                            ImutExpr::literal(mid.clone(), Value::from(lhs || rhs))
                        }
                        (BooleanBinOpKind::Xor, Some(lhs), Some(rhs)) => {
                            ImutExpr::literal(mid.clone(), Value::from(lhs ^ rhs))
                        }
                        (BooleanBinOpKind::And, Some(lhs), Some(rhs)) => {
                            ImutExpr::literal(mid.clone(), Value::from(lhs && rhs))
                        }
                        _ => ImutExpr::BinaryBoolean(b),
                    }
                } else {
                    ImutExpr::BinaryBoolean(b)
                }
            }

            ImutExpr::Unary(b) => {
                if let UnaryExpr {
                    kind,
                    expr: Lit(Literal { value, .. }),
                    mid,
                } = b.as_ref()
                {
                    let value = exec_unary(b.as_ref(), b.as_ref(), *kind, value)?.into_owned();
                    Lit(Literal {
                        mid: mid.clone(),
                        value,
                    })
                } else {
                    ImutExpr::Unary(b)
                }
            }
            ImutExpr::String(StringLit { mid, elements })
                if elements.iter().all(StrLitElement::is_lit) =>
            {
                let value = elements
                    .iter()
                    .filter_map(StrLitElement::as_str)
                    .collect::<Vec<_>>()
                    .join("")
                    .into();
                Lit(Literal { mid, value })
            }
            // handled below
            ImutExpr::Path(Path::Expr(ExprPath {
                expr,
                segments,
                mid,
                var,
            })) if expr.is_lit() => {
                let value = expr.try_into_value(self.helper)?;
                let outer = e.extent();
                let (value, segments) = reduce_path(outer, value, segments)?;
                let lit = ImutExpr::literal(mid.clone(), value);
                if segments.is_empty() {
                    lit
                } else {
                    ImutExpr::Path(Path::Expr(ExprPath {
                        expr: Box::new(lit),
                        segments,
                        mid,
                        var,
                    }))
                }
            }

            ImutExpr::Invoke(i)
            | ImutExpr::Invoke1(i)
            | ImutExpr::Invoke2(i)
            | ImutExpr::Invoke3(i)
                if i.invocable.is_const() && i.args.iter().all(ImutExpr::is_lit) =>
            {
                let ex = i.extent();

                let args: Vec<Value<'script>> =
                    i.args.into_iter().filter_map(ImutExpr::into_lit).collect();
                let args2: Vec<&Value<'script>> = args.iter().collect();
                let env = Env {
                    context: &EventContext::default(),
                    consts: NO_CONSTS.run(),
                    aggrs: &NO_AGGRS,
                    recursion_limit: crate::recursion_limit(),
                };

                let v = i
                    .invocable
                    .invoke(&env, &args2)
                    .map_err(|e| e.into_err(&ex, &ex, Some(self.helper.reg)))?;
                ImutExpr::literal(i.mid, v)
            }
            e @ (ImutExpr::Path(_)
            | ImutExpr::String(_)
            | ImutExpr::Patch(_)
            | ImutExpr::Match(_)
            | ImutExpr::Comprehension(_)
            | ImutExpr::Merge(_)
            | ImutExpr::Local { .. }
            | ImutExpr::Present { .. }
            | ImutExpr::Invoke(_)
            | ImutExpr::Invoke1(_)
            | ImutExpr::Invoke2(_)
            | ImutExpr::Invoke3(_)
            | ImutExpr::Literal(_)
            | ImutExpr::InvokeAggr(_)
            | ImutExpr::Recur(_)
            | ImutExpr::ArrayAppend { .. }) => e,
        };
        Ok(())
    }

    /// Reduce path segments by:
    /// 1) ranges, if both start and end are known turn `RangeExpr`'s into Ranges with the correct
    ///    start & end
    /// 2) Element lookups, if the expressionis a literal string, becomes a key lookup
    /// 2) Element lookups, if the expressionis a literal usize, becomes a idx lookup
    fn leave_segment(&mut self, segment: &mut Segment<'script>) -> Result<()> {
        let mut buf = Segment::Idx {
            mid: Box::new(segment.meta().clone()),
            idx: 0,
        };
        std::mem::swap(&mut buf, segment);
        *segment = match buf {
            Segment::RangeExpr {
                start, end, mid, ..
            } if start.is_lit() && end.is_lit() => {
                let inner = start.extent();
                let start = start.try_into_value(self.helper)?;
                let got = start.value_type();
                let start = start
                    .as_usize()
                    .ok_or_else(|| err_need_int(segment, &inner, got))?;

                let inner = end.extent();
                let end = end.try_into_value(self.helper)?;
                let got = end.value_type();
                let end = end
                    .as_usize()
                    .ok_or_else(|| err_need_int(segment, &inner, got))?;

                if end < start {
                    return error_decreasing_range(segment, segment, &fake_path(&mid), start, end);
                }
                Segment::Range { start, end, mid }
            }
            Segment::Element { expr, mid, .. } if expr.as_lit().is_some_and(Value::is_usize) => {
                let idx = expr
                    .as_lit()
                    .and_then(Value::as_usize)
                    .ok_or("unreachable error is int and not int")?;
                Segment::Idx { idx, mid }
            }
            Segment::Element { expr, mid, .. } if expr.as_lit().is_some_and(Value::is_str) => {
                if let Some(Value::String(key)) = expr.into_lit() {
                    let key = KnownKey::from(key);
                    Segment::Id { key, mid }
                } else {
                    return Err("unreachable error is str and not str".into());
                }
            }
            other => other,
        };
        Ok(())
    }

    /// Reduce a string element by turning all expressions that hold a literal expression
    /// into literal strings
    fn leave_string_element(&mut self, string: &mut StrLitElement<'script>) -> Result<()> {
        let mut old = StrLitElement::Lit("placeholder".into());
        std::mem::swap(&mut old, string);
        match old {
            StrLitElement::Lit(l) => *string = StrLitElement::Lit(l),
            StrLitElement::Expr(e) if e.is_lit() => {
                let value = e.try_into_value(self.helper)?;
                match value {
                    Value::String(s) => *string = StrLitElement::Lit(s),
                    // TODO: The float scenario is different in erlang and rust
                    // We knowingly excluded float correctness in string interpolation
                    // as we don't want to over engineer and write own format functions.
                    // any suggestions are welcome
                    #[cfg(feature = "erlang-float-testing")]
                    Value::Static(value_trait::StaticNode::F64(_float)) => {
                        *string = StrLitElement::Lit("42".into());
                    }
                    other => *string = StrLitElement::Lit(other.json_string()?.into()),
                }
            }
            other @ StrLitElement::Expr(_) => *string = other,
        }
        Ok(())
    }
    /// Reduce a string by concatinating strings of all adjacent literal strings into one
    fn leave_string(&mut self, string: &mut StringLit<'script>) -> Result<()> {
        let mut old = Vec::with_capacity(string.elements.len());
        std::mem::swap(&mut old, &mut string.elements);
        for next in old {
            if let StrLitElement::Lit(next_lit) = next {
                if let Some(prev) = string.elements.pop() {
                    match prev {
                        StrLitElement::Lit(l) => {
                            let mut o = l.into_owned();
                            o.push_str(&next_lit);
                            string.elements.push(StrLitElement::Lit(o.into()));
                        }
                        prev @ StrLitElement::Expr(..) => {
                            string.elements.push(prev);
                            string.elements.push(StrLitElement::Lit(next_lit));
                        }
                    }
                } else {
                    string.elements.push(StrLitElement::Lit(next_lit));
                }
            } else {
                string.elements.push(next);
            }
        }
        Ok(())
    }

    /// Reduce a record by taking all fields that have a constant string as key, and a literal as
    /// value and move them into the base
    fn leave_record(&mut self, record: &mut Record<'script>) -> Result<()> {
        let mut old_fields = Vec::with_capacity(record.fields.len());
        std::mem::swap(&mut old_fields, &mut record.fields);

        for field in old_fields {
            match field {
                Field { name, value, .. } if name.as_str().is_some() && value.is_lit() => {
                    let k = name.into_str().ok_or("unreachable error str and not str")?;
                    let v = value.try_into_value(self.helper)?;
                    record.base.insert(k, v);
                }
                other => record.fields.push(other),
            }
        }
        Ok(())
    }
}

impl<'run, 'script> ConstFolder<'run, 'script>
where
    'script: 'run,
{
    pub(crate) fn reduce_to_val(
        helper: &'run Helper<'script, '_>,
        mut expr: ImutExpr<'script>,
    ) -> Result<Value<'script>> {
        ImutExprWalker::walk_expr(&mut ConstFolder::new(helper), &mut expr)?;
        expr.try_into_value(helper)
    }
    /// New folder
    #[must_use]
    pub fn new(helper: &'run Helper<'script, '_>) -> Self {
        ConstFolder { helper }
    }
}

fn reduce_path<'script>(
    outer: Span,
    mut value: Value<'script>,
    segments: Vec<Segment<'script>>,
) -> Result<(Value<'script>, Vec<Segment<'script>>)> {
    let mut segments = segments.into_iter().peekable();
    let mut subrange: Option<&[Value<'script>]> = None;

    while let Some(segment) = segments.peek() {
        match segment {
            Segment::Id { key, mid } => {
                if !value.is_object() {
                    return error_need_obj(&outer, segment, value.value_type());
                }
                if subrange.is_some() {
                    return err_generic(&outer, segment, &"We can't index a name into a subrange.");
                }
                if let Some(v) = key.lookup(&value) {
                    subrange = None;
                    value = v.clone();
                } else {
                    return error_bad_key(
                        &outer,
                        segment,
                        &fake_path(mid),
                        key.key().to_string(),
                        value
                            .as_object()
                            .map(|o| o.keys().map(ToString::to_string).collect::<Vec<_>>())
                            .unwrap_or_default(),
                    );
                }
            }
            Segment::Idx { idx, mid } => {
                if let Some(a) = value.as_array() {
                    let range_to_consider = subrange.unwrap_or(a.as_slice());
                    if let Some(c) = range_to_consider.get(*idx) {
                        value = c.clone();
                        subrange = None;
                    } else {
                        let r = *idx..*idx;
                        let l = range_to_consider.len();
                        return error_array_out_of_bound(&outer, segment, &fake_path(mid), r, l);
                    }
                } else {
                    return error_need_arr(&outer, segment, value.value_type());
                }
            }
            Segment::Range { start, end, mid } => {
                if let Some(a) = value.as_array() {
                    let array = subrange.unwrap_or(a.as_slice());
                    let start = *start;
                    let end = *end;
                    if end > array.len() {
                        let r = start..end;
                        let l = array.len();
                        return error_array_out_of_bound(&outer, segment, &fake_path(mid), r, l);
                    }
                    subrange = array.get(start..end);
                } else {
                    return error_need_arr(&outer, segment, value.value_type());
                }
            }

            Segment::RangeExpr { .. } | Segment::Element { .. } => {
                break;
            }
        }
        segments.next();
    }
    if let Some(arr) = subrange {
        Ok((Value::from(arr.to_vec()), segments.collect()))
    } else {
        Ok((value, segments.collect()))
    }
}
