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

use crate::{
    ast::warning,
    registry::{Aggr as AggrRegistry, FResult, TremorAggrFn, TremorAggrFnWrapper},
};

use crate::{prelude::*, tremor_fn};

use std::ops::RangeInclusive;

#[derive(Clone, Debug, Default)]
struct First(Option<Value<'static>>);
impl TremorAggrFn for First {
    fn accumulate(&mut self, args: &[&Value]) -> FResult<()> {
        if self.0.is_none() {
            self.0 = args.first().map(|v| (*v).clone_static());
        }
        Ok(())
    }
    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        Ok(self.0.as_ref().map_or_else(Value::null, Clone::clone))
    }
    fn emit_and_init<'event>(&mut self) -> FResult<Value<'event>> {
        let mut r = None;
        std::mem::swap(&mut r, &mut self.0);
        Ok(r.unwrap_or_else(Value::null))
    }
    fn init(&mut self) {
        self.0 = None;
    }
    fn merge(&mut self, src: &dyn TremorAggrFn) -> FResult<()> {
        // On self is earlier then other, so we don't do anything unless
        // we had no value
        if let Some(other) = src.downcast_ref::<Self>() {
            if self.0.is_none() {
                self.0 = other.0.clone();
            }
        }
        Ok(())
    }

    fn boxed_clone(&self) -> Box<dyn TremorAggrFn> {
        Box::new(self.clone())
    }
    fn arity(&self) -> RangeInclusive<usize> {
        1..=1
    }
}

#[derive(Clone, Debug, Default)]
struct Last(Option<Value<'static>>);
impl TremorAggrFn for Last {
    fn accumulate(&mut self, args: &[&Value]) -> FResult<()> {
        self.0 = args.first().map(|v| (*v).clone_static());
        Ok(())
    }
    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        Ok(self.0.as_ref().map_or_else(Value::null, Clone::clone))
    }
    fn emit_and_init<'event>(&mut self) -> FResult<Value<'event>> {
        let mut r = None;
        std::mem::swap(&mut r, &mut self.0);
        Ok(r.unwrap_or_else(Value::null))
    }
    fn init(&mut self) {
        self.0 = None;
    }
    fn merge(&mut self, src: &dyn TremorAggrFn) -> FResult<()> {
        if let Some(other) = src.downcast_ref::<Self>() {
            // On self is earlier then other, so as long
            // as other has a value we take it
            if other.0.is_some() {
                self.0 = other.0.clone();
            }
        }
        Ok(())
    }

    fn boxed_clone(&self) -> Box<dyn TremorAggrFn> {
        Box::new(self.clone())
    }
    fn arity(&self) -> RangeInclusive<usize> {
        1..=1
    }
}

#[derive(Clone, Debug, Default)]
struct CollectFlattened(Vec<Value<'static>>);
impl TremorAggrFn for CollectFlattened {
    fn accumulate(&mut self, args: &[&Value]) -> FResult<()> {
        if let Some(a) = args.first().map(|v| (*v).clone_static()) {
            self.0.push(a);
        }
        Ok(())
    }
    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        Ok(Value::from(self.0.clone()))
    }
    fn emit_and_init<'event>(&mut self) -> FResult<Value<'event>> {
        let mut r = Vec::with_capacity(self.0.len());
        std::mem::swap(&mut r, &mut self.0);
        Ok(Value::from(r))
    }
    fn init(&mut self) {
        self.0 = Vec::with_capacity(self.0.len());
    }

    fn merge(&mut self, src: &dyn TremorAggrFn) -> FResult<()> {
        if let Some(other) = src.downcast_ref::<Self>() {
            // On self is earlier then other, so as long
            // as other has a value we take it
            self.0.append(&mut other.0.clone());
        }
        Ok(())
    }

    fn boxed_clone(&self) -> Box<dyn TremorAggrFn> {
        Box::new(self.clone())
    }
    fn arity(&self) -> RangeInclusive<usize> {
        1..=1
    }
    fn warning(&self) -> Option<(warning::Class, String)> {
        Some((
            warning::Class::Performance,
            String::from("Collect functions are very expensive memory wise, try avoiding them."),
        ))
    }
}

#[derive(Clone, Debug, Default)]
struct CollectNested(Vec<Value<'static>>);
impl TremorAggrFn for CollectNested {
    fn accumulate(&mut self, args: &[&Value]) -> FResult<()> {
        if let Some(a) = args.first().map(|v| (*v).clone_static()) {
            self.0.push(a);
        }
        Ok(())
    }
    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        Ok(Value::Array(self.0.clone()))
    }
    fn emit_and_init<'event>(&mut self) -> FResult<Value<'event>> {
        let mut r = Vec::with_capacity(self.0.len());
        std::mem::swap(&mut r, &mut self.0);
        Ok(Value::from(r))
    }
    fn init(&mut self) {
        self.0 = Vec::with_capacity(self.0.len());
    }

    fn merge(&mut self, src: &dyn TremorAggrFn) -> FResult<()> {
        if let Some(other) = src.downcast_ref::<Self>() {
            // On self is earlier then other, so as long
            // as other has a value we take it
            self.0.push(Value::from(other.0.clone()));
        }
        Ok(())
    }

    fn boxed_clone(&self) -> Box<dyn TremorAggrFn> {
        Box::new(self.clone())
    }
    fn arity(&self) -> RangeInclusive<usize> {
        1..=1
    }
    fn warning(&self) -> Option<(warning::Class, String)> {
        Some((
            warning::Class::Performance,
            String::from("Collect functions are very expensive memory wise, try avoiding them."),
        ))
    }
}

pub fn load_aggr(registry: &mut AggrRegistry) {
    registry
        .insert(TremorAggrFnWrapper::new(
            "win".to_string(),
            "first".to_string(),
            Box::<First>::default(),
        ))
        .insert(TremorAggrFnWrapper::new(
            "win".to_string(),
            "collect_flattened".to_string(),
            Box::<CollectFlattened>::default(),
        ))
        .insert(TremorAggrFnWrapper::new(
            "win".to_string(),
            "collect_nested".to_string(),
            Box::<CollectNested>::default(),
        ))
        .insert(TremorAggrFnWrapper::new(
            "win".to_string(),
            "last".to_string(),
            Box::<Last>::default(),
        ));
}

pub fn load(registry: &mut Registry) {
    registry.insert(tremor_fn! (win|cardinality(ctx) {
            Ok(Value::from(ctx.cardinality))

    }));
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::registry::fun;
    use crate::registry::FResult as Result;
    use crate::Value;

    #[test]
    fn first() -> Result<()> {
        let mut a = First::default();
        a.init();
        let one = Value::from(1);
        let two = Value::from(2);
        let three = Value::from(3);
        let four = Value::from(4);
        a.accumulate(&[&one])?;
        a.accumulate(&[&two])?;
        a.accumulate(&[&three])?;
        assert_eq!(a.emit_and_init()?, 1);
        a.accumulate(&[&four])?;
        a.accumulate(&[&three])?;
        assert_eq!(a.emit_and_init()?, 4);

        Ok(())
    }

    #[test]
    fn collect() -> Result<()> {
        let mut a = CollectFlattened::default();
        a.init();
        let one = Value::from(1);
        let two = Value::from(2);
        let three = Value::from(3);
        let four = Value::from(4);
        a.accumulate(&[&one])?;
        a.accumulate(&[&two])?;
        a.accumulate(&[&three])?;
        let r = a.emit_and_init()?;
        assert_eq!(r.get_idx(0).as_u64(), Some(1));
        assert_eq!(r.get_idx(1).as_u64(), Some(2));
        assert_eq!(r.get_idx(2).as_u64(), Some(3));
        a.accumulate(&[&four])?;
        a.accumulate(&[&three])?;
        let r = a.emit_and_init()?;
        assert_eq!(r.get_idx(0).as_u64(), Some(4));
        assert_eq!(r.get_idx(1).as_u64(), Some(3));
        Ok(())
    }

    #[test]
    fn last() -> Result<()> {
        let mut a = Last::default();
        a.init();
        let one = Value::from(1);
        let two = Value::from(2);
        let three = Value::from(3);
        let four = Value::from(4);
        a.accumulate(&[&one])?;
        a.accumulate(&[&two])?;
        a.accumulate(&[&three])?;
        assert_eq!(a.emit_and_init()?, 3);
        a.accumulate(&[&four])?;
        a.accumulate(&[&two])?;
        assert_eq!(a.emit_and_init()?, 2);
        Ok(())
    }

    #[test]
    fn first_merge() -> Result<()> {
        let mut a = First::default();
        let mut b = First::default();
        a.init();
        b.init();

        a.accumulate(&[&Value::from(1)])?;
        a.accumulate(&[&Value::from(2)])?;
        b.accumulate(&[&Value::from(3)])?;

        a.merge(&b)?;

        assert_eq!(Value::from(1), a.emit()?);

        Ok(())
    }

    #[test]
    fn collect_merge() -> Result<()> {
        let mut a = CollectFlattened::default();
        let mut b = CollectFlattened::default();
        a.init();
        b.init();

        a.accumulate(&[&Value::from(1)])?;
        a.accumulate(&[&Value::from(2)])?;
        b.accumulate(&[&Value::from(3)])?;

        a.merge(&b)?;

        let result = a.emit()?;
        assert_eq!(result.get_idx(0).as_u64(), Some(1));
        assert_eq!(result.get_idx(1).as_u64(), Some(2));
        assert_eq!(result.get_idx(2).as_u64(), Some(3));

        Ok(())
    }

    #[test]
    fn last_merge() -> Result<()> {
        let mut a = Last::default();
        let mut b = Last::default();
        a.init();
        b.init();

        a.accumulate(&[&Value::from(1)])?;
        a.accumulate(&[&Value::from(2)])?;
        b.accumulate(&[&Value::from(3)])?;

        a.merge(&b)?;

        assert_eq!(Value::from(3), a.emit()?);

        Ok(())
    }

    #[test]
    fn cardinality() {
        let f = fun("win", "cardinality");

        assert_val!(f(&[]), 0);
    }
}
