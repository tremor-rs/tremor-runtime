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

use crate::registry::{AggrRegistry, FResult, TremorAggrFn, TremorAggrFnWrapper};

use simd_json::BorrowedValue as Value;
use std::ops::RangeInclusive;

#[derive(Clone, Debug, Default)]
struct First(Option<Value<'static>>);
impl TremorAggrFn for First {
    fn accumulate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
        if self.0.is_none() {
            self.0 = Some(args[0].clone_static());
        }
        Ok(())
    }
    fn compensate<'event>(&mut self, _args: &[&Value<'event>]) -> FResult<()> {
        // FIXME: how?
        Ok(())
    }
    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        if let Some(v) = &self.0 {
            Ok(v.clone())
        } else {
            Ok(Value::Null)
        }
    }
    fn emit_and_init<'event>(&mut self) -> FResult<Value<'event>> {
        let mut r = None;
        std::mem::swap(&mut r, &mut self.0);
        if let Some(r) = r {
            Ok(r)
        } else {
            Ok(Value::Null)
        }
    }
    fn init(&mut self) {
        self.0 = None;
    }
    fn snot_clone(&self) -> Box<dyn TremorAggrFn> {
        Box::new(self.clone())
    }
    fn arity(&self) -> RangeInclusive<usize> {
        1..=1
    }
}

#[derive(Clone, Debug, Default)]
struct Last(Value<'static>);
impl TremorAggrFn for Last {
    fn accumulate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
        self.0 = args[0].clone_static();
        Ok(())
    }
    fn compensate<'event>(&mut self, _args: &[&Value<'event>]) -> FResult<()> {
        // FIXME: how?
        Ok(())
    }
    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        Ok(self.0.clone())
    }
    fn emit_and_init<'event>(&mut self) -> FResult<Value<'event>> {
        let mut r = Value::Null;
        std::mem::swap(&mut r, &mut self.0);
        Ok(r)
    }
    fn init(&mut self) {
        self.0 = Value::Null;
    }
    fn snot_clone(&self) -> Box<dyn TremorAggrFn> {
        Box::new(self.clone())
    }
    fn arity(&self) -> RangeInclusive<usize> {
        1..=1
    }
}

#[derive(Clone, Debug, Default)]
struct Collect(Vec<Value<'static>>);
impl TremorAggrFn for Collect {
    fn accumulate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
        self.0.push(args[0].clone_static());
        Ok(())
    }
    fn compensate<'event>(&mut self, _args: &[&Value<'event>]) -> FResult<()> {
        // FIXME: how?
        Ok(())
    }
    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        Ok(Value::Array(self.0.clone()))
    }
    fn emit_and_init<'event>(&mut self) -> FResult<Value<'event>> {
        let mut r = Vec::with_capacity(self.0.len());
        std::mem::swap(&mut r, &mut self.0);
        Ok(Value::Array(r))
    }
    fn init(&mut self) {
        self.0 = Vec::with_capacity(self.0.len());
    }
    fn snot_clone(&self) -> Box<dyn TremorAggrFn> {
        Box::new(self.clone())
    }
    fn arity(&self) -> RangeInclusive<usize> {
        1..=1
    }
}
pub fn load_aggr(registry: &mut AggrRegistry) {
    registry
        .insert(TremorAggrFnWrapper {
            module: "win".to_string(),
            name: "first".to_string(),
            fun: Box::new(First::default()),
        })
        .insert(TremorAggrFnWrapper {
            module: "win".to_string(),
            name: "collect".to_string(),
            fun: Box::new(Collect::default()),
        })
        .insert(TremorAggrFnWrapper {
            module: "win".to_string(),
            name: "last".to_string(),
            fun: Box::new(Last::default()),
        });
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::registry::FResult as Result;
    use simd_json::value::ValueTrait;
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
        let mut a = Collect::default();
        a.init();
        let one = Value::from(1);
        let two = Value::from(2);
        let three = Value::from(3);
        let four = Value::from(4);
        a.accumulate(&[&one])?;
        a.accumulate(&[&two])?;
        a.accumulate(&[&three])?;
        let r = a.emit_and_init()?;
        assert_eq!(r.get_idx(0).unwrap(), &1u8);
        assert_eq!(r.get_idx(1).unwrap(), &2u8);
        assert_eq!(r.get_idx(2).unwrap(), &3u8);
        a.accumulate(&[&four])?;
        a.accumulate(&[&three])?;
        let r = a.emit_and_init()?;
        assert_eq!(r.get_idx(0).unwrap(), &4);
        assert_eq!(r.get_idx(1).unwrap(), &3);
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
}
