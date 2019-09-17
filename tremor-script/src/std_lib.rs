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

mod array;
mod chash;
mod datetime;
mod dummy;
mod float;
mod integer;
mod json;
mod math;
mod random;
mod re;
mod record;
mod string;
mod r#type;

use crate::registry::{AggrRegistry, Context, Registry};

pub fn load<Ctx: 'static + Context>(registry: &mut Registry<Ctx>) {
    array::load(registry);
    chash::load(registry);
    datetime::load(registry);
    dummy::load(registry);
    integer::load(registry);
    float::load(registry);
    json::load(registry);
    math::load(registry);
    r#type::load(registry);
    random::load(registry);
    re::load(registry);
    record::load(registry);
    string::load(registry);
}

pub fn load_aggr(registry: &mut AggrRegistry) {
    use crate::registry::{FResult, TremorAggrFn, TremorAggrFnWrapper};
    use simd_json::BorrowedValue as Value;

    #[derive(Clone)]
    struct AggrCount(i64);
    impl TremorAggrFn for AggrCount {
        fn accumulate<'event>(&mut self, _arg: &Value<'event>) -> FResult<()> {
            self.0 += 1;
            Ok(())
        }
        fn compensate<'event>(&mut self, _arg: &Value<'event>) -> FResult<()> {
            self.0 += 1;
            Ok(())
        }
        fn emit<'event>(&self) -> FResult<Value<'event>> {
            Ok(Value::I64(self.0))
        }
        fn init(&mut self) {
            self.0 = 0;
        }
        fn snot_clone(&self) -> Box<dyn TremorAggrFn> {
            Box::new(self.clone())
        }
    }

    #[derive(Clone)]
    struct AggrSum(f64);
    impl TremorAggrFn for AggrSum {
        fn accumulate<'event>(&mut self, arg: &Value<'event>) -> FResult<()> {
            match arg {
                Value::I64(n) => self.0 += *n as f64,
                Value::F64(n) => self.0 += n,
                _ => (),
            }
            Ok(())
        }
        fn compensate<'event>(&mut self, arg: &Value<'event>) -> FResult<()> {
            match arg {
                Value::I64(n) => self.0 -= *n as f64,
                Value::F64(n) => self.0 -= n,
                _ => (),
            }
            Ok(())
        }
        fn emit<'event>(&self) -> FResult<Value<'event>> {
            Ok(Value::F64(self.0))
        }
        fn init(&mut self) {
            self.0 = 0.0;
        }
        fn snot_clone(&self) -> Box<dyn TremorAggrFn> {
            Box::new(self.clone())
        }
    }

    #[derive(Clone, Debug)]
    struct AggrAvg(i64, f64);
    impl TremorAggrFn for AggrAvg {
        fn accumulate<'event>(&mut self, arg: &Value<'event>) -> FResult<()> {
            self.0 += 1;
            match arg {
                Value::I64(n) => self.1 += *n as f64,
                Value::F64(n) => self.1 += n,
                _ => (),
            }
            Ok(())
        }
        fn compensate<'event>(&mut self, arg: &Value<'event>) -> FResult<()> {
            self.0 -= 1;
            match arg {
                Value::I64(n) => self.1 -= *n as f64,
                Value::F64(n) => self.1 -= n,
                _ => (),
            }
            Ok(())
        }
        fn emit<'event>(&self) -> FResult<Value<'event>> {
            dbg!(self);
            if self.0 == 0 {
                Ok(Value::Null)
            } else {
                Ok(Value::F64(self.1 / (self.0 as f64)))
            }
        }
        fn init(&mut self) {
            self.0 = 0;
            self.1 = 0.0;
        }
        fn snot_clone(&self) -> Box<dyn TremorAggrFn> {
            Box::new(self.clone())
        }
    }
    registry
        .insert(TremorAggrFnWrapper {
            module: "stats".to_string(),
            name: "count".to_string(),
            fun: Box::new(AggrCount(0)),
            argc: 1,
        })
        .insert(TremorAggrFnWrapper {
            module: "stats".to_string(),
            name: "sum".to_string(),
            fun: Box::new(AggrSum(0.0)),
            argc: 1,
        })
        .insert(TremorAggrFnWrapper {
            module: "stats".to_string(),
            name: "avg".to_string(),
            fun: Box::new(AggrAvg(0, 0.0)),
            argc: 1,
        });
}
