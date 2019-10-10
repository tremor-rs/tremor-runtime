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

use crate::registry::{
    mfa, AggrRegistry, FResult, FunctionError, TremorAggrFn, TremorAggrFnWrapper,
};
use halfbrown::hashmap;
use hdrhistogram::Histogram;
use simd_json::value::ValueTrait;
use simd_json::BorrowedValue as Value;
use std::f64;
use std::marker::Send;
use std::ops::RangeInclusive;
use std::u64;

#[derive(Clone, Debug, Default)]
struct Count(i64);
impl TremorAggrFn for Count {
    fn accumulate<'event>(&mut self, _args: &[&Value<'event>]) -> FResult<()> {
        self.0 += 1;
        Ok(())
    }
    fn compensate<'event>(&mut self, _args: &[&Value<'event>]) -> FResult<()> {
        self.0 -= 1;
        Ok(())
    }
    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        Ok(Value::I64(self.0))
    }
    fn init(&mut self) {
        self.0 = 0;
    }
    fn merge(&mut self, src: &dyn std::any::Any) -> FResult<()> {
        dbg!("merge");
        if let Some(other) = src.downcast_ref::<Self>() {
            // On self is earlier then other, so as long
            // as other has a value we take it
            self.0 += other.0;
        } else {
            dbg!(src.type_id());
        }
        Ok(())
    }
    fn snot_clone(&self) -> Box<dyn TremorAggrFn> {
        Box::new(self.clone())
    }
    fn arity(&self) -> RangeInclusive<usize> {
        0..=0
    }
}

#[derive(Clone, Debug, Default)]
struct Sum(f64);
impl TremorAggrFn for Sum {
    fn accumulate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
        if let Some(v) = args[0].cast_f64() {
            self.0 += v;
        } else {
            // FIXME type error
        }
        Ok(())
    }
    fn compensate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
        if let Some(v) = args[0].cast_f64() {
            self.0 -= v;
        } else {
            // FIXME type error
        }
        Ok(())
    }
    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        Ok(Value::F64(self.0))
    }
    fn init(&mut self) {
        self.0 = 0.0;
    }
    fn merge(&mut self, src: &dyn std::any::Any) -> FResult<()> {
        if let Some(other) = src.downcast_ref::<Self>() {
            // On self is earlier then other, so as long
            // as other has a value we take it
            self.0 += other.0;
        }
        Ok(())
    }
    fn snot_clone(&self) -> Box<dyn TremorAggrFn> {
        Box::new(self.clone())
    }
    fn arity(&self) -> RangeInclusive<usize> {
        1..=1
    }
}

#[derive(Clone, Debug, Default)]
struct Mean(i64, f64);
impl TremorAggrFn for Mean {
    fn accumulate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
        self.0 += 1;
        if let Some(v) = args[0].cast_f64() {
            self.1 += v;
        } else {
            // FIXME type error
        }
        Ok(())
    }
    fn compensate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
        self.0 -= 1;
        if let Some(v) = args[0].cast_f64() {
            self.1 -= v;
        } else {
            // FIXME type errpr
        }
        Ok(())
    }
    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
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
    fn merge(&mut self, src: &dyn std::any::Any) -> FResult<()> {
        if let Some(other) = src.downcast_ref::<Self>() {
            // On self is earlier then other, so as long
            // as other has a value we take it
            self.0 += other.0;
            self.1 += other.1;
        }
        Ok(())
    }
    fn snot_clone(&self) -> Box<dyn TremorAggrFn> {
        Box::new(self.clone())
    }
    fn arity(&self) -> RangeInclusive<usize> {
        1..=1
    }
}

#[derive(Clone, Debug, Default)]
struct Min(Option<f64>);
impl TremorAggrFn for Min {
    fn accumulate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
        if let Some(v) = args[0].cast_f64() {
            if self.0.is_none() || Some(v) < self.0 {
                self.0 = Some(v);
            };
        } else {
            // FIXME type errpr
        }
        Ok(())
    }
    fn compensate<'event>(&mut self, _args: &[&Value<'event>]) -> FResult<()> {
        // FIXME: how?
        // [a, b, c, d, e, f, g, h, i, j];
        // a -> [(0, a)]
        // b -> [(0, a), (1, b)]
        // c -> [(0, a), (1, b), (2, c)]  ...

        // [d, t, u, a, t, u, b, c, 6];
        // d -> [(0, d)]
        // t -> [(0, d), (1, t)]
        // u -> [(0, d), (1, t), (2, u)]  ...
        // a -> [(3, a)]  ...
        // t -> [(3, a), (4, t)]  ...
        // u -> [(3, a), (4, t), (5, u)]  ...
        // b -> [(3, a), (5, b)]  ...

        Ok(())
    }
    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        Ok(Value::F64(self.0.unwrap_or_default()))
    }
    fn init(&mut self) {
        self.0 = None;
    }
    fn merge(&mut self, src: &dyn std::any::Any) -> FResult<()> {
        if let Some(other) = src.downcast_ref::<Self>() {
            // On self is earlier then other, so as long
            // as other has a value we take it
            if other.0 < self.0 {
                self.0 = other.0;
            }
        }
        Ok(())
    }
    fn snot_clone(&self) -> Box<dyn TremorAggrFn> {
        Box::new(self.clone())
    }
    fn arity(&self) -> RangeInclusive<usize> {
        1..=1
    }
}

#[derive(Clone, Debug, Default)]
struct Max(Option<f64>);
impl TremorAggrFn for Max {
    fn accumulate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
        if let Some(v) = args[0].cast_f64() {
            if self.0.is_none() || Some(v) > self.0 {
                self.0 = Some(v);
            };
        } else {
            // FIXME type errpr
        }
        Ok(())
    }
    fn compensate<'event>(&mut self, _args: &[&Value<'event>]) -> FResult<()> {
        // FIXME: how?
        Ok(())
    }
    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        Ok(Value::F64(self.0.unwrap_or_default()))
    }
    fn init(&mut self) {
        self.0 = None;
    }
    fn merge(&mut self, src: &dyn std::any::Any) -> FResult<()> {
        if let Some(other) = src.downcast_ref::<Self>() {
            // On self is earlier then other, so as long
            // as other has a value we take it
            if other.0 > self.0 {
                self.0 = other.0;
            }
        }
        Ok(())
    }
    fn snot_clone(&self) -> Box<dyn TremorAggrFn> {
        Box::new(self.clone())
    }
    fn arity(&self) -> RangeInclusive<usize> {
        1..=1
    }
}

#[derive(Clone, Debug, Default)]
struct Var {
    n: u64,
    k: f64,
    ex: f64,
    ex2: f64,
}

impl TremorAggrFn for Var {
    fn accumulate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
        if let Some(v) = args[0].cast_f64() {
            if self.n == 0 {
                self.k = v;
            }
            self.n += 1;
            self.ex += v - self.k;
            self.ex2 += (v - self.k) * (v - self.k);
        } else {
            // FIXME type errpr
        }
        Ok(())
    }
    fn compensate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
        if let Some(v) = args[0].cast_f64() {
            self.n -= 1;
            self.ex -= v - self.k;
            self.ex2 -= (v - self.k) * (v - self.k);
        } else {
            // FIXME type errpr
        }
        Ok(())
    }
    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        if self.n == 0 {
            Ok(Value::F64(0.0))
        } else {
            let n = self.n as f64;
            Ok(Value::F64((self.ex2 - (self.ex * self.ex) / n) / (n - 1.0)))
        }
    }
    fn init(&mut self) {
        self.n = 0;
        self.k = 0.0;
        self.ex = 0.0;
        self.ex2 = 0.0;
    }
    fn merge(&mut self, src: &dyn std::any::Any) -> FResult<()> {
        if let Some(other) = src.downcast_ref::<Self>() {
            self.n += other.n;
            self.k += other.k;
            self.ex += other.ex;
            self.ex2 += other.ex2;
        }
        Ok(())
    }
    fn snot_clone(&self) -> Box<dyn TremorAggrFn> {
        Box::new(self.clone())
    }
    fn arity(&self) -> RangeInclusive<usize> {
        1..=1
    }
}

#[derive(Clone, Debug, Default)]
struct Stdev(Var);

impl TremorAggrFn for Stdev {
    fn accumulate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
        self.0.accumulate(args)
    }
    fn compensate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
        self.0.compensate(args)
    }
    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        self.0.emit().map(|v| match v {
            Value::F64(v) => Value::F64(v.sqrt()),
            v => v,
        })
    }
    fn init(&mut self) {
        self.0.init()
    }
    fn merge(&mut self, src: &dyn std::any::Any) -> FResult<()> {
        if let Some(other) = src.downcast_ref::<Self>() {
            // On self is earlier then other, so as long
            // as other has a value we take it
            self.0.merge(&other.0)?;
        }
        Ok(())
    }
    fn snot_clone(&self) -> Box<dyn TremorAggrFn> {
        Box::new(self.clone())
    }
    fn arity(&self) -> RangeInclusive<usize> {
        1..=1
    }
}

#[derive(Clone)]
struct Hdr {
    histo: Histogram<u64>,
    percentiles: Vec<(String, f64)>,
    percentiles_set: bool,
}

impl std::default::Default for Hdr {
    fn default() -> Self {
        Hdr {
            //ALLOW: this values have been tested so an error can never be returned
            histo: Histogram::new_with_bounds(1, u64::MAX >> 1, 2).unwrap(),
            percentiles: vec![
                ("0.5".to_string(), 0.5),
                ("0.9".to_string(), 0.9),
                ("0.95".to_string(), 0.95),
                ("0.99".to_string(), 0.99),
                ("0.999".to_string(), 0.999),
                ("0.9999".to_string(), 0.9999),
                ("0.99999".to_string(), 0.99999),
            ],
            percentiles_set: false,
        }
    }
}

unsafe impl Send for Hdr {}
unsafe impl Sync for Hdr {}

impl TremorAggrFn for Hdr {
    fn accumulate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
        if let Some(vals) = args.get(1).and_then(|v| v.as_array()) {
            if !self.percentiles_set {
                let percentiles: FResult<Vec<(String, f64)>> = vals
                    .iter()
                    .flat_map(|v| v.as_str().map(String::from))
                    .map(|s| {
                        let p = s.parse().map_err(|_| FunctionError::RuntimeError {
                            mfa: mfa("stats", "hdr", 2),
                            error: format!("Provided percentile '{}' isn't a float", s),
                        })?;
                        Ok((s, p))
                    })
                    .collect();
                self.percentiles = percentiles?;
                self.percentiles_set = true
            }
        }
        if let Some(v) = args[0].as_u64() {
            // if self.n == 0 {
            // FIXME error
            // }
            self.histo
                .record(v)
                .map_err(|e| FunctionError::RuntimeError {
                    mfa: mfa("stats", "hdr", 2),
                    error: format!("failed to record value: {:?}", e),
                })?;
        }
        Ok(())
    }

    fn merge(&mut self, src: &dyn std::any::Any) -> FResult<()> {
        if let Some(other) = src.downcast_ref::<Self>() {
            // On self is earlier then other, so as long
            // as other has a value we take it
            self.histo
                .add(&other.histo)
                .map_err(|e| FunctionError::RuntimeError {
                    mfa: mfa("stats", "hdr", 2),
                    error: format!("failed to merge historgrams: {:?}", e),
                })?;
        }
        Ok(())
    }

    fn compensate<'event>(&mut self, _args: &[&Value<'event>]) -> FResult<()> {
        // FIXME there's no facility for this with hdr histogram, punt for now
        Ok(())
    }
    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        let mut p = hashmap! {};
        for (pcn, percentile) in self.percentiles.iter() {
            p.insert(
                pcn.clone().into(),
                Value::I64(self.histo.value_at_percentile(percentile * 100.0) as i64),
            );
        }

        Ok(Value::Object(hashmap! {
            "count".into() => Value::I64(self.histo.len() as i64),
            "min".into() => Value::I64(self.histo.min() as i64),
            "max".into() => Value::I64(self.histo.max() as i64),
            "mean".into() => Value::F64(self.histo.mean()),
            "stdev".into() => Value::F64(self.histo.stdev()),
            "var".into() => Value::F64(self.histo.stdev().powf(2.0)),
            "percentiles".into() => Value::Object(p),
        }))
    }
    fn init(&mut self) {
        self.histo.reset();
    }
    fn snot_clone(&self) -> Box<dyn TremorAggrFn> {
        Box::new(self.clone())
    }
    fn arity(&self) -> RangeInclusive<usize> {
        1..=2
    }
}

pub fn load_aggr(registry: &mut AggrRegistry) {
    registry
        .insert(TremorAggrFnWrapper {
            module: "stats".to_string(),
            name: "count".to_string(),
            fun: Box::new(Count::default()),
        })
        .insert(TremorAggrFnWrapper {
            module: "stats".to_string(),
            name: "min".to_string(),
            fun: Box::new(Min::default()),
        })
        .insert(TremorAggrFnWrapper {
            module: "stats".to_string(),
            name: "max".to_string(),
            fun: Box::new(Max::default()),
        })
        .insert(TremorAggrFnWrapper {
            module: "stats".to_string(),
            name: "sum".to_string(),
            fun: Box::new(Sum::default()),
        })
        .insert(TremorAggrFnWrapper {
            module: "stats".to_string(),
            name: "var".to_string(),
            fun: Box::new(Var::default()),
        })
        .insert(TremorAggrFnWrapper {
            module: "stats".to_string(),
            name: "stdev".to_string(),
            fun: Box::new(Stdev::default()),
        })
        .insert(TremorAggrFnWrapper {
            module: "stats".to_string(),
            name: "mean".to_string(),
            fun: Box::new(Mean::default()),
        })
        .insert(TremorAggrFnWrapper {
            module: "stats".to_string(),
            name: "hdr".to_string(),
            fun: Box::new(Hdr::default()),
        });
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::registry::FResult as Result;
    use float_cmp::approx_eq;
    use simd_json::json;
    #[test]
    fn count() -> Result<()> {
        let mut a = Count::default();
        a.init();
        a.accumulate(&[])?;
        a.accumulate(&[])?;
        a.accumulate(&[])?;
        assert_eq!(a.emit()?, 3);

        let mut b = Count::default();
        b.init();
        b.accumulate(&[])?;
        b.merge(&a)?;
        assert_eq!(b.emit()?, 4);
        Ok(())
    }

    #[test]
    fn min() -> Result<()> {
        let one = Value::from(1);
        let two = Value::from(2);
        let three = Value::from(3);
        let mut a = Min::default();
        a.init();
        a.accumulate(&[&one])?;
        a.accumulate(&[&two])?;
        a.accumulate(&[&three])?;
        assert_eq!(a.emit()?, 1.0);
        let mut b = Min::default();
        b.init();
        b.merge(&a)?;
        assert_eq!(a.emit()?, 1.0);
        Ok(())
    }
    #[test]
    fn max() -> Result<()> {
        let mut a = Max::default();
        a.init();
        let one = Value::from(1);
        let two = Value::from(2);
        let three = Value::from(3);
        a.accumulate(&[&one])?;
        a.accumulate(&[&two])?;
        a.accumulate(&[&three])?;
        assert_eq!(a.emit()?, 3.0);
        Ok(())
    }

    #[test]
    fn sum() -> Result<()> {
        let mut a = Sum::default();
        a.init();
        let one = Value::from(1);
        let two = Value::from(2);
        let three = Value::from(3);
        a.accumulate(&[&one])?;
        a.accumulate(&[&two])?;
        a.accumulate(&[&three])?;
        assert_eq!(a.emit()?, 6.0);
        Ok(())
    }

    #[test]
    fn mean() -> Result<()> {
        let mut a = Mean::default();
        a.init();
        let one = Value::from(1);
        let two = Value::from(2);
        let three = Value::from(3);
        a.accumulate(&[&one])?;
        a.accumulate(&[&two])?;
        a.accumulate(&[&three])?;
        assert_eq!(a.emit()?, 2.0);
        Ok(())
    }

    #[test]
    fn variance() -> Result<()> {
        let mut a = Var::default();
        a.init();
        let two = Value::from(2);
        let four = Value::from(4);
        let nineteen = Value::from(19);
        let nine = Value::from(9);
        a.accumulate(&[&two])?;
        a.accumulate(&[&four])?;
        a.accumulate(&[&nineteen])?;
        a.accumulate(&[&nine])?;
        let r = a.emit()?.cast_f64().expect("screw it");
        assert!(approx_eq!(f64, dbg!(r), 173.0 / 3.0));

        let mut b = Var::default();
        b.init();
        b.accumulate(&[&two])?;
        b.accumulate(&[&four])?;
        b.merge(&a)?;
        let r = b.emit()?.cast_f64().expect("screw it");
        assert!(approx_eq!(f64, dbg!(r), 43.066_666_666_666_67));

        let mut c = Var::default();
        c.init();
        c.accumulate(&[&two])?;
        c.accumulate(&[&four])?;
        let r = c.emit()?.cast_f64().expect("screw it");
        assert!(approx_eq!(f64, dbg!(r), 2.0));

        b.merge(&c)?;
        let r = b.emit()?.cast_f64().expect("screw it");
        assert!(approx_eq!(f64, dbg!(r), 33.928_571_428_571_43));

        Ok(())
    }

    #[test]
    fn stdev() -> Result<()> {
        let mut a = Stdev::default();
        a.init();
        let two = Value::from(2);
        let four = Value::from(4);
        let nineteen = Value::from(19);
        a.accumulate(&[&two])?;
        a.accumulate(&[&four])?;
        a.accumulate(&[&nineteen])?;
        assert!((a.emit()?.cast_f64().expect("screw it") - (259.0 as f64 / 3.0).sqrt()) < 0.001);
        Ok(())
    }

    #[test]
    fn hdr() -> Result<()> {
        use simd_json::BorrowedValue;

        let mut a = Hdr::default();
        a.init();
        let mut i = 1;

        loop {
            if i > 100 {
                break;
            }
            a.accumulate(&[
                &Value::from(i),
                &Value::Array(vec![
                    Value::String(std::borrow::Cow::Borrowed("0.5")),
                    Value::String(std::borrow::Cow::Borrowed("0.9")),
                    Value::String(std::borrow::Cow::Borrowed("0.95")),
                    Value::String(std::borrow::Cow::Borrowed("0.99")),
                    Value::String(std::borrow::Cow::Borrowed("0.999")),
                    Value::String(std::borrow::Cow::Borrowed("0.9999")),
                ]),
            ])?;
            i += 1;
        }
        let v = a.emit()?;
        let e: BorrowedValue = json!({
            "min": 1,
            "max": 100,
            "count": 100,
            "mean": 50.5,
            "stdev": 28.866_070_047_722_12,
            "var": 833.25,
            "percentiles": {
                "0.5": 50,
                "0.9": 90,
                "0.95": 95,
                "0.99": 99,
                "0.999": 100,
                "0.9999": 100
            }
        })
        .into();
        assert_eq!(v, e);
        Ok(())
    }
}
