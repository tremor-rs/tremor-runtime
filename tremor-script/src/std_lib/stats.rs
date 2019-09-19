use crate::registry::{AggrRegistry, FResult, TremorAggrFn, TremorAggrFnWrapper};
use simd_json::value::ValueTrait;
use simd_json::BorrowedValue as Value;
use std::ops::RangeInclusive;

#[derive(Clone, Debug, Default)]
struct Count(i64);
impl TremorAggrFn for Count {
    fn accumulate<'event>(&mut self, _args: &[&Value<'event>]) -> FResult<()> {
        self.0 += 1;
        Ok(())
    }
    fn compensate<'event>(&mut self, _args: &[&Value<'event>]) -> FResult<()> {
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
    fn emit<'event>(&self) -> FResult<Value<'event>> {
        Ok(Value::F64(self.0))
    }
    fn init(&mut self) {
        self.0 = 0.0;
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
    fn emit<'event>(&self) -> FResult<Value<'event>> {
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
        Ok(())
    }
    fn emit<'event>(&self) -> FResult<Value<'event>> {
        Ok(Value::F64(self.0.unwrap_or_default()))
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
    fn emit<'event>(&self) -> FResult<Value<'event>> {
        Ok(Value::F64(self.0.unwrap_or_default()))
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
    fn emit<'event>(&self) -> FResult<Value<'event>> {
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
    fn emit<'event>(&self) -> FResult<Value<'event>> {
        self.0.emit().map(|v| match v {
            Value::F64(v) => Value::F64(v.sqrt()),
            v => v,
        })
    }
    fn init(&mut self) {
        self.0.init()
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
        });
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::registry::FunctionError;
    #[test]
    fn count() -> Result<(), FunctionError> {
        let mut a = Count::default();
        a.init();
        a.accumulate(&[])?;
        a.accumulate(&[])?;
        a.accumulate(&[])?;
        assert_eq!(a.emit()?, 3);
        Ok(())
    }

    #[test]
    fn min() -> Result<(), FunctionError> {
        let mut a = Min::default();
        a.init();
        let one = Value::from(1);
        let two = Value::from(2);
        let three = Value::from(3);
        a.accumulate(&[&one])?;
        a.accumulate(&[&two])?;
        a.accumulate(&[&three])?;
        assert_eq!(a.emit()?, 1.0);
        Ok(())
    }
    #[test]
    fn max() -> Result<(), FunctionError> {
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
    fn sum() -> Result<(), FunctionError> {
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
    fn mean() -> Result<(), FunctionError> {
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
    fn variance() -> Result<(), FunctionError> {
        let mut a = Var::default();
        a.init();
        let two = Value::from(2);
        let four = Value::from(4);
        let nineteen = Value::from(19);
        a.accumulate(&[&two])?;
        a.accumulate(&[&four])?;
        a.accumulate(&[&nineteen])?;
        assert!((a.emit()?.cast_f64().expect("screw it") - 259.0 / 3.0) < 0.001);
        Ok(())
    }

    #[test]
    fn stdev() -> Result<(), FunctionError> {
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
}
