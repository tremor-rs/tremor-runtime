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
#![allow(clippy::cast_precision_loss)]

use crate::prelude::*;
use crate::registry::{
    mfa, Aggr as AggrRegistry, FResult, FunctionError, TremorAggrFn, TremorAggrFnWrapper,
};
use hdrhistogram::Histogram;
use sketches_ddsketch::{Config as DDSketchConfig, DDSketch};
use std::cmp::max;
use std::f64;
use std::ops::RangeInclusive;
use std::u64;

/// Round up.
///
/// Round `value` up to accuracy defined by `scale`.
/// Positive `scale` defines the number of decimal digits in the result
/// while negative `scale` rounds to a whole number and defines the number
/// of trailing zeroes in the result.
///
/// # Arguments
///
/// * `value` - value to round
/// * `scale` - result accuracy
///
/// taken from <https://github.com/0x022b/libmath-rs/blob/af3aff7e1500e5f801e73f3c464ea7bf81ec83c7/src/round.rs>
pub fn ceil(value: f64, scale: i8) -> f64 {
    let multiplier = 10_f64.powi(i32::from(scale));
    (value * multiplier).ceil() / multiplier
}

#[derive(Clone, Debug, Default)]
struct Count(i64);
impl TremorAggrFn for Count {
    fn accumulate(&mut self, _args: &[&Value]) -> FResult<()> {
        self.0 += 1;
        Ok(())
    }
    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        Ok(Value::from(self.0))
    }
    fn init(&mut self) {
        self.0 = 0;
    }
    fn merge(&mut self, src: &dyn TremorAggrFn) -> FResult<()> {
        if let Some(other) = src.downcast_ref::<Self>() {
            // On self is earlier then other, so as long
            // as other has a value we take it
            self.0 += other.0;
        }
        Ok(())
    }

    fn boxed_clone(&self) -> Box<dyn TremorAggrFn> {
        Box::new(self.clone())
    }
    fn arity(&self) -> RangeInclusive<usize> {
        0..=0
    }
}

#[derive(Clone, Debug, Default)]
struct Sum(f64);
impl TremorAggrFn for Sum {
    fn accumulate(&mut self, args: &[&Value]) -> FResult<()> {
        args.first().cast_f64().map_or_else(
            || {
                Err(FunctionError::BadType {
                    mfa: mfa("stats", "sum", 1),
                })
            },
            |v| {
                self.0 += v;
                Ok(())
            },
        )
    }

    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        Ok(Value::from(self.0))
    }
    fn init(&mut self) {
        self.0 = 0.0;
    }
    fn merge(&mut self, src: &dyn TremorAggrFn) -> FResult<()> {
        if let Some(other) = src.downcast_ref::<Self>() {
            // On self is earlier then other, so as long
            // as other has a value we take it
            self.0 += other.0;
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
struct Mean(i64, f64);
impl TremorAggrFn for Mean {
    fn accumulate(&mut self, args: &[&Value]) -> FResult<()> {
        args.first().cast_f64().map_or_else(
            || {
                Err(FunctionError::BadType {
                    mfa: mfa("stats", "mean", 1),
                })
            },
            |v| {
                self.1 += v;
                self.0 += 1;
                Ok(())
            },
        )
    }
    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        if self.0 == 0 {
            Ok(Value::null())
        } else {
            Ok(Value::from(self.1 / (self.0 as f64)))
        }
    }
    fn init(&mut self) {
        self.0 = 0;
        self.1 = 0.0;
    }
    fn merge(&mut self, src: &dyn TremorAggrFn) -> FResult<()> {
        if let Some(other) = src.downcast_ref::<Self>() {
            // On self is earlier then other, so as long
            // as other has a value we take it
            self.0 += other.0;
            self.1 += other.1;
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
struct Min(Option<f64>);
impl TremorAggrFn for Min {
    fn accumulate(&mut self, args: &[&Value]) -> FResult<()> {
        args.first().cast_f64().map_or_else(
            || {
                Err(FunctionError::BadType {
                    mfa: mfa("stats", "min", 1),
                })
            },
            |v| {
                if self.0.is_none() || Some(v) < self.0 {
                    self.0 = Some(v);
                };
                Ok(())
            },
        )
    }

    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        Ok(Value::from(self.0.unwrap_or_default()))
    }
    fn init(&mut self) {
        self.0 = None;
    }
    fn merge(&mut self, src: &dyn TremorAggrFn) -> FResult<()> {
        if let Some(other) = src.downcast_ref::<Self>() {
            // On self is earlier then other, so as long
            // as other has a value we take it
            if other.0 < self.0 {
                self.0 = other.0;
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
struct Max(Option<f64>);
impl TremorAggrFn for Max {
    fn accumulate(&mut self, args: &[&Value]) -> FResult<()> {
        args.first().cast_f64().map_or_else(
            || {
                Err(FunctionError::BadType {
                    mfa: mfa("stats", "max", 1),
                })
            },
            |v| {
                if self.0.is_none() || Some(v) > self.0 {
                    self.0 = Some(v);
                };
                Ok(())
            },
        )
    }
    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        Ok(Value::from(self.0.unwrap_or_default()))
    }
    fn init(&mut self) {
        self.0 = None;
    }
    fn merge(&mut self, src: &dyn TremorAggrFn) -> FResult<()> {
        if let Some(other) = src.downcast_ref::<Self>() {
            // On self is earlier then other, so as long
            // as other has a value we take it
            if other.0 > self.0 {
                self.0 = other.0;
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
struct Var {
    n: u64,
    k: f64,
    ex: f64,
    ex2: f64,
}

impl TremorAggrFn for Var {
    fn accumulate(&mut self, args: &[&Value]) -> FResult<()> {
        args.first().cast_f64().map_or_else(
            || {
                Err(FunctionError::BadType {
                    mfa: mfa("stats", "var", 1),
                })
            },
            |v| {
                if self.n == 0 {
                    self.k = v;
                }
                self.n += 1;
                self.ex += v - self.k;
                self.ex2 += (v - self.k) * (v - self.k);
                Ok(())
            },
        )
    }
    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        if self.n == 0 {
            Ok(Value::from(0.0))
        } else {
            let n = self.n as f64;
            Ok(Value::from(
                (self.ex2 - (self.ex * self.ex) / n) / (n - 1.0),
            ))
        }
    }
    fn init(&mut self) {
        self.n = 0;
        self.k = 0.0;
        self.ex = 0.0;
        self.ex2 = 0.0;
    }
    fn merge(&mut self, src: &dyn TremorAggrFn) -> FResult<()> {
        if let Some(other) = src.downcast_ref::<Self>() {
            self.n += other.n;
            self.k += other.k;
            self.ex += other.ex;
            self.ex2 += other.ex2;
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
struct Stdev(Var);

impl TremorAggrFn for Stdev {
    fn accumulate(&mut self, args: &[&Value]) -> FResult<()> {
        self.0.accumulate(args)
    }

    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        self.0
            .emit()
            .map(|v| v.as_f64().map_or(v, |v| Value::from(v.sqrt())))
    }
    fn init(&mut self) {
        self.0.init();
    }
    fn merge(&mut self, src: &dyn TremorAggrFn) -> FResult<()> {
        if let Some(other) = src.downcast_ref::<Self>() {
            // On self is earlier then other, so as long
            // as other has a value we take it
            self.0.merge(&other.0)?;
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

#[derive(Clone)]
struct Dds {
    sketch: Option<DDSketch>,
    cache: Vec<f64>,
    percentiles: Vec<(String, f64)>,
    percentiles_set: bool,
}

impl Dds {
    fn switch_to_sketch(&mut self, mut sketch: DDSketch) {
        for v in self.cache.drain(..) {
            sketch.add(v);
        }
        self.sketch = Some(sketch);
    }

    fn new_sketch() -> DDSketch {
        DDSketch::new(DDSketchConfig::defaults())
    }
}

impl std::default::Default for Dds {
    fn default() -> Self {
        Self {
            sketch: None,
            cache: Vec::with_capacity(HIST_INITIAL_CACHE_SIZE),
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

impl TremorAggrFn for Dds {
    fn accumulate(&mut self, args: &[&Value]) -> FResult<()> {
        if !self.percentiles_set {
            // either we have a second argument (which overwrites the defaults)
            // or we use the defaults
            if let Some(vals) = args.get(1).as_array() {
                let percentiles: FResult<Vec<(String, f64)>> = vals
                    .iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .map(|s| {
                        let p = s.parse().map_err(|e| FunctionError::RuntimeError {
                            mfa: mfa("stats", "dds", 2),
                            error: format!("Provided percentile '{s}' isn't a float: {e}"),
                        })?;
                        Ok((s, p))
                    })
                    .collect();
                self.percentiles = percentiles?;
            }
            self.percentiles_set = true;
        }
        if let Some(v) = args.first().cast_f64() {
            if v < 0.0 {
                return Ok(());
            } else if let Some(ref mut sketch) = self.sketch {
                sketch.add(v);
            } else {
                self.cache.push(v);
                if self.cache.len() == HIST_MAX_CACHE_SIZE {
                    self.switch_to_sketch(Self::new_sketch());
                }
            }
        }
        Ok(())
    }

    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        fn err<T>(e: &T) -> FunctionError
        where
            T: ToString,
        {
            FunctionError::RuntimeError {
                mfa: mfa("stats", "dds", 2),
                error: e.to_string(),
            }
        }
        let mut p = Value::object_with_capacity(self.percentiles.len());

        // ensure sketch
        if self.sketch.is_none() {
            self.switch_to_sketch(Self::new_sketch());
        }
        let sketch = self
            .sketch
            .as_ref()
            .ok_or_else(|| err(&"Internal DDSketch not available"))?;
        let count = sketch.count();
        let (min, max, sum) = if count == 0 {
            for (pcn, _percentile) in &self.percentiles {
                p.try_insert(pcn.clone(), 0.0);
            }
            (0_f64, 0_f64, 0_f64)
        } else {
            for (pcn, percentile) in &self.percentiles {
                let quantile = sketch.quantile(*percentile).ok().flatten().ok_or_else(|| {
                    err(&format!("Unable to calculate percentile '{}'", *percentile))
                })?;
                let quantile_dsp = ceil(quantile, 1); // Round for equiv with HDR ( 2 digits )
                p.try_insert(pcn.clone(), quantile_dsp);
            }
            (
                sketch
                    .min()
                    .ok_or_else(|| err(&"Unable to calculate min"))?,
                sketch
                    .max()
                    .ok_or_else(|| err(&"Unable to calculate max"))?,
                sketch
                    .sum()
                    .ok_or_else(|| err(&"Unable to calculate sum"))?,
            )
        };
        let mean = if count == 0 { 0.0 } else { sum / count as f64 };
        let res = literal!({
            "count": count,
            "sum": sum,
            "min": min,
            "max": max,
            "mean": mean,
            "percentiles": p
        });
        Ok(res)
    }

    fn merge(&mut self, src: &dyn TremorAggrFn) -> FResult<()> {
        let other: Option<&Self> = src.downcast_ref::<Self>();
        if let Some(other) = other {
            if !self.percentiles_set {
                self.percentiles.clone_from(&other.percentiles);
                self.percentiles_set = true;
            };

            match (&mut self.sketch, &other.sketch) {
                (Some(sketch), Some(other)) => {
                    sketch.merge(other).ok();
                }
                (Some(sketch), None) => {
                    for v in &other.cache {
                        sketch.add(*v);
                    }
                }
                (None, Some(other)) => {
                    // only other is actually a sketch
                    self.switch_to_sketch(other.clone());
                }
                (None, None) if self.cache.len() + other.cache.len() > HIST_MAX_CACHE_SIZE => {
                    // If the cache size exceeds our maximal cache size drain them into a sketch
                    let mut sketch = Self::new_sketch();
                    for v in self.cache.drain(..) {
                        sketch.add(v);
                    }
                    for v in &other.cache {
                        sketch.add(*v);
                    }
                    self.sketch = Some(sketch);
                }
                (None, None) => {
                    // If not append it's cache
                    self.cache.extend(&other.cache);
                }
            }
        }
        Ok(())
    }

    fn init(&mut self) {
        self.sketch = None;
        self.cache.clear();
    }

    fn boxed_clone(&self) -> Box<dyn TremorAggrFn> {
        Box::new(self.clone())
    }
    fn arity(&self) -> RangeInclusive<usize> {
        1..=2
    }
}

#[derive(Clone)]
struct Hdr {
    histo: Option<Histogram<u64>>,
    cache: Vec<u64>,
    percentiles: Vec<(String, f64)>,
    percentiles_set: bool,
    high_bound: u64,
}

const HIST_MAX_CACHE_SIZE: usize = 8192;
const HIST_INITIAL_CACHE_SIZE: usize = 128;
impl std::default::Default for Hdr {
    fn default() -> Self {
        Self {
            histo: None,
            cache: Vec::with_capacity(HIST_INITIAL_CACHE_SIZE),
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
            high_bound: 0,
        }
    }
}

impl Hdr {
    fn high_bound(&self) -> u64 {
        max(self.high_bound, 4)
    }
    fn switch_to_histo(&mut self, mut histo: Histogram<u64>) -> FResult<()> {
        for v in self.cache.drain(..) {
            Self::add(v, &mut histo)?;
        }
        self.histo = Some(histo);
        Ok(())
    }

    fn add(v: u64, histo: &mut Histogram<u64>) -> FResult<()> {
        histo.record(v).map_err(Self::err("failed to record value"))
    }

    fn new_histo(high_bound: u64) -> FResult<Histogram<u64>> {
        let mut histo: Histogram<u64> = Histogram::new_with_bounds(1, high_bound, 2)
            .map_err(Self::err("failed to allocate hdr storage"))?;
        histo.auto(true);
        Ok(histo)
    }

    fn err<E: std::error::Error, S: ToString + ?Sized>(msg: &S) -> impl FnOnce(E) -> FunctionError {
        let msg = msg.to_string();
        move |e: E| FunctionError::RuntimeError {
            mfa: mfa("stats", "hdr", 2),
            error: format!("{msg}: {e:?}"),
        }
    }
}
impl TremorAggrFn for Hdr {
    fn accumulate(&mut self, args: &[&Value]) -> FResult<()> {
        if !self.percentiles_set {
            // either we get the percentiles set via the second argument
            // or we use the defaults - either way after the first `accumulate` call we have the percentiles set in stone
            if let Some(vals) = args.get(1).as_array() {
                let percentiles: FResult<Vec<(String, f64)>> = vals
                    .iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .map(|s| {
                        let p = s.parse().map_err(Self::err(&format!(
                            "Provided percentile '{s}' isn't a float",
                        )))?;
                        Ok((s, p))
                    })
                    .collect();
                self.percentiles = percentiles?;
            }
            self.percentiles_set = true;
        }
        if let Some(v) = args.first().cast_f64() {
            if v < 0.0 {
                return Ok(());
            }

            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let v = v as u64; // TODO add f64 support to HDR Histogram create -  oss
            if let Some(ref mut histo) = self.histo {
                Self::add(v, histo)?;
            } else {
                if v > self.high_bound {
                    self.high_bound = v;
                }
                self.cache.push(v);
                if self.cache.len() == HIST_MAX_CACHE_SIZE {
                    self.switch_to_histo(Self::new_histo(self.high_bound())?)?;
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, src: &dyn TremorAggrFn) -> FResult<()> {
        if let Some(other) = src.downcast_ref::<Self>() {
            // On self is earlier then other, so as long
            // as other has a value we take it
            if !self.percentiles_set {
                self.percentiles.clone_from(&other.percentiles);
                self.percentiles_set = true;
            };
            self.high_bound = max(self.high_bound, other.high_bound);
            match (&mut self.histo, &other.histo) {
                (Some(mine), Some(other)) => {
                    mine.add(other)
                        .map_err(Self::err("failed to merge histograms"))?;
                }
                (Some(mine), None) => {
                    for v in &other.cache {
                        Self::add(*v, mine)?;
                    }
                }
                (None, Some(other)) => {
                    self.switch_to_histo(other.clone())?;
                }
                (None, None) if self.cache.len() + other.cache.len() > HIST_MAX_CACHE_SIZE => {
                    // If the cache size exceeds our maximal cache size drain them into a histogram
                    let mut histo: Histogram<u64> = Self::new_histo(self.high_bound())?;
                    for v in self.cache.drain(..) {
                        Self::add(v, &mut histo)?;
                    }
                    for v in &other.cache {
                        Self::add(*v, &mut histo)?;
                    }
                    self.histo = Some(histo);
                }
                (None, None) => {
                    self.cache.extend(&other.cache);
                }
            }
        }
        Ok(())
    }

    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        fn err<T>(e: &T) -> FunctionError
        where
            T: ToString,
        {
            FunctionError::RuntimeError {
                mfa: mfa("stats", "hdr", 2),
                error: e.to_string(),
            }
        }
        let mut p = Value::object_with_capacity(self.percentiles.len());
        if self.histo.is_none() {
            self.switch_to_histo(Self::new_histo(self.high_bound())?)?;
        }
        let histo = self
            .histo
            .as_ref()
            .ok_or_else(|| err(&"HDR histogram not available"))?;
        for (pcn, percentile) in &self.percentiles {
            p.try_insert(pcn.clone(), histo.value_at_percentile(percentile * 100.0));
        }
        Ok(literal!({
            "count": histo.len(),
            "min": histo.min(),
            "max": histo.max(),
            "mean": histo.mean(),
            "stdev": histo.stdev(),
            "var": histo.stdev().powf(2.0),
            "percentiles": p,
        }))
    }
    fn init(&mut self) {
        self.histo = None;
        self.high_bound = 0;
        self.cache.clear();
    }

    fn boxed_clone(&self) -> Box<dyn TremorAggrFn> {
        Box::new(self.clone())
    }
    fn arity(&self) -> RangeInclusive<usize> {
        1..=2
    }
}

pub fn load_aggr(registry: &mut AggrRegistry) {
    // Allow: this is ok because we must use the result of insert
    registry
        .insert(TremorAggrFnWrapper::new(
            "stats".to_string(),
            "count".to_string(),
            Box::<Count>::default(),
        ))
        .insert(TremorAggrFnWrapper::new(
            "stats".to_string(),
            "min".to_string(),
            Box::<Min>::default(),
        ))
        .insert(TremorAggrFnWrapper::new(
            "stats".to_string(),
            "max".to_string(),
            Box::<Max>::default(),
        ))
        .insert(TremorAggrFnWrapper::new(
            "stats".to_string(),
            "sum".to_string(),
            Box::<Sum>::default(),
        ))
        .insert(TremorAggrFnWrapper::new(
            "stats".to_string(),
            "var".to_string(),
            Box::<Var>::default(),
        ))
        .insert(TremorAggrFnWrapper::new(
            "stats".to_string(),
            "stdev".to_string(),
            Box::<Stdev>::default(),
        ))
        .insert(TremorAggrFnWrapper::new(
            "stats".to_string(),
            "mean".to_string(),
            Box::<Mean>::default(),
        ))
        .insert(TremorAggrFnWrapper::new(
            "stats".to_string(),
            "hdr".to_string(),
            Box::<Hdr>::default(),
        ))
        .insert(TremorAggrFnWrapper::new(
            "stats".to_string(),
            "dds".to_string(),
            Box::<Dds>::default(),
        ));
}

#[cfg(test)]
mod test {
    #![allow(clippy::float_cmp, clippy::ignored_unit_patterns)]

    use super::*;
    use crate::registry::FResult as Result;
    use float_cmp::approx_eq;
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
        let mut a = Min::default();
        a.init();

        let one = Value::from(1);
        let two = Value::from(2);
        let three = Value::from(3);

        let err = Value::null();
        assert!(a.accumulate(&[&err]).is_err());

        a.accumulate(&[&one])?;
        a.accumulate(&[&two])?;
        a.accumulate(&[&three])?;

        assert_eq!(a.emit()?, 1.0);
        let mut b = Min::default();
        b.init();
        b.merge(&a)?;
        assert_eq!(a.emit()?, 1.0);

        assert_eq!(a.arity(), 1..=1);

        Ok(())
    }
    #[test]
    fn max() -> Result<()> {
        let mut a = Max::default();
        a.init();

        let one = Value::from(1);
        let two = Value::from(2);
        let three = Value::from(3);

        let err = Value::null();
        assert!(a.accumulate(&[&err]).is_err());

        a.accumulate(&[&one])?;
        a.accumulate(&[&two])?;
        a.accumulate(&[&three])?;

        assert_eq!(a.emit()?, 3.0);
        let mut b = Max::default();
        b.init();
        b.merge(&a)?;
        assert_eq!(a.emit()?, 3.0);

        assert_eq!(a.arity(), 1..=1);

        Ok(())
    }

    #[test]
    fn sum() -> Result<()> {
        let mut a = Sum::default();
        a.init();
        let one = Value::from(1);
        let two = Value::from(2);
        let three = Value::from(3);
        let four = Value::from(4);
        let err = Value::null();
        assert!(a.accumulate(&[&err]).is_err());

        a.accumulate(&[&one])?;
        a.accumulate(&[&two])?;
        a.accumulate(&[&three])?;
        assert_eq!(a.emit()?, 6.0);

        let mut b = Mean::default();
        b.init();
        a.accumulate(&[&four])?;
        a.merge(&b)?;
        assert_eq!(a.emit()?, 10.0);

        assert_eq!(a.arity(), 1..=1);
        Ok(())
    }

    #[test]
    fn mean() -> Result<()> {
        let mut a = Mean::default();
        a.init();
        assert_eq!(a.emit()?, ());
        let one = Value::from(1);
        let two = Value::from(2);
        let three = Value::from(3);
        let four = Value::from(4);
        let err = Value::null();
        assert!(a.accumulate(&[&err]).is_err());
        a.accumulate(&[&one])?;
        a.accumulate(&[&two])?;
        a.accumulate(&[&three])?;
        assert_eq!(a.emit()?, 2.0);

        let mut b = Mean::default();
        b.init();
        a.accumulate(&[&four])?;
        a.merge(&b)?;
        assert_eq!(a.emit()?, 2.5);

        assert_eq!(a.arity(), 1..=1);
        Ok(())
    }

    #[test]
    fn variance() -> Result<()> {
        let mut a = Var::default();
        a.init();

        assert_eq!(a.emit()?, 0.0);

        let two = Value::from(2);
        let four = Value::from(4);
        let nineteen = Value::from(19);
        let nine = Value::from(9);
        let err = Value::null();
        assert!(a.accumulate(&[&err]).is_err());
        a.accumulate(&[&two])?;
        a.accumulate(&[&four])?;
        a.accumulate(&[&nineteen])?;
        a.accumulate(&[&nine])?;
        let r = a.emit()?.cast_f64().expect("screw it");
        assert!(approx_eq!(f64, r, 173.0 / 3.0));

        let mut b = Var::default();
        b.init();
        b.accumulate(&[&two])?;
        b.accumulate(&[&four])?;
        b.merge(&a)?;
        let r = b.emit()?.cast_f64().expect("screw it");
        assert!(approx_eq!(f64, r, 43.066_666_666_666_67));

        let mut c = Var::default();
        c.init();
        c.accumulate(&[&two])?;
        c.accumulate(&[&four])?;
        let r = c.emit()?.cast_f64().expect("screw it");
        assert!(approx_eq!(f64, r, 2.0));

        b.merge(&c)?;
        let r = b.emit()?.cast_f64().expect("screw it");
        assert!(approx_eq!(f64, r, 33.928_571_428_571_43));

        assert_eq!(a.arity(), 1..=1);

        Ok(())
    }

    #[test]
    fn stdev() -> Result<()> {
        let mut a = Stdev::default();
        a.init();
        let two = Value::from(2);
        let four = Value::from(4);
        let nineteen = Value::from(19);

        let err = Value::null();
        assert!(a.accumulate(&[&err]).is_err());

        a.accumulate(&[&two])?;
        a.accumulate(&[&four])?;
        a.accumulate(&[&nineteen])?;
        assert!((a.emit()?.cast_f64().expect("screw it") - (259.0_f64 / 3.0).sqrt()) < 0.001);

        let mut b = Stdev::default();
        b.merge(&a)?;
        assert!((b.emit()?.cast_f64().expect("screw it") - (259.0_f64 / 3.0).sqrt()) < 0.001);

        assert_eq!(a.arity(), 1..=1);

        Ok(())
    }

    #[test]
    fn hdr() -> Result<()> {
        let mut a = Hdr::default();
        a.init();

        assert!(a
            .accumulate(&[
                &Value::from(0),
                &literal!(["snot", "0.9", "0.95", "0.99", "0.999", "0.9999"]),
            ])
            .is_err());

        for i in 1..=100 {
            a.accumulate(&[
                &Value::from(i),
                &literal!(["0.5", "0.9", "0.95", "0.99", "0.999", "0.9999"]),
            ])?;
        }

        let e = literal!({
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
        });
        assert_eq!(a.emit()?, e);

        let mut b = Hdr::default();
        b.init();
        b.merge(&a)?;
        assert_eq!(b.emit()?, e);

        assert_eq!(a.arity(), 1..=2);

        Ok(())
    }

    #[test]
    fn hdr_empty() -> Result<()> {
        let mut histo = Hdr::default();
        histo.init();
        assert_eq!(
            literal!({
                "count": 0_u64,
                "min": 0_u64,
                "max": 0_u64,
                "mean": 0.0,
                "stdev": 0.0,
                "var": 0.0,
                "percentiles": {
                    "0.5": 0_u64,
                    "0.9": 0_u64,
                    "0.95": 0_u64,
                    "0.99": 0_u64,
                    "0.999": 0_u64,
                    "0.9999": 0_u64,
                    "0.99999": 0_u64,
                }
            }),
            histo.emit()?
        );
        Ok(())
    }

    #[test]
    fn dds() -> Result<()> {
        use crate::Value;

        let mut a = Dds::default();
        assert_eq!(a.arity(), 1..=2);
        a.init();

        assert!(a
            .accumulate(&[
                &Value::from(0),
                &literal!(["snot", "0.9", "0.95", "0.99", "0.999", "0.9999"]),
            ])
            .is_err());

        for i in 1..=100 {
            a.accumulate(&[
                &Value::from(i),
                &literal!(["0.5", "0.9", "0.95", "0.99", "0.999", "0.9999"]),
            ])?;
        }

        let e = literal!({
            "count": 100_u64,
            "sum": 5050.0,
            "min": 1.0,
            "max": 100.0,
            "mean": 50.5,
            "percentiles": {
                "0.5": 50.0,
                "0.9": 89.2,
                "0.95": 94.7,
                "0.99": 98.6,
                "0.999": 98.6,
                "0.9999": 98.6,
            }
        });
        assert_eq!(a.emit()?, e);

        let mut b = Dds::default();
        b.init();
        b.merge(&a)?;
        assert_eq!(b.emit()?, e);

        Ok(())
    }

    #[test]
    fn dds_empty() -> Result<()> {
        let mut dds = Dds::default();
        dds.init();
        let value = dds.emit()?;
        assert_eq!(
            literal!({
                "count": 0,
                "sum": 0.0,
                "min": 0.0,
                "max": 0.0,
                "mean": 0.0,
                "percentiles": {
                    "0.5": 0.0,
                    "0.9": 0.0,
                    "0.95": 0.0,
                    "0.99": 0.0,
                    "0.999": 0.0,
                    "0.9999": 0.0,
                    "0.99999": 0.0
                }
            }),
            value
        );
        Ok(())
    }

    use crate::errors::Error;
    use proptest::prelude::*;

    use proptest::collection::vec;
    use proptest::num;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn dds_prop(vec1 in vec(num::f64::POSITIVE, 0..(HIST_MAX_CACHE_SIZE * 2)),
                    vec2 in vec(num::f64::POSITIVE, 0..(HIST_MAX_CACHE_SIZE * 2))) {
            let mut a = Dds::default();
            a.init();
            for v in &vec1 {
                a.accumulate(&[
                    &Value::from(*v),
                    &literal!([0.5])
                ]).map_err(|fe| Error::from(format!("{fe:?}")))?;
            }

            let mut b = Dds::default();
            b.init();
            for v in &vec2 {
                b.accumulate(&[
                    &Value::from(*v),
                    &literal!([0.5])
                ]).map_err(|fe| Error::from(format!("{fe:?}")))?;
            }

            a.merge(&b).map_err(|fe| Error::from(format!("{fe:?}")))?;

            let value = a.emit().map_err(|fe| Error::from(format!("{fe:?}")))?;
            let count = value.get_u64("count").unwrap_or_default();
            let sum = value.get_f64("sum").unwrap_or_default();
            let min = value.get_f64("min").unwrap_or_default();
            let max = value.get_f64("max").unwrap_or_default();

            // validate min, max, sum, count against the input
            let input: Vec<f64> = vec1.iter().chain(vec2.iter()).copied().collect::<Vec<_>>();
            prop_assert_eq!(input.len() as u64, count);
            let iter_sum: f64 = input.iter().sum();
            prop_assert_eq!(iter_sum, sum);

            let iter_min = input.iter().copied().reduce(f64::min).unwrap_or(0.0_f64);
            prop_assert_eq!(iter_min, min);
            let iter_max = input.iter().copied().reduce(f64::max).unwrap_or(0.0_f64);
            prop_assert_eq!(iter_max, max);
        }
        #[test]
        fn hdr_prop(vec1 in vec(0_u64..100_u64, 0..(HIST_MAX_CACHE_SIZE * 2)),
                    vec2 in vec(0_u64..100_u64, 0..(HIST_MAX_CACHE_SIZE * 2))) {
            let mut a = Hdr::default();
            a.init();
            for v in &vec1 {
                a.accumulate(&[
                    &Value::from(*v),
                    &literal!([0.5, 0.99])
                ]).map_err(|fe| Error::from(format!("{fe:?}")))?;
            }
            let mut b = Hdr::default();
            b.init();
            for v in &vec2 {
                b.accumulate(&[
                    &Value::from(*v),
                    &literal!([0.5, 0.99])
                ]).map_err(|fe| Error::from(format!("{fe:?}")))?;
            }
            a.merge(&b).map_err(|fe| Error::from(format!("{fe:?}")))?;
            let value = a.emit().map_err(|fe| Error::from(format!("{fe:?}")))?;
            let count = value.get_u64("count").unwrap_or_default();
            let min = value.get_u64("min").unwrap_or_default();
            let max = value.get_u64("max").unwrap_or_default();

            let input: Vec<u64> = vec1.iter().chain(vec2.iter()).copied().collect();
            prop_assert_eq!(input.len() as u64, count);
            let iter_min = input.iter().copied().min().unwrap_or(0);
            prop_assert_eq!(iter_min, min);

            let iter_max = input.iter().copied().max().unwrap_or(0);
            prop_assert_eq!(iter_max, max);
        }
    }
}
