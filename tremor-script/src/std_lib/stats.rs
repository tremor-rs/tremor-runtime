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
use crate::Value;
use halfbrown::hashmap;
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
    let multiplier = 10_f64.powi(i32::from(scale)) as f64;
    (value * multiplier).ceil() / multiplier
}

#[derive(Clone, Debug, Default)]
struct Count(i64);
impl TremorAggrFn for Count {
    fn accumulate<'event>(&mut self, _args: &[&Value<'event>]) -> FResult<()> {
        self.0 += 1;
        Ok(())
    }
    // fn compensate<'event>(&mut self, _args: &[&Value<'event>]) -> FResult<()> {
    //     self.0 -= 1;
    //     Ok(())
    // }
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
    #[cfg(not(tarpaulin_include))]
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
    fn accumulate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
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
    // fn compensate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
    //     args.first().cast_f64().map_or_else(
    //         || {
    //             Err(FunctionError::BadType {
    //                 mfa: mfa("stats", "sum", 1),
    //             })
    //         },
    //         |v| {
    //             self.0 -= v;
    //             Ok(())
    //         },
    //     )
    // }
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
    #[cfg(not(tarpaulin_include))]
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
    fn accumulate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
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
    // fn compensate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
    //     self.0 -= 1;
    //     args.first().cast_f64().map_or_else(
    //         || {
    //             Err(FunctionError::BadType {
    //                 mfa: mfa("stats", "mean", 1),
    //             })
    //         },
    //         |v| {
    //             self.1 -= v;
    //             Ok(())
    //         },
    //     )
    // }
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
    #[cfg(not(tarpaulin_include))]
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
    fn accumulate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
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
    // fn compensate<'event>(&mut self, _args: &[&Value<'event>]) -> FResult<()> {
    //     // TODO: how?
    //     // [a, b, c, d, e, f, g, h, i, j];
    //     // a -> [(0, a)]
    //     // b -> [(0, a), (1, b)]
    //     // c -> [(0, a), (1, b), (2, c)]  ...

    //     // [d, t, u, a, t, u, b, c, 6];
    //     // d -> [(0, d)]
    //     // t -> [(0, d), (1, t)]
    //     // u -> [(0, d), (1, t), (2, u)]  ...
    //     // a -> [(3, a)]  ...
    //     // t -> [(3, a), (4, t)]  ...
    //     // u -> [(3, a), (4, t), (5, u)]  ...
    //     // b -> [(3, a), (5, b)]  ...

    //     Ok(())
    // }
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
    #[cfg(not(tarpaulin_include))]
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
    fn accumulate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
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
    // fn compensate<'event>(&mut self, _args: &[&Value<'event>]) -> FResult<()> {
    //     // TODO: how?
    //     Ok(())
    // }
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
    #[cfg(not(tarpaulin_include))]
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
    fn accumulate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
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
    // fn compensate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
    //     args.first().cast_f64().map_or_else(
    //         || {
    //             Err(FunctionError::BadType {
    //                 mfa: mfa("stats", "var", 1),
    //             })
    //         },
    //         |v| {
    //             self.n -= 1;
    //             self.ex -= v - self.k;
    //             self.ex2 -= (v - self.k) * (v - self.k);
    //             Ok(())
    //         },
    //     )
    // }
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
    #[cfg(not(tarpaulin_include))]
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
    fn accumulate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
        self.0.accumulate(args)
    }
    // fn compensate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
    //     self.0.compensate(args)
    // }
    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        self.0
            .emit()
            .map(|v| v.as_f64().map_or(v, |v| Value::from(v.sqrt())))
    }
    fn init(&mut self) {
        self.0.init()
    }
    fn merge(&mut self, src: &dyn TremorAggrFn) -> FResult<()> {
        if let Some(other) = src.downcast_ref::<Self>() {
            // On self is earlier then other, so as long
            // as other has a value we take it
            self.0.merge(&other.0)?;
        }
        Ok(())
    }
    #[cfg(not(tarpaulin_include))]
    fn boxed_clone(&self) -> Box<dyn TremorAggrFn> {
        Box::new(self.clone())
    }
    fn arity(&self) -> RangeInclusive<usize> {
        1..=1
    }
}

struct Dds {
    histo: Option<DDSketch>,
    cache: Vec<f64>,
    percentiles: Vec<(String, f64)>,
    percentiles_set: bool,
    //    digits_significant_precision: usize,
}

impl std::clone::Clone for Dds {
    // DDSKetch does not implement clone
    fn clone(&self) -> Self {
        Self {
            histo: match &self.histo {
                Some(dds) => {
                    let config = DDSketchConfig::defaults();
                    let mut histo = DDSketch::new(config);
                    histo.merge(&dds).ok();
                    Some(histo)
                }
                None => None,
            },
            cache: self.cache.clone(),
            percentiles: self.percentiles.clone(),
            percentiles_set: self.percentiles_set,
            //            digits_significant_precision: 2,
        }
    }
}

impl std::default::Default for Dds {
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
            //            digits_significant_precision: 2,
        }
    }
}

impl TremorAggrFn for Dds {
    fn accumulate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
        if !self.percentiles_set {
            if let Some(vals) = args.get(1).as_array() {
                let percentiles: FResult<Vec<(String, f64)>> = vals
                    .iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .map(|s| {
                        let p = s.parse().map_err(|e| FunctionError::RuntimeError {
                            mfa: mfa("stats", "dds", 2),
                            error: format!("Provided percentile '{}' isn't a float: {}", s, e),
                        })?;
                        Ok((s, p))
                    })
                    .collect();
                self.percentiles = percentiles?;
                self.percentiles_set = true
            }
        }
        if let Some(v) = args.first().cast_f64() {
            if v < 0.0 {
                return Ok(());
            } else if let Some(ref mut histo) = self.histo {
                histo.add(v);
            } else {
                self.cache.push(v);
                if self.cache.len() == HIST_MAX_CACHE_SIZE {
                    let mut histo: DDSketch = DDSketch::new(DDSketchConfig::defaults());
                    for v in self.cache.drain(..) {
                        histo.add(v);
                    }
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
        let mut p = Value::object_with_capacity(self.percentiles.len() + 3);
        let histo = if let Some(histo) = self.histo.as_ref() {
            histo
        } else {
            let mut histo: DDSketch = DDSketch::new(DDSketchConfig::defaults());
            for v in self.cache.drain(..) {
                histo.add(v);
            }
            self.histo = Some(histo);
            if let Some(histo) = self.histo.as_ref() {
                histo
            } else {
                // ALLOW: we just set it, we know it exists
                unreachable!()
            }
        };

        let count = histo.count();
        let (min, max, sum) = if count == 0 {
            for (pcn, _percentile) in &self.percentiles {
                p.try_insert(pcn.clone(), 0.0);
            }
            (0_f64, 0_f64, 0_f64)
        } else {
            for (pcn, percentile) in &self.percentiles {
                let quantile = histo.quantile(*percentile).ok().flatten().ok_or_else(|| {
                    err(&format!("Unable to calculate percentile '{}'", *percentile))
                })?;
                let quantile_dsp = ceil(quantile, 1); // Round for equiv with HDR ( 2 digits )
                p.try_insert(pcn.clone(), quantile_dsp);
            }
            (
                histo.min().ok_or_else(|| err(&"Unable to calculate min"))?,
                histo.max().ok_or_else(|| err(&"Unable to calculate max"))?,
                histo.sum().ok_or_else(|| err(&"Unable to calculate sum"))?,
            )
        };
        let mut res = Value::object_with_capacity(5);
        res.try_insert("count", count);
        res.try_insert("min", min);
        res.try_insert("max", max);
        res.try_insert("mean", sum / count as f64);
        res.try_insert("percentiles", p);
        Ok(res)
    }

    // fn compensate<'event>(&mut self, _args: &[&Value<'event>]) -> FResult<()> {
    //     // TODO there's no facility for this with dds histogram, punt for now
    //     Ok(())
    // }

    fn merge(&mut self, src: &dyn TremorAggrFn) -> FResult<()> {
        let other: Option<&Self> = src.downcast_ref::<Self>();
        if let Some(other) = other {
            if !self.percentiles_set {
                self.percentiles = other.percentiles.clone();
                self.percentiles_set = true;
            };

            if let Some(ref mut histo) = self.histo {
                //  If this is a histogram and we merge
                if let Some(ref other) = other.histo {
                    // If the other was also a histogram merge them
                    histo.merge(other).ok();
                } else {
                    // if the other was still a cache add it's values
                    for v in &other.cache {
                        histo.add(*v);
                    }
                }
            } else {
                // If we were a cache
                match other.histo {
                    Some(ref other) => {
                        // If the other was a histogram clone it and empty our values
                        let mut histo: DDSketch = DDSketch::new(DDSketchConfig::defaults());
                        histo.merge(other).ok();
                        for v in self.cache.drain(..) {
                            histo.add(v);
                        }
                        self.histo = Some(histo)
                    }
                    None => {
                        // If both are caches
                        if self.cache.len() + other.cache.len() > HIST_MAX_CACHE_SIZE {
                            // If the cache size exceeds our maximal cache size drain them into a histogram
                            let mut histo: DDSketch = DDSketch::new(DDSketchConfig::defaults());
                            for v in self.cache.drain(..) {
                                histo.add(v);
                            }
                            for v in &other.cache {
                                histo.add(*v);
                            }
                            self.histo = Some(histo);
                        } else {
                            // If not append it's cache
                            self.cache.extend(&other.cache);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn init(&mut self) {
        self.histo = None;
        self.cache.clear();
    }
    #[cfg(not(tarpaulin_include))]
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
            //ALLOW: this values have been tested so an error can never be returned
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
}
impl TremorAggrFn for Hdr {
    fn accumulate<'event>(&mut self, args: &[&Value<'event>]) -> FResult<()> {
        if let Some(vals) = args.get(1).as_array() {
            if !self.percentiles_set {
                let percentiles: FResult<Vec<(String, f64)>> = vals
                    .iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .map(|s| {
                        let p = s.parse().map_err(|e| FunctionError::RuntimeError {
                            mfa: mfa("stats", "hdr", 2),
                            error: format!("Provided percentile '{}' isn't a float: {}", s, e),
                        })?;
                        Ok((s, p))
                    })
                    .collect();
                self.percentiles = percentiles?;
                self.percentiles_set = true
            }
        }
        if let Some(v) = args.first().cast_f64() {
            if v < 0.0 {
                return Ok(());
            }

            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let v = v as u64; // TODO add f64 support to HDR Histogram create -  oss
            if let Some(ref mut histo) = self.histo {
                histo.record(v).map_err(|e| FunctionError::RuntimeError {
                    mfa: mfa("stats", "hdr", 2),
                    error: format!("failed to record value: {:?}", e),
                })?;
            } else {
                if v > self.high_bound {
                    self.high_bound = v;
                }
                self.cache.push(v);
                if self.cache.len() == HIST_MAX_CACHE_SIZE {
                    let mut histo: Histogram<u64> =
                        Histogram::new_with_bounds(1, self.high_bound(), 2).map_err(|e| {
                            FunctionError::RuntimeError {
                                mfa: mfa("stats", "hdr", 2),
                                error: format!("failed to allocate hdr storage: {:?}", e),
                            }
                        })?;
                    histo.auto(true);
                    for v in self.cache.drain(..) {
                        histo.record(v).map_err(|e| FunctionError::RuntimeError {
                            mfa: mfa("stats", "hdr", 2),
                            error: format!("failed to record value: {:?}", e),
                        })?;
                    }
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
                self.percentiles = other.percentiles.clone();
                self.percentiles_set = true;
            };
            self.high_bound = max(self.high_bound, other.high_bound);

            if let Some(ref mut histo) = self.histo {
                //  If this is a histogram and we merge
                if let Some(ref other) = other.histo {
                    // If the other was also a histogram merge them
                    histo.add(other).map_err(|e| FunctionError::RuntimeError {
                        mfa: mfa("stats", "hdr", 2),
                        error: format!("failed to merge histograms: {:?}", e),
                    })?;
                } else {
                    // if the other was still a cache add it's values
                    for v in &other.cache {
                        histo.record(*v).map_err(|e| FunctionError::RuntimeError {
                            mfa: mfa("stats", "hdr", 2),
                            error: format!("failed to record value: {:?}", e),
                        })?;
                    }
                }
            } else {
                // If we were a cache
                if let Some(ref other) = other.histo {
                    // If the other was a histogram clone it and empty our values
                    let mut histo = other.clone();
                    for v in self.cache.drain(..) {
                        histo.record(v).map_err(|e| FunctionError::RuntimeError {
                            mfa: mfa("stats", "hdr", 2),
                            error: format!("failed to record value: {:?}", e),
                        })?;
                    }
                    self.histo = Some(histo)
                } else {
                    // If both are caches
                    if self.cache.len() + other.cache.len() > HIST_MAX_CACHE_SIZE {
                        // If the cache size exceeds our maximal cache size drain them into a histogram
                        self.high_bound = max(self.high_bound, other.high_bound);
                        let mut histo: Histogram<u64> =
                            Histogram::new_with_bounds(1, self.high_bound(), 2).map_err(|e| {
                                FunctionError::RuntimeError {
                                    mfa: mfa("stats", "hdr", 2),
                                    error: format!("failed to init historgrams: {:?}", e),
                                }
                            })?;
                        histo.auto(true);
                        for v in self.cache.drain(..) {
                            histo.record(v).map_err(|e| FunctionError::RuntimeError {
                                mfa: mfa("stats", "hdr", 2),
                                error: format!("failed to record value: {:?}", e),
                            })?;
                        }
                        for v in &other.cache {
                            histo.record(*v).map_err(|e| FunctionError::RuntimeError {
                                mfa: mfa("stats", "hdr", 2),
                                error: format!("failed to record value: {:?}", e),
                            })?;
                        }
                        self.histo = Some(histo);
                    } else {
                        // If not append it's cache
                        self.cache.extend(&other.cache);
                    }
                }
            }
        }
        Ok(())
    }

    // fn compensate<'event>(&mut self, _args: &[&Value<'event>]) -> FResult<()> {
    //     // TODO there's no facility for this with hdr histogram, punt for now
    //     Ok(())
    // }
    fn emit<'event>(&mut self) -> FResult<Value<'event>> {
        let mut p = hashmap! {};
        if let Some(histo) = &self.histo {
            for (pcn, percentile) in &self.percentiles {
                p.insert(
                    pcn.clone().into(),
                    Value::from(histo.value_at_percentile(percentile * 100.0)),
                );
            }
            Ok(Value::from(hashmap! {
                "count".into() => Value::from(histo.len()),
                "min".into() => Value::from(histo.min()),
                "max".into() => Value::from(histo.max()),
                "mean".into() => Value::from(histo.mean()),
                "stdev".into() => Value::from(histo.stdev()),
                "var".into() => Value::from(histo.stdev().powf(2.0)),
                "percentiles".into() => Value::from(p),
            }))
        } else {
            let mut histo: Histogram<u64> = Histogram::new_with_bounds(1, self.high_bound(), 2)
                .map_err(|e| FunctionError::RuntimeError {
                    mfa: mfa("stats", "hdr", 2),
                    error: format!("failed to init historgrams: {:?}", e),
                })?;
            histo.auto(true);
            for v in self.cache.drain(..) {
                histo.record(v).map_err(|e| FunctionError::RuntimeError {
                    mfa: mfa("stats", "hdr", 2),
                    error: format!("failed to record value: {:?}", e),
                })?;
            }
            for (pcn, percentile) in &self.percentiles {
                p.insert(
                    pcn.clone().into(),
                    Value::from(histo.value_at_percentile(percentile * 100.0)),
                );
            }
            let res = Value::from(hashmap! {
                "count".into() => Value::from(histo.len()),
                "min".into() => Value::from(histo.min()),
                "max".into() => Value::from(histo.max()),
                "mean".into() => Value::from(histo.mean()),
                "stdev".into() => Value::from(histo.stdev()),
                "var".into() => Value::from(histo.stdev().powf(2.0)),
                "percentiles".into() => Value::from(p),
            });
            self.histo = Some(histo);
            Ok(res)
        }
    }
    fn init(&mut self) {
        self.histo = None;
        self.high_bound = 0;
        self.cache.clear();
    }
    #[cfg(not(tarpaulin_include))]
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
            Box::new(Count::default()),
        ))
        .insert(TremorAggrFnWrapper::new(
            "stats".to_string(),
            "min".to_string(),
            Box::new(Min::default()),
        ))
        .insert(TremorAggrFnWrapper::new(
            "stats".to_string(),
            "max".to_string(),
            Box::new(Max::default()),
        ))
        .insert(TremorAggrFnWrapper::new(
            "stats".to_string(),
            "sum".to_string(),
            Box::new(Sum::default()),
        ))
        .insert(TremorAggrFnWrapper::new(
            "stats".to_string(),
            "var".to_string(),
            Box::new(Var::default()),
        ))
        .insert(TremorAggrFnWrapper::new(
            "stats".to_string(),
            "stdev".to_string(),
            Box::new(Stdev::default()),
        ))
        .insert(TremorAggrFnWrapper::new(
            "stats".to_string(),
            "mean".to_string(),
            Box::new(Mean::default()),
        ))
        .insert(TremorAggrFnWrapper::new(
            "stats".to_string(),
            "hdr".to_string(),
            Box::new(Hdr::default()),
        ))
        .insert(TremorAggrFnWrapper::new(
            "stats".to_string(),
            "dds".to_string(),
            Box::new(Dds::default()),
        ));
}

#[cfg(test)]
mod test {
    // use std::ffi::VaList;
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
        assert!((a.emit()?.cast_f64().expect("screw it") - (259.0 as f64 / 3.0).sqrt()) < 0.001);

        let mut b = Stdev::default();
        b.merge(&a)?;
        assert!((b.emit()?.cast_f64().expect("screw it") - (259.0 as f64 / 3.0).sqrt()) < 0.001);

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
    fn dds() -> Result<()> {
        use crate::Value;

        let mut a = Dds::default();
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
            "min": 1.0,
            "max": 100.0,
            "count": 100,
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

        assert_eq!(a.arity(), 1..=2);

        Ok(())
    }
}
