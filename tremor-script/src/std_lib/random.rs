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

use crate::prelude::*;
use crate::registry::{mfa, FResult, FunctionError, Registry, TremorFn, TremorFnWrapper};
use crate::tremor_fn;
use crate::EventContext;
use crate::Value;
use rand::distributions::Alphanumeric;
use rand::{rngs::SmallRng, Rng, SeedableRng};

// TODO see if we can cache the RNG here across function calls (or at least
// at the thread level, like via rand::thread_rng()
//
// also, `rng.gen_range()` calls here are optimized for a single sample from
// the range. we will be sampling a lot during a typical use case, so swap it
// for a distribution based sampling (if we can cache the distribution):
// https://docs.rs/rand/0.7.0/rand/trait.Rng.html#method.gen_range
#[derive(Clone, Debug, Default)]
struct RandomInteger {}
impl TremorFn for RandomInteger {
    fn invoke<'event, 'c>(
        &self,
        ctx: &'c EventContext,
        args: &[&Value<'event>],
    ) -> FResult<Value<'event>> {
        let this_mfa = || mfa("random", "integer", args.len());
        // TODO add event id to the seed? also change ingest_ns() for tremor-script binary runs too
        let mut rng = SmallRng::seed_from_u64(ctx.ingest_ns());
        match args {
            [low, high] => {
                if let (Some(low), Some(high)) = (low.as_i64(), high.as_i64()) {
                    if low < high {
                        // random integer between low and high (not including high)
                        Ok(Value::from(rng.gen_range(low..high)))
                    } else {
                        Err(FunctionError::RuntimeError {
                                mfa: this_mfa(),
                                error:
                                    "Invalid arguments. First argument must be lower than second argument".to_string(),
                            })
                    }
                } else {
                    Err(FunctionError::BadType { mfa: this_mfa() })
                }
            }
            [input] => {
                input.as_u64().map_or_else(
                    || Err(FunctionError::BadType { mfa: this_mfa() }),
                    |input| {
                        if input == 0 {
                            // avoid panic in gen_range
                            Err(FunctionError::RuntimeError {
                                mfa: this_mfa(),
                                error: "Invalid arguments. Single argument value must be > 0."
                                    .to_string(),
                            })
                        } else {
                            // random integer between 0 and input (not including input)
                            Ok(Value::from(rng.gen_range(0..input)))
                        }
                    },
                )
            }
            [] => Ok(Value::from(
                rng.gen::<i64>(), // random integer
            )),
            _ => Err(FunctionError::BadArity {
                mfa: this_mfa(),
                calling_a: args.len(),
            }),
        }
    }
    fn boxed_clone(&self) -> Box<dyn TremorFn> {
        Box::new(self.clone())
    }
    fn arity(&self) -> std::ops::RangeInclusive<usize> {
        0..=2
    }
    fn is_const(&self) -> bool {
        false
    }
}
// TODO try to consolidate this with the integer implementation -- mostly a copy-pasta
// of that right now, with types changed
#[derive(Clone, Debug, Default)]
struct RandomFloat {}
impl TremorFn for RandomFloat {
    fn invoke<'event, 'c>(
        &self,
        ctx: &'c EventContext,
        args: &[&Value<'event>],
    ) -> FResult<Value<'event>> {
        let this_mfa = || mfa("random", "float", args.len());
        let mut rng = SmallRng::seed_from_u64(ctx.ingest_ns());
        match args {
            [low, high] => {
                if let (Some(low), Some(high)) = (low.cast_f64(), high.cast_f64()) {
                    if low < high {
                        // random integer between low and high (not including high)
                        Ok(Value::from(rng.gen_range(low..high)))
                    } else {
                        Err(FunctionError::RuntimeError {
                                mfa: this_mfa(),
                                error:
                                    "Invalid arguments. First argument must be lower than second argument".to_string(),
                            })
                    }
                } else {
                    Err(FunctionError::BadType { mfa: this_mfa() })
                }
            }
            [input] => {
                input.cast_f64().map_or_else(
                    || Err(FunctionError::BadType { mfa: this_mfa() }),
                    |input| {
                        if input <= 0.0 {
                            // avoid panic in gen_range
                            Err(FunctionError::RuntimeError {
                                mfa: this_mfa(),
                                error: "Invalid arguments. Single argument value must be > 0.0."
                                    .to_string(),
                            })
                        } else {
                            // random integer between 0 and input (not including input)
                            Ok(Value::from(rng.gen_range(0.0..input)))
                        }
                    },
                )
            }
            [] => Ok(Value::from(
                rng.gen::<f64>(), // random integer
            )),
            _ => Err(FunctionError::BadArity {
                mfa: this_mfa(),
                calling_a: args.len(),
            }),
        }
    }

    fn boxed_clone(&self) -> Box<dyn TremorFn> {
        Box::new(self.clone())
    }
    fn arity(&self) -> std::ops::RangeInclusive<usize> {
        0..=2
    }
    fn is_const(&self) -> bool {
        false
    }
}

pub fn load(registry: &mut Registry) {
    registry
        .insert(tremor_fn! (random::bool(_context) {
            Ok(Value::from(
                SmallRng::seed_from_u64(_context.ingest_ns())
                    .gen::<bool>()
            ))
        }))
        // TODO support specifying range of characters as a second (optional) arg
        .insert(tremor_fn! (random::string(_context, _length) {
            _length.as_usize().map_or_else(
                ||Err(FunctionError::BadType{mfa: this_mfa()}),
                |n| {
                // random string with chars uniformly distributed over ASCII letters and numbers
                Ok(Value::from(
                 SmallRng::seed_from_u64(_context.ingest_ns())
                    .sample_iter(&Alphanumeric).map(char::from).take(n).collect::<String>()
                ))
                }
            )
        }))
        .insert(TremorFnWrapper::new(
            "random".to_string(),
            "integer".to_string(),
            Box::new(RandomInteger::default()),
        ))
        .insert(TremorFnWrapper::new(
            "random".to_string(),
            "float".to_string(),
            Box::new(RandomFloat::default()),
        ));
}

#[cfg(test)]
mod test {
    use crate::registry::fun;
    use crate::Value;
    use proptest::prelude::*;
    use simd_json::Value as ValueTrait;

    #[test]
    fn bool() {
        let f = fun("random", "bool");
        assert!(f(&[]).ok().map(|v| v.is_bool()).unwrap_or_default());
    }

    #[test]
    fn string() {
        let f = fun("random", "string");
        let n = 0;
        assert_val!(f(&[&Value::from(n)]), "");
        let n = 16;
        assert!(match f(&[&Value::from(n)]) {
            Ok(Value::String(s)) => s.len() as i64 == n,
            _ => false,
        });
    }

    #[test]
    fn integer() {
        let f = fun("random", "integer");
        let v1 = Value::from(0);
        let v2 = Value::from(1);
        assert_val!(f(&[&v1, &v2]), 0);
        let v1 = Value::from(-42);
        let v2 = Value::from(-41);
        assert_val!(f(&[&v1, &v2]), -42);
        let v = Value::from(1);
        assert_val!(f(&[&v]), 0);
        assert!(f(&[]).ok().map(|v| v.is_i64()).unwrap_or_default());
        let null = Value::from(0);
        assert!(f(&[&null]).is_err());
    }

    proptest! {

        #[test]
        #[cfg(not(tarpaulin))] // avoid coverage from this
            fn integer_single_arg_no_error(x in 1u64..u64::MAX) { // exclude 0 here
            let f = fun("random", "integer");
            let v = Value::from(x);
            assert!(f(&[&v]).is_ok())
        }
    }

    #[test]
    fn float() {
        let f = fun("random", "float");
        let v1 = 0.0;
        let v2 = 100.0;
        assert!(f(&[&Value::from(v1), &Value::from(v2)])
            .ok()
            .and_then(|v| v.as_f64())
            .map(|a| a >= v1 && a < v2)
            .unwrap_or_default());
        let v = 100.0;
        assert!(f(&[&Value::from(v)])
            .ok()
            .and_then(|v| v.as_f64())
            .map(|a| a >= 0.0 && a < v)
            .unwrap_or_default());
        let null = Value::from(0.0);
        assert!(f(&[&null]).is_err());
        assert!(f(&[])
            .ok()
            .and_then(|v| v.as_f64())
            .map(|a| a >= 0.0 && a < 1.0)
            .unwrap_or_default());
    }

    proptest! {
        #[test]
        #[cfg(not(tarpaulin))]
        fn float_single_arg_no_error(x in (0.0f64..f64::MAX).prop_filter("Values must be > 0.0", |x| *x > 0.0)) {
            let f = fun("random", "float");
            let v = Value::from(x);
            assert!(f(&[&v]).is_ok())
        }
    }
}
