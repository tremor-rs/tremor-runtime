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

use crate::registry::{mfa, FResult, FunctionError, Registry, TremorFn, TremorFnWrapper};
use crate::tremor_fn;
use crate::EventContext;
use rand::distributions::Alphanumeric;
use rand::{rngs::SmallRng, Rng, SeedableRng};
use simd_json::{BorrowedValue as Value, ValueTrait};

pub fn load(registry: &mut Registry) {
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
            match args.len() {
                2 => {
                    let (low, high) = (&args[0], &args[1]);
                    match (low, high) {
                    (Value::I64(low), Value::I64(high)) if low < high => Ok(Value::I64(
                        // random integer between low and high (not including high)
                        rng.gen_range(low, high),
                    )),
                    (Value::I64(_), Value::I64(_)) => Err(FunctionError::RuntimeError {
                        mfa: this_mfa(),
                        error:
                            "Invalid arguments. First argument must be lower than second argument"
                                .to_string(),
                    }),
                    _ => Err(FunctionError::BadType { mfa: this_mfa() }),
                }
                }
                1 => {
                    let input = &args[0];
                    match input {
                        Value::I64(input) if *input > 0 => Ok(Value::I64(
                            // random integer between 0 and input (not including input)
                            rng.gen_range(0, input),
                        )),
                        Value::I64(_) => Err(FunctionError::RuntimeError {
                            mfa: this_mfa(),
                            error: "Invalid argument. Must be greater than 0".to_string(),
                        }),
                        _ => Err(FunctionError::BadType { mfa: this_mfa() }),
                    }
                }
                0 => Ok(Value::I64(
                    rng.gen(), // random integer
                )),
                _ => Err(FunctionError::BadArity {
                    mfa: this_mfa(),
                    calling_a: args.len(),
                }),
            }
        }
        fn snot_clone(&self) -> Box<dyn TremorFn> {
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
            match args.len() {
                2 => {
                    let (low, high) = (&args[0], &args[1]);
                    match (low, high) {
                    (Value::F64(low), Value::F64(high)) if low < high => Ok(Value::F64(
                        // random float between low and high (not including high)
                        rng.gen_range(low, high),
                    )),
                    (Value::F64(_), Value::F64(_)) => Err(FunctionError::RuntimeError {
                        mfa: this_mfa(),
                        error:
                            "Invalid arguments. First argument must be lower than second argument"
                                .to_string(),
                    }),
                    _ => Err(FunctionError::BadType { mfa: this_mfa() }),
                }
                }
                1 => {
                    let input = &args[0];
                    match input {
                        Value::F64(input) if *input > 0.0 => Ok(Value::F64(
                            // random float between 0 and input (not including input)
                            rng.gen_range(0.0, input),
                        )),
                        Value::F64(_) => Err(FunctionError::RuntimeError {
                            mfa: this_mfa(),
                            error: "Invalid argument. Must be greater than 0.0".to_string(),
                        }),
                        _ => Err(FunctionError::BadType { mfa: this_mfa() }),
                    }
                }
                0 => Ok(Value::F64(
                    rng.gen(), // random float (between 0.0 and 1.0, not including 1.0)
                )),
                _ => Err(FunctionError::BadArity {
                    mfa: this_mfa(),
                    calling_a: args.len(),
                }),
            }
        }

        fn snot_clone(&self) -> Box<dyn TremorFn> {
            Box::new(self.clone())
        }
        fn arity(&self) -> std::ops::RangeInclusive<usize> {
            0..=2
        }
        fn is_const(&self) -> bool {
            false
        }
    }
    registry
        .insert(tremor_fn! (random::bool(_context) {
            Ok(Value::Bool(
                SmallRng::seed_from_u64(_context.ingest_ns())
                    .gen()
            ))
        }))
        // TODO support specifying range of characters as a second (optional) arg
        .insert(tremor_fn! (random::string(_context, _length) {
            if let Some(n) = _length.as_usize() {
                // random string with chars uniformly distributed over ASCII letters and numbers
                Ok(Value::String(
                 SmallRng::seed_from_u64(_context.ingest_ns())
                    .sample_iter(&Alphanumeric).take(n).collect()
                ))
            } else {
                Err(FunctionError::BadType{mfa: this_mfa()})
            }
        }))
        .insert(TremorFnWrapper {
            module: "random".to_string(),
            name: "integer".to_string(),
            fun: Box::new(RandomInteger::default()),
        })
        .insert(TremorFnWrapper {
            module: "random".to_string(),
            name: "float".to_string(),
            fun: Box::new(RandomFloat::default()),
        });
}

#[cfg(test)]
mod test {
    use crate::registry::fun;
    use simd_json::BorrowedValue as Value;

    macro_rules! assert_val {
        ($e:expr, $r:expr) => {
            assert_eq!($e, Ok(Value::from($r)))
        };
    }

    #[test]
    fn bool() {
        let f = fun("random", "bool");
        assert!(match f(&[]) {
            Ok(Value::Bool(_)) => true,
            _ => false,
        });
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
        assert!(match f(&[]) {
            Ok(Value::I64(_)) => true,
            _ => false,
        });
    }

    #[test]
    fn float() {
        let f = fun("random", "float");
        let v1 = 0.0;
        let v2 = 100.0;
        assert!(match f(&[&Value::from(v1), &Value::from(v2)]) {
            Ok(Value::F64(a)) if a >= v1 && a < v2 => true,
            _ => false,
        });
        let v = 100.0;
        assert!(match f(&[&Value::from(v)]) {
            Ok(Value::F64(a)) if a >= 0.0 && a < v => true,
            _ => false,
        });
        assert!(match f(&[]) {
            Ok(Value::F64(a)) if a >= 0.0 && a < 1.0 => true,
            _ => false,
        });
    }
}
