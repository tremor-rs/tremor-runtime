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

use crate::interpreter::val_eq;
use crate::prelude::*;
use crate::tremor_fn;
use crate::{registry::Registry, TRUE};

pub fn load(registry: &mut Registry) {
    registry.insert(tremor_fn! (test|assert(ctx, desc, expected, got) {
        if val_eq(expected, got) {
            Ok(TRUE)
        } else if ctx.panic_on_assert {
            Err(to_runtime_error(format!(r#"
Assertion for {desc} failed:
    expected: {}
    got: {}
"#, expected.encode(), got.encode())))
        } else {
            Ok(Value::from(vec![(*expected).clone(), (*got).clone()]))
        }
    }));
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::{registry::FResult, NO_CONTEXT};

    #[test]
    fn test() -> FResult<()> {
        let mut reg = Registry::default();
        load(&mut reg);
        let fun = reg.find("test", "assert")?;
        let mut ctx = NO_CONTEXT;
        let one = Value::from(1_u64);
        let two = Value::from(2_i64);
        assert_eq!(
            fun.invoke(&ctx, &[&Value::from("DESC"), &one, &one]),
            Ok(Value::from(true))
        );

        assert_eq!(
            fun.invoke(&ctx, &[&Value::from("DESC"), &one, &two]),
            Ok(Value::from(vec![one.clone(), two.clone()]))
        );

        ctx.panic_on_assert = true;
        assert!(fun
            .invoke(&ctx, &[&Value::from("DESC"), &one, &two])
            .is_err());
        Ok(())
    }
}
