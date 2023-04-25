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
use crate::registry::Registry;
use crate::tremor_const_fn;
use tremor_value::StaticNode;

pub fn load(registry: &mut Registry) {
    registry
        .insert(
            tremor_const_fn! (path|try_default(_context, _base, _segments, _otherwise) {
                let base = _base.clone_static();
                let mut pivot = &base;

                if let Value::Object(root) = _base {
                    if let Value::Array(segments) = _segments {
                        let segment_len = &segments.len();
                        let mut i = 0;
                        for segment in segments {
                            if let Some(field) = segment.as_str() {
                                if let Value::Object(test) = pivot {
                                    if let Some(next) = test.get(field) {
                                        pivot = next;
                                        i += 1;
                                        continue;
                                    }

                                    // NOTE This is non-strict for dynamic / computed uses of the fn - we return the default rather than a runtime error

                                    return Ok(_otherwise.clone_static());
                                }
                            }

                            if let Value::Array(test) = pivot {
                                if let Some(index) = segment.as_usize() {
                                    if let Some(next) = test.get(index) {
                                        pivot = next;
                                        i += 1;
                                        continue;
                                    }
                                }

                                // NOTE This is non-strict for dynamic / computed uses of the fn - we return the default rather than a runtime error

                                return Ok(_otherwise.clone_static());
                            }
                        }
                        if i > 0 && segments.len() > i {
                            // NOTE This is non-strict for dynamic / computed uses of the fn - we return the default rather than a runtime error
                            return Ok(_otherwise.clone_static());
                        }

                        Ok(pivot.clone_static())
                    } else {
                        Err(FunctionError::RuntimeError{mfa: this_mfa(), error: "Expected path segment array as second argument".into()})
                    }
                } else if let Value::Array(root) = _base {
                    let mut i = 0;
                    if let Value::Array(segments) = _segments {
                        for segment in segments {
                            if let Some(field) = segment.as_str() {
                                if let Value::Object(test) = pivot {
                                    if let Some(next) = test.get(field) {
                                        pivot = next;
                                        i += 1;
                                        continue;
                                    }

                                    // NOTE This is non-strict for dynamic / computed uses of the fn - we return the default rather than a runtime error
                                    return Ok(_otherwise.clone_static());
                                }
                            }

                            if let Value::Array(test) = pivot {
                                if let Some(index) = segment.as_usize() {
                                    if let Some(next) = test.get(index) {
                                        pivot = next;
                                        i += 1;
                                        continue;
                                    }
                                }

                                // NOTE This is non-strict for dynamic / computed uses of the fn - we return the default rather than a runtime error
                                return Ok(_otherwise.clone_static());
                            }
                        }
                        if i > 0 && segments.len() > i {
                            // NOTE This is non-strict for dynamic / computed uses of the fn - we return the default rather than a runtime error
                            return Ok(_otherwise.clone_static());
                        }

                        Ok(pivot.clone_static())
                    } else {
                        Err(FunctionError::RuntimeError{mfa: this_mfa(), error: "Expected path segment array as second argument".into()})
                    }
                } else if let Value::Static(StaticNode::Null) = _base {
                    return Ok(_otherwise.clone_static());
                } else {
                     Err(FunctionError::RuntimeError{mfa: this_mfa(), error: "Expected record or array or literal `null` as first argument".into()})
                }
            }
        ));
}

#[cfg(test)]
mod test {
    use crate::registry::fun;
    use crate::registry::FunctionError;
    use crate::registry::Mfa;
    use tremor_value::literal;

    fn try_default_mfa() -> Mfa {
        Mfa::new("path", "try_default", 3)
    }

    #[test]
    fn path_default_base_is_invalid() {
        let f = fun("path", "try_default");

        let base = literal!(1);
        let segments = literal!([]);
        let otherwise = literal!("error");

        let e = f(&[&base, &segments, &otherwise]);

        assert_eq!(
            Err(FunctionError::RuntimeError {
                mfa: try_default_mfa(),
                error: "Expected record or array or literal `null` as first argument".to_string(),
            }),
            e
        );
    }

    #[test]
    fn path_default_segments_is_invalid() {
        let f = fun("path", "try_default");

        let base = literal!({});
        let segments = literal!(1);
        let otherwise = literal!("error");

        let e = f(&[&base, &segments, &otherwise]);
        assert!(e.is_err());
        assert_eq!(
            Some(FunctionError::RuntimeError {
                mfa: try_default_mfa(),
                error: "Expected path segment array as second argument".to_string(),
            }),
            e.err()
        );

        let base = literal!([]);
        let segments = literal!(1);
        let otherwise = literal!("error");

        let e = f(&[&base, &segments, &otherwise]);
        assert!(e.is_err());
        assert_eq!(
            Some(FunctionError::RuntimeError {
                mfa: try_default_mfa(),
                error: "Expected path segment array as second argument".to_string(),
            }),
            e.err()
        );
    }

    #[test]
    fn path_default_when_base_is_null_defaults() {
        let f = fun("path", "try_default");

        let base = literal!(null);
        let segments = literal!([]);
        let otherwise = literal!("pass");

        assert_val!(f(&[&base, &segments, &otherwise]), "pass");
    }

    #[test]
    fn path_default_depth_exceeds_base_defaults() {
        let f = fun("path", "try_default");

        let base = literal!({"snot": "badger"});
        let segments = literal!(["snot", "badger"]);
        let otherwise = literal!("pass");

        assert_val!(f(&[&base, &segments, &otherwise]), "pass");

        let base = literal!(["snot", "badger"]);
        let segments = literal!(["snot", "badger"]);
        let otherwise = literal!("pass");

        assert_val!(f(&[&base, &segments, &otherwise]), "pass");
    }

    #[test]
    fn path_default_ok() {
        let f = fun("path", "try_default");

        let base = literal!({"snot": { "snot": "badger" }});
        let segments = literal!(["snot", "snot"]);
        let otherwise = literal!("not_used_for_this_call");

        assert_val!(f(&[&base, &segments, &otherwise]), "badger");

        let base = literal!(["snot", ["snot", "badger"]]);
        let segments = literal!([1, 1]);
        let otherwise = literal!("not_used_for_this_call");

        assert_val!(f(&[&base, &segments, &otherwise]), "badger");
    }
}
