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

// Taken from: <https://github.com/serde-rs/json/blob/5b5f95831d9e0d769367b30b76a686339bffd209/src/macros.rs>

// The value_internal macro above cannot invoke vec directly because it uses
// local_inner_macros. A vec invocation there would resolve to $crate::vec.
// Instead invoke vec here outside of local_inner_macros.
#[macro_export]
#[doc(hidden)]
macro_rules! literal_internal_vec {
($($content:tt)*) => {
    vec![$($content)*]
};
}

#[macro_export]
#[doc(hidden)]
macro_rules! value_unexpected {
    () => {};
}

/// Convenience to generate tremor-value from json literals
///
/// Note that this literal macro does not support binary syntax literals
/// at this time.
///
/// Adapted from: <https://github.com/serde-rs/json/blob/5b5f95831d9e0d769367b30b76a686339bffd209/src/macros.rs>
/// Constructs a `tremor_value::Value` from a JSON literal.
///
/// Create a value literal of the form:
///
/// ```edition2021
/// # use tremor_value::{literal, Value};
/// #
/// let value: Value = literal!({
///     "code": 200,
///     "success": true,
///     "payload": {
///         "features": [
///             "serde",
///             "json"
///         ]
///     }
/// });
/// ```
///
///
/// Variables or expressions can be interpolated into the literal. Any type
/// interpolated into an array element or object value must implement Serde's
/// `Serialize` trait, while any type interpolated into an object key must
/// implement `Into<String>`. If the `Serialize` implementation of the
/// interpolated type decides to fail, or if the interpolated type contains a
/// map with non-string keys, the `literal!` macro will panic.
///
/// ```edition2021
/// # use tremor_value::literal;
/// #
/// let code = 200;
/// let features = vec!["serde", "json"];
///
/// let value = literal!({
///     "code": code,
///     "success": code == 200,
///     "payload": {
///         features[0]: features[1]
///     }
/// });
/// ```
///
/// Trailing commas are allowed inside both arrays and objects.
///
/// ```edition2021
/// # use tremor_value::literal;
/// #
/// let value = literal!([
///     "notice",
///     "the",
///     "trailing",
///     "comma -->",
/// ]);
/// ```
/// ````
#[macro_export(local_inner_macros)]
macro_rules! literal {
    // Hide distracting implementation details from the generated rustdoc.
    ($($json:tt)+) => {
        literal_internal!($($json)+)
    };
}

// Changes are fine as long as `json_internal!` does not call any new helper
// macros and can still be invoked as `json_internal!($($json)+)`.
#[macro_export(local_inner_macros)]
#[doc(hidden)]
macro_rules! literal_internal {
    //////////////////////////////////////////////////////////////////////////
    // TT muncher for parsing the inside of an array [...]. Produces a vec![...]
    // of the elements.
    //
    // Must be invoked as: literal_internal!(@array [] $($tt)*)
    //////////////////////////////////////////////////////////////////////////

    // Done with trailing comma.
    (@array [$($elems:expr,)*]) => {
        literal_internal_vec![$($elems,)*]
    };

    // Done without trailing comma.
    (@array [$($elems:expr),*]) => {
        literal_internal_vec![$($elems),*]
    };

    // Next element is `null`.
    (@array [$($elems:expr,)*] null $($rest:tt)*) => {
        literal_internal!(@array [$($elems,)* literal_internal!(null)] $($rest)*)
    };

    // Next element is `true`.
    (@array [$($elems:expr,)*] true $($rest:tt)*) => {
        literal_internal!(@array [$($elems,)* literal_internal!(true)] $($rest)*)
    };

    // Next element is `false`.
    (@array [$($elems:expr,)*] false $($rest:tt)*) => {
        literal_internal!(@array [$($elems,)* literal_internal!(false)] $($rest)*)
    };

    // Next element is an array.
    (@array [$($elems:expr,)*] [$($array:tt)*] $($rest:tt)*) => {
        literal_internal!(@array [$($elems,)* literal_internal!([$($array)*])] $($rest)*)
    };

    // Next element is a map.
    (@array [$($elems:expr,)*] {$($map:tt)*} $($rest:tt)*) => {
        literal_internal!(@array [$($elems,)* literal_internal!({$($map)*})] $($rest)*)
    };

    // Next element is an expression followed by comma.
    (@array [$($elems:expr,)*] $next:expr, $($rest:tt)*) => {
        literal_internal!(@array [$($elems,)* literal_internal!($next),] $($rest)*)
    };

    // Last element is an expression with no trailing comma.
    (@array [$($elems:expr,)*] $last:expr) => {
        literal_internal!(@array [$($elems,)* literal_internal!($last)])
    };

    // Comma after the most recent element.
    (@array [$($elems:expr),*] , $($rest:tt)*) => {
        literal_internal!(@array [$($elems,)*] $($rest)*)
    };

    // Unexpected token after most recent element.
    (@array [$($elems:expr),*] $unexpected:tt $($rest:tt)*) => {
        value_unexpected!($unexpected)
    };

    //////////////////////////////////////////////////////////////////////////
    // TT muncher for parsing the inside of an object {...}. Each entry is
    // inserted into the given map variable. Entries are parsed from the TT
    // and then inserted into a stack of entries to be appended all at once
    // at the end, allowing us to pre-allocate the map.
    //
    // Must be invoked as: json_internal!(@object $map () ($($tt)*) ($($tt)*))
    //
    // We require two copies of the input tokens so that we can match on one
    // copy and trigger errors on the other copy.
    //////////////////////////////////////////////////////////////////////////

    // Done.
    (@object $object:ident [@entries] () () ()) => {};

    // Count the number of entries to insert.
    // Modified version of https://docs.rs/halfbrown/0.1.11/src/halfbrown/macros.rs.html#46-62
    (@object @count [@entries $(($value:expr => $($key:tt)+))*]) => {
        <[()]>::len(&[$(literal_internal!(@object @count @single ($($key)+))),*])
    };
    (@object @count @single ($($key:tt)+)) => {()};

    // Done. Insert all entries from the stack
    (@object $object:ident [@entries $(($value:expr => $($key:tt)+))*] () () ()) => {
        let len = literal_internal!(@object @count [@entries $(($value:expr => $($key:tt)+))*]);
        $object = $crate::Object::with_capacity(len);
        $(
            // ALLOW: this is a macro, we don't care about the return value
            let _ = $object.insert(($($key)+).into(), $value);
        )*
    };

    // Insert the current entry (followed by trailing comma) into the stack.
    // Entries are inserted in reverse order, the captured $entries is expanded first,
    // keeping inserts in the same order that they're defined in the macro.
    //
    // We expand the $key _after_ the $value since $key => $value leads to a parsing ambiguity.
    (@object $object:ident [@entries $($entries:tt)*] [$($key:tt)+] ($value:expr) , $($rest:tt)*) => {
        literal_internal!(@object $object [@entries $($entries)* ($value => $($key)+) ] () ($($rest)*) ($($rest)*));
    };

    // Insert the last entry (without trailing comma) into the stack.
    // At this point the recursion is complete and the entries can now be inserted.
    (@object $object:ident [@entries $($entries:tt)*] [$($key:tt)+] ($value:expr)) => {
        literal_internal!(@object $object [@entries $($entries)* ($value => $($key)+) ] () () ());
    };

    // Current entry followed by unexpected token.
    (@object $object:ident [@entries $($entries:tt)*] [$($key:tt)+] ($value:expr) $unexpected:tt $($rest:tt)*) => {
        value_unexpected!($unexpected);
    };


    // Next value is `null`.
    (@object $object:ident [@entries $($entries:tt)*] ($($key:tt)+) (: null $($rest:tt)*) $copy:tt) => {
        literal_internal!(@object $object [@entries $($entries)*] [$($key)+] (literal_internal!(null)) $($rest)*);
    };

    // Next value is `true`.
    (@object $object:ident [@entries $($entries:tt)*] ($($key:tt)+) (: true $($rest:tt)*) $copy:tt) => {
        literal_internal!(@object $object [@entries $($entries)*] [$($key)+] (literal_internal!(true)) $($rest)*);
    };

    // Next value is `false`.
    (@object $object:ident [@entries $($entries:tt)*] ($($key:tt)+) (: false $($rest:tt)*) $copy:tt) => {
        literal_internal!(@object $object [@entries $($entries)*] [$($key)+] (literal_internal!(false)) $($rest)*);
    };

    // Next value is an array.
    (@object $object:ident [@entries $($entries:tt)*] ($($key:tt)+) (: [$($array:tt)*] $($rest:tt)*) $copy:tt) => {
        literal_internal!(@object $object [@entries $($entries)*] [$($key)+] (literal_internal!([$($array)*])) $($rest)*);
    };

    // Next value is a map.
    (@object $object:ident [@entries $($entries:tt)*] ($($key:tt)+) (: {$($map:tt)*} $($rest:tt)*) $copy:tt) => {
        literal_internal!(@object $object [@entries $($entries)*] [$($key)+] (literal_internal!({$($map)*})) $($rest)*);
    };

    // Next value is an expression followed by comma.
    (@object $object:ident [@entries $($entries:tt)*] ($($key:tt)+) (: $value:expr , $($rest:tt)*) $copy:tt) => {
        literal_internal!(@object $object [@entries $($entries)*] [$($key)+] (literal_internal!($value)) , $($rest)*);
    };

    // Last value is an expression with no trailing comma.
    (@object $object:ident [@entries $($entries:tt)*] ($($key:tt)+) (: $value:expr) $copy:tt) => {
        literal_internal!(@object $object [@entries $($entries)*] [$($key)+] (literal_internal!($value)));
    };

    // Missing value for last entry. Trigger a reasonable error message.
    (@object $object:ident [@entries $($entries:tt)*] ($($key:tt)+) (:) $copy:tt) => {
        // "unexpected end of macro invocation"
        literal_internal!();
    };

    // Missing colon and value for last entry. Trigger a reasonable error
    // message.
    (@object $object:ident [@entries $($entries:tt)*] ($($key:tt)+) () $copy:tt) => {
        // "unexpected end of macro invocation"
        literal_internal!();
    };

    // Misplaced colon. Trigger a reasonable error message.
    (@object $object:ident [@entries $($entries:tt)*] () (: $($rest:tt)*) ($colon:tt $($copy:tt)*)) => {
        // Takes no arguments so "no rules expected the token `:`".
        value_unexpected!($colon);
    };

    // Found a comma inside a key. Trigger a reasonable error message.
    (@object $object:ident [@entries $($entries:tt)*] ($($key:tt)*) (, $($rest:tt)*) ($comma:tt $($copy:tt)*)) => {
        // Takes no arguments so "no rules expected the token `,`".
        value_unexpected!($comma);
    };

    // Key is fully parenthesized. This avoids clippy double_parens false
    // positives because the parenthesization may be necessary here.
    (@object $object:ident [@entries $($entries:tt)*] () (($key:expr) : $($rest:tt)*) $copy:tt) => {
        literal_internal!(@object $object [@entries $($entries)*] ($key) (: $($rest)*) (: $($rest)*));
    };

    // Munch a token into the current key.
    (@object $object:ident [@entries $($entries:tt)*] ($($key:tt)*) ($tt:tt $($rest:tt)*) $copy:tt) => {
        literal_internal!(@object $object [@entries $($entries)*] ($($key)* $tt) ($($rest)*) ($($rest)*));
    };

    //////////////////////////////////////////////////////////////////////////
    // The main implementation.
    //
    // Must be invoked as: json_internal!($($json)+)
    //////////////////////////////////////////////////////////////////////////

    (null) => {
        $crate::Value::Static($crate::StaticNode::Null)
    };

    (true) => {
        $crate::Value::Static($crate::StaticNode::Bool(true))
    };

    (false) => {
        $crate::Value::Static($crate::StaticNode::Bool(false))
    };

    ([]) => {
        $crate::Value::Array(literal_internal_vec![])
    };

    ([ $($tt:tt)+ ]) => {
        $crate::Value::Array(literal_internal!(@array [] $($tt)+))
    };

    ({}) => {
        {
            use $crate::prelude::BuilderTrait;
            $crate::Value::object()
        }
    };

    ({ $($tt:tt)+ }) => {
        $crate::Value::from({
            let mut object;
            literal_internal!(@object object [@entries] () ($($tt)+) ($($tt)+));
            object
        })
    };

    // Any Serialize type: numbers, strings, struct literals, variables etc.
    // Must be below every other rule.
    ($other:expr) => {
        $crate::Value::from($other)
    };
}

#[cfg(test)]
mod tests {
    use crate::Value;

    #[test]
    fn value_macro() {
        let v = literal!({ "snot": "badger"});
        assert_eq!(literal!({ "snot": "badger"}), v);
    }

    #[test]
    fn array() {
        let v: Value = literal!(vec![1]);
        assert_eq!(Value::from(vec![1_u64]), v);
        let v: Value = literal!([1]);
        assert_eq!(Value::from(vec![1_u64]), v);
        let v: Value = literal!([]);
        assert_eq!(Value::Array(vec![]), v);
    }
}
