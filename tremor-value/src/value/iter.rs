// Copyright 2022, The Tremor Team
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

use super::Value;
use value_trait::ValueAccess;

/// Iterator over value references
pub struct ValueIter<'value, 'borrow>(Box<dyn Iterator<Item = &'borrow Value<'value>> + 'borrow>)
where
    'value: 'borrow;

impl<'value, 'borrow> Iterator for ValueIter<'value, 'borrow> {
    type Item = &'borrow Value<'value>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }

    fn count(self) -> usize
    where
        Self: Sized,
    {
        self.0.count()
    }

    fn last(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        self.0.last()
    }
}

/// Iterator over mutable references of values
pub struct MutValueIter<'value, 'borrow>(
    Box<dyn Iterator<Item = &'borrow mut Value<'value>> + 'borrow>,
)
where
    'value: 'borrow;

impl<'value, 'borrow> Iterator for MutValueIter<'value, 'borrow> {
    type Item = &'borrow mut Value<'value>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }

    fn count(self) -> usize
    where
        Self: Sized,
    {
        self.0.count()
    }

    fn last(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        self.0.last()
    }
}

impl<'value> Value<'value> {
    /// Return an iterator over array values.
    /// For non-arrays, returns `None`.
    #[must_use]
    pub fn array_iter<'borrow>(
        &'borrow self,
    ) -> Option<impl Iterator<Item = &'borrow Value<'value>>> {
        self.as_array().map(|a| a.iter())
    }
}

impl<'value> IntoIterator for Value<'value> {
    type Item = Value<'value>;

    type IntoIter = <Vec<Value<'value>> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Value::Array(vec) => vec.into_iter(),
            _ => vec![self].into_iter(),
        }
    }
}

impl<'value, 'borrow> IntoIterator for &'borrow Value<'value> {
    type Item = &'borrow Value<'value>;

    type IntoIter = ValueIter<'value, 'borrow>;

    fn into_iter(self) -> Self::IntoIter {
        if let Some(a) = self.as_array() {
            ValueIter(Box::new(a.iter()))
        } else {
            ValueIter(Box::new(std::iter::once(self)))
        }
    }
}

impl<'value, 'borrow> IntoIterator for &'borrow mut Value<'value> {
    type Item = &'borrow mut Value<'value>;

    type IntoIter = MutValueIter<'value, 'borrow>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Value::Array(vec) => MutValueIter(Box::new(vec.iter_mut())),
            s => MutValueIter(Box::new(std::iter::once(s))),
        }
    }
}

#[cfg(test)]
mod tests {
    use simd_json::{Value, ValueAccess};

    use crate::literal;

    #[test]
    fn value_iter() {
        let value = literal!(true);
        assert!(value.array_iter().is_none());
        assert!(literal!("").array_iter().is_none());
        assert!(literal!(14.6789).array_iter().is_none());
        assert!(literal!({}).array_iter().is_none());

        let empty = literal!([]);
        let mut iter = empty
            .array_iter()
            .expect("Expect an iter for an empty array");
        assert!(iter.next().is_none());

        let array = literal!([{}, null, [1, 2.5, true, "string", null], 1]);
        let mut iter = array.array_iter().expect("Expect an iter for array value");
        assert_eq!(Some(&literal!({})), iter.next());
        assert_eq!(Some(&literal!(null)), iter.next());
        assert_eq!(Some(&literal!([1, 2.5, true, "string", null])), iter.next());
        assert_eq!(Some(&literal!(1)), iter.next());
        assert_eq!(None, iter.next());
    }

    #[test]
    fn value_into_iter() {
        let mut value = literal!([true]);
        for elem in &value {
            assert_eq!(Some(true), elem.as_bool());
        }
        for elem in &mut value {
            assert_eq!(Some(true), elem.as_bool());
        }
        for elem in value {
            assert_eq!(Some(true), elem.as_bool());
        }

        let mut non_array = literal!({});
        for elem in &non_array {
            assert!(elem.is_object());
            assert_eq!(0, elem.as_object().map(|o| o.len()).unwrap_or(1000))
        }

        for elem in &mut non_array {
            assert!(elem.is_object());
            assert_eq!(0, elem.as_object().map(|o| o.len()).unwrap_or(1000))
        }

        for elem in non_array {
            assert!(elem.is_object());
            assert_eq!(0, elem.as_object().map(|o| o.len()).unwrap_or(1000))
        }
    }
}
