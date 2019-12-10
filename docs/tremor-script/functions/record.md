# The `record` namespace

The `record` module contains functions to work with records.

## Functions

### record::len(record) -> bool

Returns the length of an record (number of key value pairs).

### record::is_empty(record) -> bool

Returns if an record is empty.

### record::contains(record, key) -> bool

Returns if an record contains a given key.

### record::keys(record) -> array

Returns an array of record keys.

```tremor
record::keys({"a": 1, "b": 2}) == ["a", "b"]
```

### record::values(record) -> array

Returns an array of record values.

```tremor
record::values({"a": 1, "b": 2}) == [1, 2]
```

### record::to_array(record) -> array

Turns the `record` into an array of key value pairs.

```tremor
record::to_array({"a": 1, "b": 2}) == [["a", 1], ["b", 2]]
```

### record::from_array(array) -> record

Turns an `array` of key value pairs into an record.

Note: `array`'s elements need to be arrays of two elements with the first element being a string.

```tremor
record::from_array([["a", 1], ["b", 2]]) == {"a": 1, "b": 2}
```

### record::select(record, array) -> record

'Selects' a given set of field from an `record`, removing all others.

```tremor
record::select({"a": 1, "b": 2, "c": 3}, ["a", "c"]) == {"a": 1, "c": 3}
```

### record::merge(left, right) -> record

Merges the two records `left` and `right` overwriting existing values in `left` with those provided in `right`

```tremor
record::merge({"a": 1, "b": 2, "c": 4}, {"c": 3, "d": 4}) == {"a": 1, "b": 2, "c": 3, "d": 4}
```

### record::rename(target, changes) -> record

Renames the keys in the record  `target` based on the key value pairs in the record `changes` where the `key` is the current name and the `value` is the new name.

```tremor
record::rename({"a": 1, "b": 2, "c": 4}, {"a": "A", "b": "B"}) == {"A": 1, "B": 2, "c": 4}
```
