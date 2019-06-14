# The `record` namespace

The `record` module contains functions to work with objects.

### record::len(object) -> bool

Returns the length of an record (number of key value pairs).

### record::is_empty(object) -> bool

Returns if an record is empty.

### record::contains(object, key) -> bool

Returns if an record contains a given key.

### record::to_array(object) -> array

Turns the `record` into an array of key value pairs.

Example:

```rust
record::to_array({"a": 1, "b": 2}) == [["a", 1], ["b", 2]]
```

### record::from_array(array) -> object

Turns an `array` of key value pairs into an record.

Note: `array`'s elements need to be arrays of two elements with the first element being a string.

**Example:**

```rust
record::from_array([["a", 1], ["b", 2]]) == {"a": 1, "b": 2}
```

### record::select(object, array) -> object

'Selects' a given set of field from an `record`, removing all others.

**Example:**

```rust
record::to_array({"a": 1, "b": 2, "c": 3}, ["a", "c"]) == {"a": 1, "c": 3}
```

### record::merge(left, right) -> object

Merges the two records `left` and `right` overwriting existing values in `left` with those provided in `right`

**Example:**

```rust
record::merge({"a": 1, "b": 2, "c": 4}, {"c": 3, "d": 4}) == {"a": 1, "b": 2, "c": 3, "d": 4}
```
