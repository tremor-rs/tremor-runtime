# The `object` namespace

The `object` module contains functions to work with objects.

### object::len(object) -> bool

Returns the length of an object (number of key value pairs).

### object::is_empty(object) -> bool

Returns if an object is empty.

### object::contains(object, key) -> bool

Returns if an object contains a given key.

### object::to_array(object) -> array

Turns the `object` into an array of key value pairs.

Example:

```rust
object::to_array({"a": 1, "b": 2}) == [["a", 1], ["b", 2]]
```



### object::from_array(array) -> object

Turns an `array` of key value pairs into an object. 

Note: `array`'s elements need to be arrays of two elements with the first element being a string.

**Example:**

```rust
object::from_array([["a", 1], ["b", 2]]) == {"a": 1, "b": 2}
```

### object::select(object, array) -> object

'Selects' a given set of field from an `object`, removing all others.

**Example:**

```rust
object::to_array({"a": 1, "b": 2, "c": 3}, ["a", "c"]) == {"a": 1, "c": 3}
```



### object::merge(left, right) -> object

Merges the two objects `left` and `right` overwriting existing values in `left` with those provided in `right`

**Example:**

```rust
object::merge({"a": 1, "b": 2, "c": 4}, {"c": 3, "d": 4}) == {"a": 1, "b": 2, "c": 3, "d": 4}
```

