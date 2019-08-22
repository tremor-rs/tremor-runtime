# The `array` namespace

The `array` module contains functions to work with arrays.

### array::len(array) -> int

Returns the length of an array.

### array::is_empty(array) -> bool

Returns if an array is empty.

### array::contains(array, element) -> bool

Returns if an array contains an element.

### array::push(array, element) -> array

Adds an `element` to an array.

### array::zip(left, right) -> array

Zips two arrays, returning a new array of tuples with the first element being part of the `left` array and the second element part of the `right` array.

Note: `left` and `right` need to have the same length.

```rust
left = [1, 2, 3];
right = ["a", "b", "c"];
array::zip(left, right) == [[1, "a"], [2, "b"], [3, "c"]]
```

### array::unzip(array) -> array

Unzips an array of tuples into an array of two arrays.

Note: `array`'s elements need to be arrays of two elements.

```rust
array::unzip([[1, "a"], [2, "b"], [3, "c"]]) ==  [[1, 2, 3], ["a", "b", "c"]]
```

### array::flatten(array) -> array

Flattens a nested array recursively.

```rust
array::flatten([[1, 2, 3], ["a", "b", "c"]]) = [1, 2, 3, "a", "b", "c"]
```

### array::coalesce(array) -> array

Returns the array with `null` values removed.

```rust
array::coalesce([1, null, 2, null, 3]) = [1, 2, 3]
```

### array::join(array, string) -> string

Joins the elements of an array (turing them into Strings) with a given separator.

```rust
array:join(["this", "is", "a", "cake"], " ") => "this is a cake"
```
