# The `re` namespace

The `re` module contains functions for regular expression handing. Please note that if applicable literal regular expressions are faster.

### re::replace(regex, input, to) -> string

Replaces the **first** occurrence of `regex` in the `input` string with `to`.

References to match groups can be done using `$` as either numbered references like `$1` inserting the first capture or named using `$foo` inserting the capture named `foo`.

### re::replace_all(regex, input, to) -> string

Replaces all** occurrences of `regex` in the `input` string with `to`.

References to match groups can be done using `$` as either numbered references like `$1` inserting the first capture or named using `$foo` inserting the capture named `foo`.

### re::is_match(regex, input) -> bool

Returns if the `regex` machines `input`.

### re::split(regex, input) -> array

Splits the `input` string using the provided regular expression  `regex` as separator.

**Example:**

```rust
re::split(" ", "this is a test") == ["this", "is", "a", "string"].
```



