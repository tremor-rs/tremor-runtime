# The `string` namespace

The string module contains functions that primarily work with strings.

## Format

### string::format(format, arg, …) -> string

The placeholder `{}` is replaced by the arguments in the list in order.

```tremor
string::format("the {} is {}.", "meaning of life", 42)
```

would result in the string `"the meaning of life is 42"`.

To use `{` or `}` as string literals in your format string, it needs to be escapedby adding another parenthesis of the same type.

```tremor
string::format("{{ this is a string format in parenthesis }}")
```

this will output:

```text
{ this is a string format in parenthesis }
```

### string::is_empty(input) -> bool

Returns if the `input` string is empty or not.

### string::len(input) -> int

Returns the length of the `input` string (counted as utf8 characters not bytes!).

### string::bytes(input) -> int

Returns the number of bytes composing the `input` string (may not be equivalent to the number of characters!).

### string::replace(input, from, to) -> string

Replaces all occurrences of `from` in `Input` to `to`.

### string::trim(input) -> string

Trims whitespaces both at the start and end of the `input` string.

### string::trim_start(input) -> string

Trims whitespaces at the start of the `input` string.

### string::trim_end(input) -> string

Trims whitespaces at the end of the `input` string.

### string::lowercase(input) -> string

Turns all characters in the `input` string to lower case.

### string::uppercase(input) -> string

Turns all characters in the `input` string to upper case.

### string::capitalize(input) -> string

Turns the first character in the `input` string to upper case. This does not ignore leading non letters!

### string::substr(input, start, end) -> string

Get all characters from index `start` to `end-1`.

### string::split(input, separator) -> array

Splits the `input` string at every occurrence of the `separator` string and turns the result in an array.
