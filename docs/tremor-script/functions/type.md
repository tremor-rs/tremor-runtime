# The `type` namespace

The type module contains functions that help inspecting types of values.

## Functions

### type::as_string(value) -> string

Returns a string representation for the value type:

* `"null"`
* `"bool"`
* `"integer"`
* `"float"`
* `"string"`
* `"array"`
* `"record"`

### type::is_null(value) -> bool

Returns if the value is `null`.

### type::is_bool(value) -> bool

Returns if the value is a boolean.

### type::is_integer(value) -> bool

Returns if the value is an integer.

### type::is_float(value) -> bool

Returns if the value is a float.

### type::is_number(value) -> bool

Returns if the value is either a float or an integer.

### type::is_string(value) -> bool

Returns if the value is a string.

### type::is_array(value) -> bool

Returns if the value is an array.

### type::is_record(value) -> bool

Returns if the value is a record.
