# The `type` namespace

The type module contains functions that help inspecting types of values.

### type::as_string(value)

Returns a string representation for the value type:

* `"null"`
* `"bool"`
* `"number"`
* `"string"`
* `"array"`
* `"object"`

### type::is_null(value)

Returns if the value is `null`.

### type::is_bool(value)

Returns if the value is a boolean.

### type::is_integer(value)

Returns if the value is an integer.

### type::is_float(value)

Returns if the value is a float.

### type::is_number(value)

Returns if the value is either a float or an integer.

### type::is_string(value)

Returns if the value is a string.

### type::is_array(value)

Returns if the value is an array.

### type::is_object(value)

Returns if the object is an object.