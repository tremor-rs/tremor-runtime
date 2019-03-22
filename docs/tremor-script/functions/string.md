# The string namespace

The string module contains functions that primarily work with strings.

## string::format(format, arg, ...)

The placeholder `{}` is replaced by the arguments in the list in order.

```
string::format("the {} is {}.", "meaning of life", 42)
```

would result in the string `"the meaning of life is 42"`.

To use `{` or `}` as string literals in your format string, it needs to be escapedby adding another parenthesis of the same type.

```
string::format("{{ this is a string format in parenthesis }}")
```

this will output: 
```
{ this is a string format in parenthesis }
```

