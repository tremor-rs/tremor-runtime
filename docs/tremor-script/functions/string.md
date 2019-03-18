# The string namespace

The string module contains functions that primarily work with strings.

## string::format(format, arg, ...)

WARNING: At the moment the string {} can not be a literal in the string itself, neither can it be passed in in a string argument.
The format function is used to create a new string based on a template and a set of variables.

The placeholder `{}` is replaced by the arguments in the list in order.

```
string::format("the {} is {}.", "meaning of life", 42)
```

would result in the string `"the meaning of life is 42"`.
