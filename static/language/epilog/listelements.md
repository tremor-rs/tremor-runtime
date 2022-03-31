### List literals

Unlike JSON, trailing commas are supported

```tremor
[foo,] # A non empty list, a trailing comma is optionally permissible
```

Except in empty lists, where the idiomatic form is preferred:

```tremor
[] # An empty list - no trailing comma here!
```

