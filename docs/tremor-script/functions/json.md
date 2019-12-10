# The `json` namespace

The `json` module contains functions that work with son structures

## Functions

### json::decode(string) -> any

Decodes a string containing a JSON structure.

```tremor
json::decode("[1, 2, 3, 4]") => [1, 2, 3, 4]
```

### json::encode(any) -> string

Encodes a data structure into a json string using minimal encoding.

```tremor
json::encode([1, 2, 3, 4]) = "[1,2,3,4]"
```

### json::encode_pretty(any) -> string

Encodes a data structure into a prettified json string.

```tremor
json::encode_pretty([1, 2, 3, 4]) =
"[
  1,
  2,
  3,
  4
]"
```
