# The `json` namespace

The `json` module contains functions that work with son structures

### json::decode(string) -> any

Decodes a string containing a JSON structure.

```rust
json::decode("[1, 2, 3, 4]") => [1, 2, 3, 4]
```



### json::encode(any) -> string

Encodes a data structure into a json string using minimal encoding.

```rust
json::encode([1, 2, 3, 4]) = "[1,2,3,4]"
```

### json::encode_pretty(any) -> string

Encodes a data structure into a prettified json string.

```rust
json::encode([1, 2, 3, 4]) =
"[
  1,
  2,
  3,
  4
]"
```