## How do I merge two two records?

```tremor
merge {"a": 1, "b": 2, "c": 3 } of
  { "b": "bravo", "c": "charlie", "d": "delta" }
end;
```

The merge expression loosely follows the semantics of [RFC 7396 - JSON Merge Patch](https://datatracker.ietf.org/doc/html/rfc7396).

From our example:
* The field `a` is not patched and preserved
* The field `b` is patched with the value `bravo` - the original value is replaced
* The field `d` is not in the original, and is added.

The expression is useful when one record with another.

An alternative to the `Merge` expression is the `Patch` expression which is operation
rather than value based.

