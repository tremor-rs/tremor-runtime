### How do I insert a new value into a record?

```tremor
patch {"a": 1, "b": 2, "c": 3 } of
  insert "d" => "delta"
end;
```

It is a semantic error to insert a value if the field already exists:

```tremor
Error in foo.tremor:2:3
    1 | patch {"a": 1, "b": 2, "c": 3 } of
    2 |   insert "b" => "bravo"
      |   ^^^^^^^^^^^^^^^^^^^^^ The key that is supposed to be written to already exists: b
    3 | end;
```

### How do I update an existing value in a record?

```tremor
patch {"a": 1, "b": 2, "c": 3 } of
  update "b" => "bravo"
end;
```

It is a semantic error to update a value if the field does not already exist:


```tremor
    1 | patch {"a": 1, "b": 2, "c": 3 } of
    2 |   update "d" => "delta"
      |   ^^^^^^^^^^^^^^^^^^^^^ The key that is supposed to be updated does not exists: d
    3 | end;
```

### How do I insert or update a value in a record?

If the distinction between an `insert` and an `update` is not significant the
`upsert` operation will insert a new field, or update an existing field. This
operation is more flexible, but does not offer compile time errors to protect
against invalid usage. Where possible, use `insert` or `update` in preference
to `upsert` when a new field or replacing an existing fields value would be
an error given the business logic at hand.

### How do I erase a field from a record?

```tremor
patch {"a": 1, "b": 2, "c": 3 } of
  erase "d"
end;
```

The field `c` is removed from our record

### How do I rename a field?

```tremor
patch {"a": 1, "b": 2, "c": 3 } of
  move "c" => "d" # The value MUST be a string literal as it represents a field name
end;
```

The `c` field is removed and a `d` field is added with the value from `c`

## How do I duplicate a field?

Similar to `move`, the `copy` operation copies the value of one field to a new field

```tremor
patch {"a": 1, "b": 2, "c": 3 } of
  copy "c" => "d" # The value MUST be a string literal as it represents a field name
end;
```

The `c` field is preserved, and the `d` field is added with a copy of the value from `c`

### Can I use `patch` and `merge` together?

The `merge` operation in a `patch` expression can be applied to the patch
target record or to a specified field.

```tremor
patch {"a": 1, "b": 2, "c": 3 } of
  merge "d" => {}
end;
```

The field `d` is created in this case with the empty record.

```tremor
patch {"a": 1, "b": 2, "c": 3 } of
  merge => { "snot": "badger", "b": "bravo" } # This is a terse form of insert for `snot`, and `update for `b`
end;
```

### Defaults

For repetitive or template operations, the `default` operation allows a patch record
to be defined that is effectively merged with the target document

```tremor
patch event of
  default => {"snot": {"badger": "goose"}}
end
```

If event is an empty record, the result is the same as the `default` value expression.
If event has a `snot` field, the value for snot is preserved. The default value is not used.

Like `merge` operations, the `default` merge operation can be very effective for reducing the
boilerplate complexity of patch operations and improving the readability of the transformations
being performed by readers of the code.

For example, we could limit our `default` record patch to apply to only the `snot` field as follows:

```tremor
patch event of
  default "snot" => {"badger": "goose"}
end
```

