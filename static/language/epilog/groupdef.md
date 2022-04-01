### Example group definitions

A string value
```tremor
event.string
```

The serialization of any legal tremor data value

```tremor
event
```

A set based on multiple expressions:

```tremor
set(event.key, state[key])
```

An set computed from an interation

let keys = ['snot', 'badger', 'goose'];

# ...

each(keys)
```

