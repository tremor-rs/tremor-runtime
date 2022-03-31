### Record Comprehension

```tremor
for { "snot": "badger" } of
  case (name, value) => value
end;
```

### Array Comprehension

```tremor
for [1, "foo", 2, "bar"] of
  case (index, value) => value
end;
```

### Guards

```tremor
use std::type;

for [1, "foo", 2, "bar"] of
  case (index, value) when type::is_string(value) => { "string": value }
  case (index, value) when type::is_integer(value) => { "integer": value }
end;
```

