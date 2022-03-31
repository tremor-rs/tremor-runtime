### How do i define record fields?

The syntax is similar to JSON:

```tremor
{ "field": "value" }
```

With the exception that fields may have an optional ',' terminal trailing comma

### Interpolated field names

As literal strings in tremor support string interpolation the following
variants are equivalent:

```tremor
{ "snot": "badger" }

let snot = "snot";
{ "#{snot}": "badger" };
```

But, not all legal variations are recommended:

```tremor
let snot = """
snot"""; # This may result in tears
{ "#{snot}": "badger" };
```

Another legal but likely not useful variation:

```tremor
let snot = { "snot": "badger" };
{ "#{snot}": "badger" };
```

Will result in a stringifield json being encoded as the field name:

```json
{"{\"snot\":\"badger\"}":"badger"}
```

