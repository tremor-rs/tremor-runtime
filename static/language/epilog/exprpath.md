
Allows dereferencing literal values vi path expressions

```tremor
{"snot": 0, "badger": 1, "goose": 2}["badger"];
{"snot": 0, "badger": 1, "goose": 2}.badger;

...

some_record_fn().record_field
```

