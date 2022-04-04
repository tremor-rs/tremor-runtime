
### Patch insert a field in a record

```tremor
let a = patch event of
  insert "test" => 1
end;
```

### Default patch templates
```tremor
patch event of
  default => {"snot": {"badger": "goose"}}
end

