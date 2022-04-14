
### An empty record pattern

Matches any record value

```tremor
%{ }
```

### A record with a field called 'snot'

```tremor
%{ snot }
```

### A record with a field called 'snot', with a string literal value 'badger'

```tremor
%{ snot == "badger" }
```

### A record with a field called 'snot', whose string contents is well-formed embedded JSON

```tremor
%{ snot ~= json|| }
```

