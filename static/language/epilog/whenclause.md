
### Implicit guard

When no `when` clause is provided it is always executed equivalent to

```tremor
when true
```

### Explicit guards

Guards are predicate or boolean expressions, so any expression that reduces
to a boolean result can be used in the `WhenClause`

```tremor
when present state.snot and present event.badger
```

In the above rule, the state must be a record with a field `snot` 
present and the current event must be a record with a field `badger` present

