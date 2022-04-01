
```tremor
select event from in
where present event.important
into out
```

The `where` filters events before computations occur upon them in operators
that support the clause. Any predicate ( boolean ) expression can be used
in a `where` filter.

