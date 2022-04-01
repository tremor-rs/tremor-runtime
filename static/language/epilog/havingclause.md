
```tremor
select event from in
having present event.important
into out
```

The `having` filters events __after__ computations has occured within them in operators
that support the clause. Any predicate ( boolean ) expression can be used
in a `having` filter.

When appropriate, the `where` clause should be used in preference over the `having` clause.
It is better to filter early before computation occurs when this is practicable or possible.

