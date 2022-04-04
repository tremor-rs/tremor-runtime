
### How do i reference arguments?

Arguments are encapsualted via the `args` keyword symbol.

```tremor
let what = args;
```

Arguments are always record structured

```tremor
    1 | let args = 1;
      |     ^^^^^^^^ Can't assign to a constant expression
```

Arguments cannot be assigned to or overridden in scripts.


