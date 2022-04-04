
The rule assigns an expression to a path by reference.

```tremor
x = 2
```

Assigns `x` to the value literal 2

```tremor
x.y = 2
```

Assigns the field y on record x to the value literal 2

Assignments expressions can be constant such as via the `Const` rule or mutable such as via the `Let` rule.

