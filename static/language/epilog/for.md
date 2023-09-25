
An illustration of the rule is a set of case statements, followed by an optional `default` case

```tremor
  for event of
    ## Cases
  end
```

Alternatively for expressions have an initial value and a fold operator. If neither is given the
initial value is `[]` and the operator `+` is used - so the above example can also be written as:

```tremor
  for event of
    ## Cases
  into []
  use +
  end
```

For inserting a few keys into a record we could write

```tremor
  let base = {"snot": "badger"}
  for new_values of
    case (k,v) => {k: v}
  into base
  end
```

For creating the product of numbers in a array we use:

```tremor
  for event of
    case (k,v) => v
  into 1
  use *
  end
```

The current supported operators in `use` are: `+`, `-`, `*` and `/`.



The `ForCaseClause` rule has examples of the two basic forms for record and array comprehensions.

