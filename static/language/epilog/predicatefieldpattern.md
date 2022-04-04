###  Extraction

```tremor
x ~= json||
```

Tests if the field x represents an embedded json script. If true,
extracts and parses the embedded JSON and stores the parsed document
in the field `x` in the predicate patterns result, if configured.

### Aliased extraction


```tremor
alias = x ~= json||
```

Tests if the field x represents an embedded json script. If true,
extracts and parses the embedded JSON and stores the parsed document
in the field `alias` in the predicate patterns result, if configured.

### Sub records

```tremor
x ~= %{}
```

Tests if the field x represents a record value. If true, extracts embedded
record and stores it in the field `x` in the predicate patterns result, if configured.

### Sub arrays

```tremor
x ~= %[]
```

Tests if the field x represents an array value. If true, extracts embedded
array and stores it in the field `x` in the predicate patterns result, if configured.


```tremor
x ~= %()
```

Tests if the field x represents an array value. If true, extracts embedded
array and stores it in the field `x` in the predicate patterns result, if configured.

### Presence and absence

```tremor
present x
```

Is the field `x` present in the record? If true, extracts the field.

```tremor
absent x
```

Is the field `x` absent in the record?

### Comprison and Equality tests

```tremor
x >= 10
```

Tests if the numeric field x is greater than or equal to the literal 10. If true, extracts the field.

